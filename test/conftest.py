import pytest
from annotationengine import create_app
import cloudvolume
import numpy as np
import tempfile
import shutil
from google.cloud import bigtable, exceptions
import subprocess
from annotationengine.anno_database import DoNothingCreds
import grpc
from time import sleep
import os
from signal import SIGTERM
from pychunkedgraph.backend import chunkedgraph
from itertools import product


@pytest.fixture(scope='session')
def bigtable_settings():
    return 'anno_test', 'cg_test'


@pytest.fixture(scope='session', autouse=True)
def bigtable_client(request, bigtable_settings):
    # setup Emulator
    cg_project, cg_table = bigtable_settings
    bt_emul_host = "localhost:8086"
    os.environ["BIGTABLE_EMULATOR_HOST"] = bt_emul_host
    bigtables_emulator = subprocess.Popen(["gcloud",
                                           "beta",
                                           "emulators",
                                           "bigtable",
                                           "start",
                                           "--host-port",
                                           bt_emul_host],
                                          preexec_fn=os.setsid,
                                          stdout=subprocess.PIPE)

    startup_msg = "Waiting for BigTables Emulator to start up at {}..."
    print('bteh', startup_msg.format(os.environ["BIGTABLE_EMULATOR_HOST"]))
    c = bigtable.Client(project=cg_project,
                        credentials=DoNothingCreds(),
                        admin=True)
    retries = 5
    while retries > 0:
        try:
            c.list_instances()
        except exceptions._Rendezvous as e:
            # Good error - means emulator is up!
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                print(" Ready!")
                break
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                sleep(1)
            retries -= 1
            print(".")
    if retries == 0:
        print("\nCouldn't start Bigtable Emulator."
              " Make sure it is setup correctly.")
        exit(1)

    yield c

    # setup Emulator-Finalizer
    def fin():
        try:
            gid = os.getpgid(bigtables_emulator.pid)
            os.killpg(gid, SIGTERM)
        except ProcessLookupError:
            pass
        bigtables_emulator.wait()
        print('BigTable stopped')
    request.addfinalizer(fin)


@pytest.fixture(scope='session')
def cv(N=64, blockN=16):

    block_per_row = int(N / blockN)

    tempdir = tempfile.mkdtemp()
    path = "file:/{}".format(tempdir)
    chunk_size = [32, 32, 32]
    num_chunks = [int(N/cs) for cs in chunk_size]
    info = cloudvolume.CloudVolume.create_new_info(
        num_channels=1,
        layer_type='segmentation',
        data_type='uint64',
        encoding='raw',
        resolution=[4, 4, 40],  # Voxel scaling, units are in nanometers
        voxel_offset=[0, 0, 0],  # x,y,z offset in voxels from the origin
        # Pick a convenient size for your underlying chunk representation
        # Powers of two are recommended, doesn't need to cover image exactly
        chunk_size=chunk_size,  # units are voxels
        volume_size=[N, N, N],
    )
    vol = cloudvolume.CloudVolume(path, info=info)
    vol.commit_info()
    xx, yy, zz = np.meshgrid(*[np.arange(0, N) for cs in chunk_size])
    id_ind = (np.uint64(xx / blockN),
                np.uint64(yy / blockN),
                np.uint64(zz / blockN))
    id_shape = (block_per_row, block_per_row, block_per_row)
    seg = np.ravel_multi_index(id_ind, id_shape)
    vol[:] = np.uint64(seg)

    yield path

    shutil.rmtree(tempdir)


@pytest.fixture(scope='session')
def test_dataset():
    return 'test'


@pytest.fixture(scope='session')
def app(cv, test_dataset, bigtable_settings):
    bt_project, bt_table = bigtable_settings

    app = create_app(
        {
            'project_id': bt_project,
            'emulate': True,
            'TESTING': True,
            'PYCHUNKEDGRAPH_ENDPOINT':  "https://pychunkedgraph/pychunkgraph",
            'DATASETS': [
                {
                    'name': test_dataset,
                    'CV_SEGMENTATION_PATH': cv
                }
            ],
            'BIGTABLE_CONFIG': {
                'emulate': True
            },
            'CHUNKGRAPH_TABLE_ID': bt_table
        }
    )
    yield app


@pytest.fixture(scope='session')
def client(app):
    return app.test_client()
