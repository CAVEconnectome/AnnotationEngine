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
def cg_settings():
    return 'anno_test', 'cg_test'


@pytest.fixture(scope='session', autouse=True)
def bigtable_client(request, cg_settings):
    # setup Emulator
    cg_project, cg_table = cg_settings
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
def chunkgraph_tuple(request,
                     bigtable_client,
                     cg_settings,
                     fan_out=2,
                     n_layers=3):
    cg_project, cg_table = cg_settings
    print('creating table {}'.format(cg_project))
    graph = chunkedgraph.ChunkedGraph(cg_table,
                                      client=bigtable_client,
                                      project_id=cg_project,
                                      is_new=True, fan_out=fan_out,
                                      n_layers=n_layers, cv_path="",
                                      chunk_size=(32, 32, 32))

    yield graph, cg_table

    def fin():
        graph.table.delete()
    request.addfinalizer(fin)
    print("\n\nTABLE DELETED")


def to_label(cgraph, l, x, y, z, segment_id):
    return cgraph.get_node_id(np.uint64(segment_id), layer=l, x=x, y=y, z=z)


@pytest.fixture(scope='session')
def cv(chunkgraph_tuple, N=64, blockN=16):
    cgraph, cg_table = chunkgraph_tuple

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
    xx, yy, zz = np.meshgrid(*[np.arange(0, cs) for cs in chunk_size])

    for x, y, z in product(*[range(nc) for nc in num_chunks]):
        seg = np.uint64(xx / blockN) + \
            block_per_row * np.uint64(yy / blockN) + \
            block_per_row * block_per_row * np.uint64(zz / blockN)
        for seg_id in np.unique(seg):
            new_id = to_label(cgraph, 1, x, y, z, seg_id)
            is_id = seg == seg_id
            seg[is_id] = new_id

        vol[x*chunk_size[0]:(x+1)*chunk_size[0],
            y*chunk_size[1]:(y+1)*chunk_size[1],
            z*chunk_size[2]:(z+1)*chunk_size[2]] = np.uint64(seg)

    yield path

    shutil.rmtree(tempdir)


@pytest.fixture(scope='session')
def test_dataset():
    return 'test'


@pytest.fixture(scope='session')
def app(cv, test_dataset, cg_settings):
    cg_project, cg_table = cg_settings

    app = create_app(
        {
            'project_id': cg_project,
            'emulate': True,
            'TESTING': True,
            'DATASETS': [
                {
                    'name': test_dataset,
                    'CV_SEGMENTATION_PATH': cv
                }
            ],
            'BIGTABLE_CONFIG': {
                'emulate': True
            },
            'CHUNKGRAPH_TABLE_ID': cg_table
        }
    )
    yield app


@pytest.fixture(scope='session')
def client(app):
    return app.test_client()
