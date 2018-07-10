import pytest
from annotationengine import create_app
import cloudvolume
import numpy as np
import tempfile
import shutil
from google.cloud import bigtable, exceptions
import subprocess
from annotationengine.database import DoNothingCreds
import grpc
from time import sleep
import os
from signal import SIGTERM


@pytest.fixture(scope='session', autouse=True)
def bigtable_emulator(request):
    # setup Emulator
    bigtables_emulator = subprocess.Popen(["gcloud",
                                           "beta",
                                           "emulators",
                                           "bigtable",
                                           "start"],
                                          preexec_fn=os.setsid,
                                          stdout=subprocess.PIPE)

    # bt_env_init = subprocess.run(
    #     ["gcloud",
    #      "beta",
    #      "emulators",
    #      "bigtable",
    #      "env-init"],
    #     stdout=subprocess.PIPE)
    # bt_emul_host = bt_env_init.stdout.decode(
    #     "utf-8").strip().split('=')[-1]
    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    startup_msg = "Waiting for BigTables Emulator to start up at {}..."
    print(startup_msg.format(os.environ["BIGTABLE_EMULATOR_HOST"]))
    c = bigtable.Client(project='', credentials=DoNothingCreds(), admin=True)
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

    # setup Emulator-Finalizer
    def fin():
        os.killpg(os.getpgid(bigtables_emulator.pid), SIGTERM)
        bigtables_emulator.wait()

    request.addfinalizer(fin)


@pytest.fixture
def cv(N=64, blockN=16):
    block_per_row = N / blockN

    tempdir = tempfile.mkdtemp()
    path = "file:/{}".format(tempdir)

    info = cloudvolume.CloudVolume.create_new_info(
        num_channels=1,
        layer_type='segmentation',
        data_type='uint64',
        encoding='raw',
        resolution=[4, 4, 40],  # Voxel scaling, units are in nanometers
        voxel_offset=[0, 0, 0],  # x,y,z offset in voxels from the origin
        # Pick a convenient size for your underlying chunk representation
        # Powers of two are recommended, doesn't need to cover image exactly
        chunk_size=[64, 64, 64],  # units are voxels
        volume_size=[N, N, N],
    )
    vol = cloudvolume.CloudVolume(path, info=info)
    vol.commit_info()

    xx, yy, zz = np.meshgrid(np.arange(0, N),
                             np.arange(0, N),
                             np.arange(0, N))

    seg = np.int64(xx / blockN) + \
        block_per_row * np.int64(yy / blockN) + \
        block_per_row * block_per_row * np.int64(zz / blockN)
    vol[:] = np.uint64(seg)

    yield path

    shutil.rmtree(tempdir)


@pytest.fixture
def test_dataset():
    return 'test'


@pytest.fixture
def app(cv, test_dataset):
    app = create_app(
        {
            'TESTING': True,
            'DATASETS': [
                {
                    'name': test_dataset,
                    'CV_SEGMENTATION_PATH': cv
                }
            ],
            'BIGTABLE_CONFIG': {
                'emulate': True
            }
        }
    )
    yield app


@pytest.fixture
def client(app):
    return app.test_client()
