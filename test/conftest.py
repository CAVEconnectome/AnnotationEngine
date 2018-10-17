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
import requests_mock

INFOSERVICE_ENDPOINT = "http://infoservice"
TEST_DATASET_NAME = 'test'
tempdir = tempfile.mkdtemp()
TEST_PATH = "file:/{}".format(tempdir)
PYCHUNKEDGRAPH_ENDPOINT = "http://pcg/segmentation"


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

    chunk_size = [32, 32, 32]
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
    vol = cloudvolume.CloudVolume(TEST_PATH, info=info)
    vol.commit_info()
    xx, yy, zz = np.meshgrid(*[np.arange(0, N) for cs in chunk_size])
    id_ind = (np.uint64(xx / blockN),
              np.uint64(yy / blockN),
              np.uint64(zz / blockN))
    id_shape = (block_per_row, block_per_row, block_per_row)

    seg = np.ravel_multi_index(id_ind, id_shape)
    vol[:] = np.uint64(seg)

    yield TEST_PATH

    shutil.rmtree(tempdir)


@pytest.fixture(scope='session')
def root_id_vol(N=64, blockN=32):

    block_per_row = int(N / blockN)

    chunk_size = [32, 32, 32]
    xx, yy, zz = np.meshgrid(*[np.arange(0, N) for cs in chunk_size])
    root_ind = (np.uint64(xx / (blockN)),
                np.uint64(yy / (blockN)),
                np.uint64(zz / (blockN)))
    root_shape = (block_per_row, block_per_row, block_per_row)
    root_id = np.ravel_multi_index(root_ind, root_shape)+1000

    return root_id


@pytest.fixture(scope='session')
def test_dataset():
    return TEST_DATASET_NAME


def get_supervoxel_leaves(cv, root_id_vol, root_id):
    vol = cloudvolume.CloudVolume(cv)
    vol = vol[:]
    print(np.squeeze(vol[::4, ::4, 0]))
    print(np.squeeze(root_id_vol[::4, ::4, 0]))

    return np.unique(vol[root_id_vol == root_id])


@pytest.fixture(scope='session')
def app(cv, root_id_vol, test_dataset, bigtable_settings):
    bt_project, bt_table = bigtable_settings

    with requests_mock.Mocker() as m:
        dataset_url = os.path.join(INFOSERVICE_ENDPOINT, 'api/datasets')
        m.get(dataset_url, json=[test_dataset])
        dataset_info_url = os.path.join(INFOSERVICE_ENDPOINT,
                                        'api/dataset/{}'.format(test_dataset))
        dataset_d = {
            "annotation_engine_endpoint": "http://35.237.200.246",
            "flat_segmentation_source": cv,
            "id": 1,
            "image_source": cv,
            "name": test_dataset,
            "pychunkgraph_endpoint": PYCHUNKEDGRAPH_ENDPOINT,
            "pychunkgraph_segmentation_source": cv
        }
        m.get(dataset_info_url, json=dataset_d)
        root_id = 100000
        cg_url = PYCHUNKEDGRAPH_ENDPOINT + \
            '/1.0/segment/{}/leaves'.format(root_id)
        seg_ids = get_supervoxel_leaves(cv, root_id_vol, root_id)
        m.get(cg_url, json=seg_ids)
        app = create_app(
            {
                'project_id': 'test',
                'emulate': True,
                'TESTING': True,
                'INFOSERVICE_ENDPOINT':  INFOSERVICE_ENDPOINT,
                'BIGTABLE_CONFIG': {
                    'emulate': True
                },
                'CHUNKGRAPH_TABLE_ID': test_dataset
            }
        )

    yield app


@pytest.fixture(scope='session')
def client(app):
    return app.test_client()


def mock_info_service(requests_mock):
    dataset_url = os.path.join(INFOSERVICE_ENDPOINT, 'api/datasets')
    requests_mock.get(dataset_url, json=[TEST_DATASET_NAME])
    dataset_info_url = os.path.join(INFOSERVICE_ENDPOINT,
                                    'api/dataset/{}'.format(TEST_DATASET_NAME))
    dataset_d = {
        "annotation_engine_endpoint": "http://35.237.200.246",
        "flat_segmentation_source": TEST_PATH,
        "id": 1,
        "image_source": TEST_PATH,
        "name": TEST_DATASET_NAME,
        "pychunkgraph_endpoint": "http://pcg/segmentation",
        "pychunkgraph_segmentation_source": TEST_PATH
    }
    requests_mock.get(dataset_info_url, json=dataset_d)
