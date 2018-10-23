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
from emannotationschemas.blueprint_app import get_type_schema, get_types

INFOSERVICE_ENDPOINT = "http://infoservice"
SCHEMA_SERVICE_ENDPOINT = "http://schemaservice"
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
def test_dataset():
    return TEST_DATASET_NAME


@pytest.fixture(scope='session')
def app( test_dataset, bigtable_settings):
    bt_project, bt_table = bigtable_settings

    with requests_mock.Mocker() as m:
        dataset_url = os.path.join(INFOSERVICE_ENDPOINT, 'api/datasets')
        m.get(dataset_url, json=[test_dataset])
        dataset_info_url = os.path.join(INFOSERVICE_ENDPOINT,
                                        'api/dataset/{}'.format(test_dataset))
        dataset_d = {
            "annotation_engine_endpoint": "http://35.237.200.246",
            "id": 1,
            "name": test_dataset,
            "pychunkgraph_endpoint": PYCHUNKEDGRAPH_ENDPOINT,
        }
        m.get(dataset_info_url, json=dataset_d)
        
        app = create_app(
            {
                'project_id': 'test',
                'emulate': True,
                'TESTING': True,
                'INFOSERVICE_ENDPOINT':  INFOSERVICE_ENDPOINT,
                'SCHEMA_SERVICE_ENDPOINT': SCHEMA_SERVICE_ENDPOINT,
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


def mock_schema_service(requests_mock):
    types_url = os.path.join(SCHEMA_SERVICE_ENDPOINT)
    types = get_types()
    requests_mock.get(types_url, json=types)
    for type_ in types:
        url = os.path.join(SCHEMA_SERVICE_ENDPOINT, type_)
        requests_mock.get(url, json=get_type_schema(type_))


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
