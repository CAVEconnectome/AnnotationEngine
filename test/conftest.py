import logging
import os
import tempfile
import json

import psycopg2
import pytest
from annotationengine import create_app, db
from flask import g, current_app

logging.basicConfig(level=logging.DEBUG)
test_logger = logging.getLogger()


INFOSERVICE_ENDPOINT = "http://infoservice"
SCHEMA_SERVICE_ENDPOINT = "http://schemaservice"
TEST_DATASET_NAME = "test"
tempdir = tempfile.mkdtemp()
TEST_PATH = "file:/{}".format(tempdir)
PYCHUNKEDGRAPH_ENDPOINT = "http://pcg/segmentation"


def pytest_addoption(parser):
    parser.addoption(
        "--docker",
        action="store",
        default=False,
        help="Use docker for postgres testing",
    )


@pytest.fixture(scope="session")
def docker_mode(request):
    return request.config.getoption("--docker")


def pytest_configure(config):
    config.addinivalue_line("markers", "docker: use postgres in docker")


@pytest.fixture(scope="session")
def test_aligned_volume():
    return TEST_DATASET_NAME


@pytest.fixture(scope="module")
def client():
    flask_app = create_app(config_name=os.environ.get("FLASK_CONFIGURATION", "testing"))
    test_logger.info("Starting test flask app...")

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        # Establish an application context
        with flask_app.app_context():
            db.create_all()
            yield testing_client
            db.drop_all()


@pytest.fixture(scope="module")
def app_config(client):
    yield client.application


@pytest.fixture()
def modify_g(client):
    g.user = "test"
    g.id = 1


@pytest.fixture()
def mock_info_service():
    aligned_volume_url = os.path.join(INFOSERVICE_ENDPOINT, "api/aligned_volumes")
    aligned_volume_url.get(aligned_volume_url, json=[TEST_DATASET_NAME])
    aligned_volume_info_url = os.path.join(
        INFOSERVICE_ENDPOINT, "api/aligned_volume/{}".format(TEST_DATASET_NAME)
    )
    aligned_volume_d = {
        "annotation_engine_endpoint": "http://35.237.200.246",
        "flat_segmentation_source": TEST_PATH,
        "id": 1,
        "image_source": TEST_PATH,
        "name": TEST_DATASET_NAME,
        "pychunkgraph_endpoint": "http://pcg/segmentation",
        "pychunkgraph_segmentation_source": TEST_PATH,
    }
    return json(aligned_volume_d)


def check_database(sql_uri: str) -> None:  # pragma: no cover
    try:
        test_logger.info("ATTEMPT TO CONNECT")
        conn = psycopg2.connect(sql_uri)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        test_logger.info("CONNECTED")

        cur.close()
        conn.close()
    except Exception as e:
        test_logger.info(e)


# @pytest.fixture(scope="session")
# def mock_schema_service(requests_mock):
#     types_url = os.path.join(SCHEMA_SERVICE_ENDPOINT, 'type')
#     types = get_types()
#     requests_mock.get(types_url, json=types)
#     for type_ in types:
#         url = os.path.join(SCHEMA_SERVICE_ENDPOINT, 'type', type_)
#         requests_mock.get(url, json=get_type_schema(type_))

# @pytest.fixture(scope="session")
# def mock_info_service(requests_mock):
#     aligned_volume_url = os.path.join(INFOSERVICE_ENDPOINT, 'api/aligned_volumes')
#     requests_mock.get(aligned_volume_url, json=[TEST_DATASET_NAME])
#     aligned_volume_info_url = os.path.join(INFOSERVICE_ENDPOINT,
#                                     'api/aligned_volume/{}'.format(TEST_DATASET_NAME))
#     aligned_volume_d = {
#         "annotation_engine_endpoint": "http://35.237.200.246",
#         "flat_segmentation_source": TEST_PATH,
#         "id": 1,
#         "image_source": TEST_PATH,
#         "name": TEST_DATASET_NAME,
#         "pychunkgraph_endpoint": "http://pcg/segmentation",
#         "pychunkgraph_segmentation_source": TEST_PATH
#     }
#     requests_mock.get(aligned_volume_info_url, json=aligned_volume_d)
