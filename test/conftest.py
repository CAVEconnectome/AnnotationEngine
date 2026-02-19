import logging
import time
import uuid
import warnings
import os
import tempfile
import json

import docker
import psycopg2
import pytest
from annotationengine import create_app, db
from flask import appcontext_pushed, g, current_app

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
def database_metadata() -> dict:
    yield {
        "postgis_docker_image": "postgis/postgis:13-master",
        "db_host": "localhost",
        "sql_uri": "postgresql://postgres:postgres@localhost:5432/test_aligned_volume",
    }


@pytest.fixture(scope="session")
def test_aligned_volume():
    return TEST_DATASET_NAME


@pytest.fixture(scope="module")
def client():
    config_name = os.environ.get("FLASK_CONFIGURATION", "testing")
    flask_app = create_app(config_name=config_name)
    test_logger.info("Starting test flask app...")

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        # Establish an application context
        with flask_app.app_context():
            db.create_all()
            logging.info("yielding testing_client")
            yield testing_client
            # --- Cleanup Phase ---
            logging.info("Tearing down testing_client, committing session")
            
            db.session.commit()

            logging.info("dropping all tables")
            # 2. Drop the tables
            db.drop_all()
            logging.info("removing session")
            # 1. Remove the scoped session to prevent "ResourceBusy" errors
            db.session.remove()
            logging.info("disposing engine")
            # 3. Dispose of the engine to kill the connection pool (The Fix)
            db.engine.dispose()
            logging.info("cleanup complete")


@pytest.fixture(scope="module")
def app_config(client):
    logging.info(current_app.config["AUTH_DISABLED"])
    yield client.application


@pytest.fixture()
def modify_g(client):
    g.auth_user = {"id": 1}
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


@pytest.fixture(scope="session", autouse=True)
def postgis_server(docker_mode, database_metadata: dict) -> None:

    postgis_docker_image = database_metadata["postgis_docker_image"]
    sql_uri = database_metadata["sql_uri"]

    if docker_mode:
        test_logger.info(f"PULLING {postgis_docker_image} IMAGE")
        try:
            docker_client = docker.from_env()
            docker_client.images.pull(repository=postgis_docker_image)
        except Exception:
            test_logger.exception("Failed to pull postgres image")

        container_name = f"test_postgis_server_{uuid.uuid4()}"

        test_container = docker_client.containers.run(
            image=postgis_docker_image,
            detach=True,
            hostname="test_postgres",
            auto_remove=True,
            name=container_name,
            environment=[
                "POSTGRES_USER=postgres",
                "POSTGRES_PASSWORD=postgres",
                "POSTGRES_DB=test_aligned_volume",
            ],
            ports={"5432/tcp": 5432},
        )

        test_logger.info("STARTING IMAGE")
        try:
            time.sleep(10)
            check_database(sql_uri)
        except Exception as e:
            raise (e)
    yield
    if docker_mode:
        warnings.filterwarnings(
            action="ignore", message="unclosed", category=ResourceWarning
        )
        container = docker_client.containers.get(container_name)
        container.stop()


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
