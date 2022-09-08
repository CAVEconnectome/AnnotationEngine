# Define the application directory
import os
from annotationengine.utils import get_app_base_path
import logging


class BaseConfig(object):
    HOME = os.path.expanduser("~")
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    proj_dir = os.path.split(get_app_base_path())[0]
    SQLALCHEMY_DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/annotation"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    NEUROGLANCER_URL = "https://neuroglancer-demo.appspot.com"
    GLOBAL_SERVER = "https://global.daf-apis.com"
    INFOSERVICE_ENDPOINT = f"{GLOBAL_SERVER}/info"
    AUTH_URI = f"{GLOBAL_SERVER}/auth"
    SCHEMA_SERVICE_ENDPOINT = f"{GLOBAL_SERVER}/schema"
    TESTING = False
    LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    LOGGING_LOCATION = f"{HOME}/annoeng/bookshelf.log"
    AUTH_DATABASE_NAME = "minnie65"


class DevConfig(BaseConfig):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "postgresql://postgres:annodb@db:5432/annodb"
    LOGGING_LEVEL = logging.DEBUG


class TestConfig(BaseConfig):
    ENV = "testing"
    TESTING = True
    AUTH_DISABLED = True
    SQLALCHEMY_DATABASE_URI = (
        "postgresql://postgres:postgres@localhost:5432/test_aligned_volume"
    )


config = {
    "development": "annotationengine.config.DevConfig",
    "testing": "annotationengine.config.TestConfig",
    "default": "annotationengine.config.BaseConfig",
}


def configure_app(app):
    config_name = os.getenv("FLASK_CONFIGURATION", "default")
    # object-based default configuration
    app.config.from_object(config[config_name])
    if "ANNOTATION_ENGINE_SETTINGS" in os.environ.keys():
        app.config.from_envvar("ANNOTATION_ENGINE_SETTINGS")
    # instance-folders configuration
    app.config.from_pyfile("config.cfg", silent=True)

    return app
