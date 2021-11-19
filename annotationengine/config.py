# Define the application directory
import os
from annotationengine.utils import get_app_base_path
import logging


class BaseConfig(object):
    HOME = os.path.expanduser("~")
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    DEBUG = True
    proj_dir = os.path.split(get_app_base_path())[0]
    SQLALCHEMY_DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/annodb"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    NEUROGLANCER_URL = "https://neuroglancer-demo.appspot.com"
    INFOSERVICE_ENDPOINT = "http://globalv1.daf-apis.com/info"
    AUTH_URI = "https://globalv1.daf-apis.com/auth"
    GLOBAL_SERVER = "https://globalv1.daf-apis.com/auth"
    SCHEMA_SERVICE_ENDPOINT = "https://globalv1.daf-apis.com/schema/"
    TESTING = False
    LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    LOGGING_LOCATION = HOME + "/annoeng/bookshelf.log"
    LOGGING_LEVEL = logging.DEBUG
    AUTH_DATABASE_NAME = "minnie65"


class TestConfig(BaseConfig):
    ENV = "testing"
    TESTING = True


config = {
    "development": "annotationengine.config.BaseConfig",
    "testing": "annotationengine.config.BaseConfig",
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
