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

    NEUROGLANCER_URL = "https://neuroglancer-demo.appspot.com"
    INFOSERVICE_ENDPOINT = "http://35.196.170.230"
    BIGTABLE_CONFIG = {
        'instance_id': 'pychunkedgraph',
        'project_id': "neuromancer-seung-import"
    }
    TESTING = False
    LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOGGING_LOCATION = HOME + '/annoeng/bookshelf.log'
    LOGGING_LEVEL = logging.DEBUG
    # TODO what is this suppose to be by default?
    CHUNKGRAPH_TABLE_ID = "chunkedgraph"

    CHUNKGRAPH_TABLE_ID = "pinky40_fanout2_v7"


config = {
    "development": "annotationengine.config.BaseConfig",
    "testing": "annotationengine.config.BaseConfig",
    "default": "annotationengine.config.BaseConfig"
}


def configure_app(app):
    config_name = os.getenv('FLASK_CONFIGURATION', 'default')
    # object-based default configuration
    app.config.from_object(config[config_name])
    if 'ANNOTATION_ENGINE_SETTINGS' in os.environ.keys():
        app.config.from_envvar('ANNOTATION_ENGINE_SETTINGS')
    # instance-folders configuration
    app.config.from_pyfile('config.cfg', silent=True)

    return app
