# Define the application directory
import os


class BaseConfig(object):
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    DEBUG = True

    CV_VOLUME_PATH = "gs://neuroglancer/basil_v0/son_of_alignment/\
                      v3.04_cracks_only_normalized_rechunked"
    CV_SEGMENTATION_PATH = "gs://neuroglancer/basil_v0/basil_full/seg-aug"
    NEUROGLANCER_URL = "https://neuroglancer-demo.appspot.com"


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
