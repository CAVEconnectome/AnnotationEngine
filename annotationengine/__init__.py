from flask import Flask
from annotationengine.config import configure_app
from annotationengine.utils import get_instance_folder_path
from annotationengine import annotation
from annotationengine import voxel
__version__ = "0.0.1"


def create_app():

    # Define the Flask Object
    app = Flask(__name__,
                instance_path=get_instance_folder_path(),
                instance_relative_config=True)

    app = configure_app(app)
    app.register_blueprint(annotation.bp)
    app.register_blueprint(voxel.bp)

    return app
