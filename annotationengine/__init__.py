from flask import Flask
from annotationengine.config import configure_app
from annotationengine.utils import get_instance_folder_path
from annotationengine import annotation
from annotationengine import voxel
__version__ = "0.0.1"


def create_app(test_config=None):

    # Define the Flask Object
    app = Flask(__name__,
                instance_path=get_instance_folder_path(),
                instance_relative_config=True)
    # load configuration (from test_config if passed)
    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)
    # register blueprints
    app.register_blueprint(annotation.bp)
    app.register_blueprint(voxel.bp)

    with app.app_context():
        db = annotation.get_db()
        types = annotation.get_types()
        for type_ in types:
            if not db.has_table(type_):
                db.add_table(type_)

    return app
