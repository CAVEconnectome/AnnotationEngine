from flask import Flask
from annotationengine.config import configure_app
from annotationengine.utils import get_instance_folder_path
from annotationengine import annotation
from annotationengine import schemas
from annotationengine import voxel
from annotationengine import dataset as dataset_mod

try:
    from pychunkedgraph.app import app_blueprint as pychg_app_blueprint
    pychg_avail = True
except:
    pychg_avail = False

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
    app.register_blueprint(schemas.bp)
    app.register_blueprint(dataset_mod.bp)
    with app.app_context():
        db = annotation.get_db()
        types = schemas.get_types()
        for dataset in app.config['DATASETS']:
            for type_ in types:
                if not db.has_table(dataset['name'], type_):
                    db.create_table(dataset['name'], type_)
                    print('creating table {}:{}'.format(dataset['name'],
                                                        type_))
                else:
                    print('table exists {} {}'.format(dataset['name'], type_))
    return app


def create_app_seg(test_config=None):

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
    app.register_blueprint(schemas.bp)
    app.register_blueprint(dataset_mod.bp)
    app.register_blueprint(pychg_app_blueprint.bp)
    with app.app_context():
        db = annotation.get_db()
        types = schemas.get_types()
        for dataset in app.config['DATASETS']:
            for type_ in types:
                if not db.has_table(dataset['name'], type_):
                    db.create_table(dataset['name'], type_)
                    print('creating table {}:{}'.format(dataset['name'],
                                                        type_))
                else:
                    print('table exists {} {}'.format(dataset['name'], type_))
    return app