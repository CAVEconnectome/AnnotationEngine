
__version__ = "0.0.37"


def create_app(test_config=None):
    from flask import Flask
    from annotationengine.config import configure_app
    from annotationengine.utils import get_instance_folder_path
    from annotationengine.dataset import get_dataset_db
    from annotationengine import annotation
    from annotationengine import schemas
    from annotationengine import voxel
    from annotationengine import dataset as dataset_mod
    from annotationengine import chunked_annotation

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
    app.register_blueprint(chunked_annotation.bp)

    with app.app_context():
        db = annotation.get_db()
        types = schemas.get_types()
        dataset_db = get_dataset_db()

        for dataset_name in dataset_db.get_dataset_names():      
            for type_ in types:
                if not db.has_table(dataset_name, type_):
                    db.create_table(dataset_name, type_)
                    print('creating table {}:{}'.format(dataset_name,
                                                        type_))
                else:
                    print('table exists {} {}'.format(dataset_name, type_))
    return app
