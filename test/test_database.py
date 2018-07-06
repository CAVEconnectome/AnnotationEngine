from annotationengine.database import get_db
from annotationengine.dataset import get_dataset_db
from annotationengine.schemas import get_types


def test_db(app):
    with app.app_context():
        db = get_db()
        dataset_db = get_dataset_db()
        types = get_types()
        for dataset in dataset_db.get_dataset_names():
            print(db._annotation_tables.keys())
            tables = db.get_existing_annotation_types(dataset)
            for type_ in types: 
                assert(type_ in tables)
