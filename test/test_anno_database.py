from annotationengine.anno_database import get_db
from annotationengine.dataset import get_dataset_db
from annotationengine.schemas import get_types
from conftest import mock_info_service
import pytest

@pytest.fixture()
def mock_me(requests_mock):
    mock_info_service(requests_mock)

def test_db(app, mock_me):
    with app.app_context():
        db = get_db()
        dataset_db = get_dataset_db()
        types = get_types()
        for dataset in dataset_db.get_dataset_names():
            tables = db.get_existing_annotation_types(dataset)
            for type_ in types:
                assert(type_ in tables)
