from annotationengine.anno_database import get_db
from annotationengine.dataset import get_datasets
from emannotationschemas import get_types
from conftest import mock_info_service
import pytest


@pytest.fixture()
def mock_me(requests_mock):
    mock_info_service(requests_mock)


def test_db(app, mock_me):
    with app.app_context():
        db = get_db()
        types = get_types()
        for dataset in get_datasets():
            for type_ in types:
                db.create_table('test', dataset, type_, type_)
            mds = db.get_existing_tables_metadata(dataset)
            print(mds)
            for type_ in types:
                md = next(md for md in mds if md['table_name']==type_)
