# from annotationengine.anno_database import get_db
# from annotationengine.aligned_volume import get_aligned_volumes
# from emannotationschemas import get_types
# from conftest import mock_info_service
# import pytest


# @pytest.fixture()
# def mock_me(requests_mock):
#     mock_info_service(requests_mock)


# def test_db(app, mock_me):
#     with app.app_context():
#         db = get_db()
#         types = get_types()
#         for aligned_volume in get_aligned_volumes():
#             for type_ in types:
#                 db.create_table('test', aligned_volume, type_, type_)
#             mds = db.get_existing_tables_metadata(aligned_volume)
#             print(mds)
#             for type_ in types:
#                 md = next(md for md in mds if md['table_name']==type_)
