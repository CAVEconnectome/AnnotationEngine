"""
Regression test for pd.read_sql compatibility with SQLAlchemy 1.4+ and pandas 2.x.

pandas 2.3+ requires SQLAlchemy >= 2.0.0.  When SQLAlchemy 1.4 is installed,
pandas' import_optional_dependency("sqlalchemy") returns None, so pd.read_sql
falls through to the SQLiteDatabase DBAPI2 path and raises:
    TypeError: Query must be a string unless using sqlalchemy.

The fix is to execute via SQLAlchemy directly and build the DataFrame from the
result, bypassing pd.read_sql entirely:
    with engine.connect() as conn:
        result = conn.execute(query.statement)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
"""

import json
import os
from unittest import mock

import pytest

aligned_volume_name = "test_aligned_volume"
table_name = "test_view_table"


@pytest.fixture(scope="module", autouse=True)
def create_test_table(client):
    data = {
        "table_name": table_name,
        "schema_type": "synapse",
        "metadata": {
            "user_id": "1",
            "description": "Test table for views",
            "flat_segmentation_source": "precomputed://gs://test/image",
            "voxel_resolution_x": 4,
            "voxel_resolution_y": 4,
            "voxel_resolution_z": 40,
        },
    }
    url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
    with mock.patch("annotationengine.api.check_aligned_volume") as mock_cv:
        mock_cv.return_value = aligned_volume_name
        client.post(url, json=data, content_type="application/json", follow_redirects=True)


class TestViewPages:
    def test_aligned_volume_view_returns_200(self, client):
        url = f"/annotation/views/aligned_volume/{aligned_volume_name}"
        with mock.patch.dict(os.environ, {"AUTH_URI": "auth.test.example.com"}):
            response = client.get(url, follow_redirects=False)
        assert response.status_code == 200

    def test_table_view_returns_200(self, client):
        url = f"/annotation/views/aligned_volume/{aligned_volume_name}/table/{table_name}"
        with mock.patch.dict(os.environ, {"AUTH_URI": "auth.test.example.com"}):
            response = client.get(url, follow_redirects=False)
        assert response.status_code == 200
