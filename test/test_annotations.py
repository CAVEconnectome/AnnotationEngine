import json
from conftest import mock_info_service, mock_schema_service
import pytest

@pytest.fixture()
def mock_me(requests_mock):
    mock_info_service(requests_mock)
    mock_schema_service(requests_mock)


def test_post_bad_type(client, test_dataset, mock_me):
    junk_d = {
        'type': 'junk_annotation',
        'tag': 'junk'
    }
    url = '/annotation/dataset/{}/junk_annotation'.format(test_dataset)
    response = client.post(url,
                           data=json.dumps(junk_d))
    assert(response.status_code == 404)
