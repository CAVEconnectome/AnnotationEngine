from conftest import mock_info_service
import pytest

@pytest.fixture()
def mock_me(requests_mock):
    mock_info_service(requests_mock)

def test_types(client, mock_me):
    response = client.get('/schema')
    assert response.status_code == 200
    assert type(response.json) == list


def test_bad_schema(client, mock_me):
    response = client.get('/schema/not_a_type')
    print(response.data)
    assert(response.status_code == 404)


def test_get_synapse_schema(app, client, mock_me):
    url = '/schema/synapse'.format()
    response = client.get(url)
    assert(response.status_code == 200)
    assert(len(response.data) > 0)
    # TODO make a better test
