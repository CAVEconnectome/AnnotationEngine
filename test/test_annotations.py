import json
from conftest import mock_info_service
import pytest


@pytest.fixture()
def mock_me(requests_mock):
    mock_info_service(requests_mock)


@pytest.fixture()
def synapse_table_md():
    return {'table_name': 'test_synapse', 'schema_name': 'synapse'}


@pytest.fixture()
def test_synapse_table(client, test_dataset, synapse_table_md, mockme):
    url = '/annotaiton/dataset/{}'.format(test_dataset)
    response = client.post(url, json=synapse_table_md)
    assert(response.status_code == 200)

    match = next([d for d in respons.json if d['table_name'] == 'synapse'])

    url = 'annotation/dataset/test_synapse'
    r = client.get(url)
    assert(r.status_code == 200)
    assert(r.json == match)
    return synapse_table_md['table_name']


def test_annotation_dataset(client, test_dataset, test_synapse_table, mock_me):
    url = '/annotation/dataset/test_synapse'
    response = client.get(url)
    assert(response.status_code == 200)


def test_post_bad_type(client, test_dataset, mock_me):
    junk_d = {
        'type': 'junk_annotation',
        'tag': 'junk'
    }
    url = '/annotation/dataset/{}/junk_annotation'.format(test_dataset)
    response = client.post(url,
                           data=json.dumps(junk_d))
    assert(response.status_code == 404)
