import pandas as pd
from conftest import mock_info_service
import pytest


@pytest.fixture()
def mock_me(requests_mock):
    mock_info_service(requests_mock)


def test_junk_synapse(client, test_dataset, mock_me):
    junk_d = {
        'type': 'synapse',
        'pt_prt': {
            'position': [4, 4, 0]
        }
    }
    synapse_url = '/annotation/dataset/{}/synapse'.format(test_dataset)
    response = client.post(synapse_url,
                           json=junk_d)
    assert response.status_code == 422


def test_synapse(client, app, test_dataset, mock_me):
    synapse_d = {
        'type': 'synapse',
        'pre_pt':
            {
                'position': [31, 31, 0],
            },
        'ctr_pt':
            {
                'position': [32, 32, 0],
            },
        'post_pt':
            {
                'position': [33, 33, 0],
            }
    }
    synapse_url = '/annotation/dataset/{}/synapse'.format(test_dataset)
    response = client.post(synapse_url,
                           json=[synapse_d])
    assert response.status_code == 200
    response_d = response.json
    oid = response_d[0]
    assert(type(oid) == int)

    url = '/annotation/dataset/{}/synapse/{}'.format(test_dataset,
                                                     oid)
    # test that the client can retreive it
    response = client.get(url)
    assert(response.status_code == 200)
    synapse_d = response.json
    assert(type(synapse_d['pre_pt']['supervoxel_id']) == int)

    # # test that we can search for it
    # TODO implement this feature
    # response = client.get(synapse_url)
    # assert (response.status_code == 200)
    # assert (oid in response.json)

    # now lets modify it and update it with put
    synapse_d['pre_pt']['position'] = [31, 30, 0]
    response = client.put(url, json=synapse_d)
    assert(response.status_code == 200)

    # test that updating it with bad data fails
    junk_d = {
        'type': 'synapse',
        'pt_prt': {
            'position': [4, 4, 0]
        }
    }
    response = client.put(url, json=junk_d)
    assert response.status_code == 422

    # test that it is changed when we get it again
    response = client.get(url)
    assert(response.status_code == 200)
    synapse_d = response.json
    assert(synapse_d['pre_pt']['position'] == [31, 30, 0])

    # test that we can delete it
    response = client.delete(url)
    assert(response.status_code == 200)

    # test that we get 404 when we try to get it again
    response = client.get(url)
    assert(response.status_code == 404)

    # test that we get 404 when we try to delete it again
    response = client.delete(url)
    assert(response.status_code == 404)


def test_bulk_synapse(client, app, test_dataset, mock_me):
    data = [[[0, 0, 0], [0, 0, 1], [0, 0, 2]],
            [[10, 10, 10], [10, 13, 10], [10, 15, 10]],
            [[20, 25, 5], [22, 25, 5], [25, 25, 5]]]

    df = pd.DataFrame(data)
    df.columns = ['pre_pt.position', 'ctr_pt.position', 'post_pt.position']
    url = '/annotation/dataset/{}/synapse?bulk=true'.format(test_dataset)
    response = client.post(url, json=df.to_json())
    assert(response.status_code == 200)
    assert(len(response.json) == 3)

    url = '/annotation/dataset/{}/synapse/{}'
    for k, oid in enumerate(response.json):
        response = client.get(url.format(test_dataset, oid))
        assert(response.json['pre_pt']['position'] == data[k][0])
