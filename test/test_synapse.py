import json
from annotationengine.database import get_db


def test_junk_synapse(client, test_dataset):
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


def test_synapse(client, app, test_dataset):
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

    # test that it is now in the database
    with app.app_context():
        db = get_db()
        synapse_r = db.get_annotation_data(test_dataset,
                                           'synapse',
                                           oid)
        assert(synapse_r is not None)
        synapse = json.loads(synapse_r)
        assert(synapse['pre_pt']['supervoxel_id'] == 5)

    url = '/annotation/dataset/{}/synapse/{}'.format(test_dataset,
                                                     oid)
    # test that the client can retreive it
    response = client.get(url)
    assert(response.status_code == 200)
    synapse_d = response.json
    assert(synapse_d['pre_pt']['supervoxel_id'] == 5)

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

    # test that it is now changed in the database
    with app.app_context():
        db = get_db()
        synapse = db.get_annotation_data(test_dataset,
                                         'synapse',
                                         oid)
        synapse = json.loads(synapse)
        assert(synapse['pre_pt']['position'] == [31.0, 30.0, 0.0])
        print('ann_ids in 10:', db.get_annotation_ids_from_sv(
            test_dataset, 'synapse', 10))
        print('ann_ids in 6:', db.get_annotation_ids_from_sv(
            test_dataset, 'synapse', 5))

    # test that we can delete it
    response = client.delete(url)
    assert(response.status_code == 200)

    # test that we get 404 when we try to get it again
    response = client.get(url)
    assert(response.status_code == 404)

    # test that we get 404 when we try to delete it again
    response = client.delete(url)
    assert(response.status_code == 404)
