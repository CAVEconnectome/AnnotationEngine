import json
from annotationengine.database import get_db


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
    url = '/annotation/dataset/{}/synapse'.format(test_dataset)
    response = client.post(url,
                           data=json.dumps([synapse_d]))
    assert response.status_code == 200
    response_d = json.loads(response.data)
    oid = response_d[0]
    assert(type(oid) == int)

    # test that it is now in the database
    with app.app_context():
        db = get_db()
        synapse_r = db.get_annotation(test_dataset,
                                      'synapse',
                                      response_d[0])
        assert(synapse_r is not None)
        synapse = json.loads(synapse_r)
        assert(synapse['pre_pt']['supervoxel_id'] == 5)

    url = '/annotation/dataset/{}/synapse/{}'.format(test_dataset,
                                                     oid)
    # test that the client can retreive it
    response = client.get(url)
    assert(response.status_code == 200)
    synapse_d = json.loads(response.data)
    assert(synapse_d['oid'] == response_d[0]['oid'])

    # now lets modify it and update it with put
    synapse_d['pre_pt']['position'] = [31, 30, 0]
    response = client.put(url, data=json.dumps(synapse_d))
    assert(response.status_code == 200)

    # test that it is now changed in the database
    with app.app_context():
        db = get_db()
        synapse = db.get_annotation(test_dataset,
                                    'synapse',
                                    response_d[0]['oid'])
        assert(synapse['oid'] == response_d[0]['oid'])
        assert(synapse['pre_pt']['position'] == [31, 30, 0])

    # test that we can delete it
    response = client.delete(url)
    assert(response.status_code == 200)

    # test that we get 404 when we try to get it again
    response = client.get(url)
    assert(response.status_code == 404)

    # test that we get 404 when we try to delete it again
    response = client.delete(url)
    assert(response.status_code == 404)
