import json
from annotationengine.database import get_db


def test_synapse(client, app):
    synapse_d = {
        'type': 'synapse',
        'points': [
            {
                'position': [31, 31, 0],
            },
            {
                'position': [33, 33, 0],
            },
            {
                'position': [34, 34, 0]
            }
        ]
    }

    response = client.post('/annotation/synapse', data=json.dumps([synapse_d]))
    assert response.status_code == 200
    response_d = json.loads(response.data)
    assert('oid' in response_d[0].keys())
    assert(len(response_d[0]['points']) == 3)
    assert(response_d[0]['points'][0]['supervoxel_id'] == 5)

    # test that it is now in the database
    with app.app_context():
        db = get_db()
        synapse = db.get_annotation('synapse', response_d[0]['oid'])
        assert(synapse['oid'] == response_d[0]['oid'])

    url = '/annotation/synapse/{}'.format(synapse['oid'])
    # test that the client can retreive it
    response = client.get(url)
    assert(response.status_code == 200)
    synapse_d = json.loads(response.data)
    assert(synapse_d['oid'] == response_d[0]['oid'])
    # now lets modify it and update it with put
    print(synapse_d)
    synapse_d['points'][0]['position'] = [31, 30, 0]
    response = client.put(url, data=json.dumps(synapse_d))
    assert(response.status_code == 200)

    # test that it is now changed in the database
    with app.app_context():
        db = get_db()
        synapse = db.get_annotation('synapse', response_d[0]['oid'])
        assert(synapse['oid'] == response_d[0]['oid'])
        assert(synapse['points'][0]['position'] == [31, 30, 0])

    # test that we can delete it
    response = client.delete(url)
    assert(response.status_code == 200)

    # test that we get 404 when we try to get it again
    response = client.get(url)
    assert(response.status_code == 404)

    # test that we get 404 when we try to delete it again
    response = client.delete(url)
    assert(response.status_code == 404)


def test_get_schema(app, client):
    response = client.get('/annotation/synapse/schema')
    assert(response.status_code == 200)
    assert(len(response.data) > 0)
    # TODO make a better test