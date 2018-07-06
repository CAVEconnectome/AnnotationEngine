import json


def test_types(client):
    response = client.get('/schema')
    assert response.status_code == 200
    assert len(json.loads(response.data)) == 1


def test_bad_schema(client):
    response = client.get('/schema/not_a_type')
    print(response.data)
    assert(response.status_code == 404)


def test_get_synapse_schema(app, client):
    url = '/schema/synapse'.format()
    response = client.get(url)
    assert(response.status_code == 200)
    assert(len(response.data) > 0)
    # TODO make a better test
