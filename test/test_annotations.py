import json


def test_types(client):
    response = client.get('/annotation')
    assert response.status_code == 200
    assert len(json.loads(response.data)) == 1


def test_bad_schema(client):
    response = client.get('/annotation/not_a_type/schema')
    print(response.data)
    assert(response.status_code == 404)


def test_post_bad_type(client):
    junk_d = {
        'type': 'junk_annotation',
        'tag': 'junk'
    }
    response = client.post('/annotation/junk_annotation',
                           data=json.dumps(junk_d))
    assert(response.status_code == 404)
