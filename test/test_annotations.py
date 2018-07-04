import json


def test_types(client, app):
    response = client.get('/annotation')
    assert response.status_code == 200
    assert len(json.loads(response.data)) == 1
