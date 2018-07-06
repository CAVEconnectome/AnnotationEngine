import json


def test_get_datasets(client, test_dataset):
    response = client.get('/dataset')
    assert response.status_code == 200
    assert json.loads(response.data)[0] == test_dataset


def test_get_dataset(client, test_dataset):
    response = client.get('/dataset/{}'.format(test_dataset))
    assert response.status_code == 200
    assert len(json.loads(response.data)['cv_path']) > 0
