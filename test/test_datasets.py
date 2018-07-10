def test_get_datasets(client, test_dataset):
    response = client.get('/dataset')
    assert response.status_code == 200
    assert response.json[0] == test_dataset


def test_get_dataset(client, test_dataset):
    response = client.get('/dataset/{}'.format(test_dataset))
    assert response.status_code == 200
    assert response.json['name'] == test_dataset


def test_bad_dataset(client):
    response = client.get('dataset/NOT_A_DATASET')
    assert response.status_code == 404
