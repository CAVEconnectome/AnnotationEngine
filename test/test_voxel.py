
def test_voxel(client, test_dataset):
    url = '/voxel/dataset/{}/0_0_0'.format(test_dataset)
    response = client.get(url)
    assert response.status_code == 200
    assert response.json == 0


def test_bad_voxel(client):
    url = '/voxel/dataset/not_a_project/0_0_0'
    response = client.get(url)
    assert response.status_code == 404
