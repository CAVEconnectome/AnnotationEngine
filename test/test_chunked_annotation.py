def test_get_synapses_involving_root(client, test_dataset):
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, 0)
    response = client.get(url)
    assert(response.status_code == 200)
    # TODO implement actual test of data


def test_get_synapses_involving_root_spatial(client, test_dataset):
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, 0)
    json = [[0, 0, 0], [200, 200, 200]]
    response = client.post(url, json=json)
    assert(response.status_code == 200)
    # TODO implement actual test of data back


def test_bad_bounding_box(client, test_dataset):
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, 0)
    json = [[0, 0], [200, 200]]
    response = client.post(url, json=json)
    assert(response.status_code == 420)
