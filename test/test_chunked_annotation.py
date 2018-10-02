import pytest
import cloudvolume
from itertools import product

@pytest.fixture(scope='module')
def example_synapse(client, test_dataset):
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
    return oid, synapse_d

def mock_chunkedgraph(cv):
    vol = cloudvolume.CloudVolume(cv)
    objects = [1000000=k for k in range(8)]
    xx, yy, zz = np.meshgrid(*[np.arange(0, N) for cs in chunk_size])
    id_ind = (np.uint64(xx / blockN),
                np.uint64(yy / blockN),
                np.uint64(zz / blockN))
    id_shape = (block_per_row, block_per_row, block_per_row)
    seg = np.ravel_multi_index(id_ind, id_shape)
        

def test_get_synapses_involving_root(client, app, test_dataset,
                                     example_synapse, requests_mock):
    
    root_id = 1000000
    cg_endpoint = app.config['PYCHUNKEDGRAPH_ENDPOINT']
    cg_url = os.path.join(cg_endpoint, '/1.0/segment/{}/leaves'.format(root_id))

    
    requests_mock.get(cg_url, json=[])

    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, root_id)
    response = client.get(url)
    assert(response.status_code == 200)
    assert(len(response.json.keys()) == 1)


def test_get_synapses_involving_root_spatial(client, test_dataset,
                                             example_synapse):
    root_id = to_label(test_cg, 3, 0, 0, 0, 1)
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, root_id)
    # this should get the synapse because the bounding box is big enough
    j = [[0, 0, 0], [200, 200, 200]]
    response = client.post(url, json=j)
    assert(response.status_code == 200)
    assert(len(response.json.keys()) == 1)

    # if we restrict ourselves to the opposite corner we should get none
    j = [[33, 33, 33], [64, 64, 64]]
    response = client.post(url, json=j)
    assert(response.status_code == 200)
    assert(len(response.json.keys()) == 0)


def test_bad_bounding_box(client, test_dataset,  example_synapse):
    root_id = to_label(test_cg, 3, 0, 0, 0, 1)
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, root_id)
    j = [[0, 0], [200, 200]]
    response = client.post(url, json=j)
    assert(response.status_code == 422)
