import pytest
import cloudvolume
import numpy as np
import os
from conftest import PYCHUNKEDGRAPH_ENDPOINT

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


def get_supervoxel_leaves(cv, root_id_vol, root_id):
    vol = cloudvolume.CloudVolume(cv)
    vol = vol[:]
    return np.unique(vol[root_id_vol == root_id])


def test_get_synapses_involving_root(client, app, test_dataset,
                                     example_synapse, cv, root_id_vol,
                                     requests_mock):
    
    root_id = 100000
    cg_url = os.path.join(PYCHUNKEDGRAPH_ENDPOINT, '/1.0/segment/{}/leaves'.format(root_id))

    seg_ids = get_supervoxel_leaves(cv, root_id_vol, root_id)
    requests_mock.get(cg_url, json=seg_ids.tolist())

    url = '/annotation/segmentation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, root_id)
    response = client.get(url)
    print(response.json)
    assert(response.status_code == 200)
    assert(len(response.json.keys()) == 1)


# def test_get_synapses_involving_root_spatial(client, test_dataset,
#                                              example_synapse):
#     root_id = to_label(test_cg, 3, 0, 0, 0, 1)
#     url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
#     url = url.format(test_dataset, root_id)
#     # this should get the synapse because the bounding box is big enough
#     j = [[0, 0, 0], [200, 200, 200]]
#     response = client.post(url, json=j)
#     assert(response.status_code == 200)
#     assert(len(response.json.keys()) == 1)

#     # if we restrict ourselves to the opposite corner we should get none
#     j = [[33, 33, 33], [64, 64, 64]]
#     response = client.post(url, json=j)
#     assert(response.status_code == 200)
#     assert(len(response.json.keys()) == 0)


# def test_bad_bounding_box(client, test_dataset,  example_synapse):
#     root_id = to_label(test_cg, 3, 0, 0, 0, 1)
#     url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
#     url = url.format(test_dataset, root_id)
#     j = [[0, 0], [200, 200]]
#     response = client.post(url, json=j)
#     assert(response.status_code == 422)
