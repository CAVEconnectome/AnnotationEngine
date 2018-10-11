import pytest
import cloudvolume
import numpy as np
import os
from conftest import PYCHUNKEDGRAPH_ENDPOINT, get_supervoxel_leaves
import mock


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


def test_get_synapses_involving_root(client, app, test_dataset,
                                     example_synapse, cv, root_id_vol):

    with mock.patch('annotationengine.chunked_annotation.get_leaves') as MockClass:
        root_id = 1000
        MockClass.return_value = get_supervoxel_leaves(cv,
                                                       root_id_vol,
                                                       root_id)
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
