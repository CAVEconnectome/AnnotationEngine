import numpy as np
import pytest
from itertools import product
import os
from pychunkedgraph.backend import chunkedgraph


def create_chunk(cgraph, vertices=None, edges=None, timestamp=None):
    """
    Helper function to add vertices and edges to the chunkedgraph - no safety checks!
    """
    if not vertices:
        vertices = []

    if not edges:
        edges = []

    vertices = np.unique(np.array(vertices, dtype=np.uint64))
    edges = [(np.uint64(v1), np.uint64(v2), np.float32(aff))
             for v1, v2, aff in edges]
    edge_ids = []
    cross_edge_ids = []
    edge_affs = []
    cross_edge_affs = []
    isolated_node_ids = [x for x in vertices if (x not in [edges[i][0] for i in range(len(edges))]) and
                                                (x not in [edges[i][1] for i in range(len(edges))])]

    for e in edges:
        if cgraph.test_if_nodes_are_in_same_chunk(e[0:2]):
            edge_ids.append([e[0], e[1]])
            edge_affs.append(e[2])
        else:
            cross_edge_ids.append([e[0], e[1]])
            cross_edge_affs.append(e[2])

    edge_ids = np.array(edge_ids, dtype=np.uint64).reshape(-1, 2)
    edge_affs = np.array(edge_affs, dtype=np.float32).reshape(-1, 1)
    cross_edge_ids = np.array(cross_edge_ids, dtype=np.uint64).reshape(-1, 2)
    cross_edge_affs = np.array(
        cross_edge_affs, dtype=np.float32).reshape(-1, 1)
    isolated_node_ids = np.array(isolated_node_ids, dtype=np.uint64)

    cgraph.add_atomic_edges_in_chunks(edge_ids, cross_edge_ids,
                                      edge_affs, cross_edge_affs,
                                      isolated_node_ids)


def to_label(cgraph, l, x, y, z, segment_id):
    return cgraph.get_node_id(np.uint64(segment_id), layer=l, x=x, y=y, z=z)


@pytest.fixture
def test_cg(chunkgraph_tuple, N=64, blockN=16):
    """
    Create graph where a 4x4x4 grid of supervoxels is connected
    into a 2x2x2 grid of root_ids
    """

    cgraph, table_id = chunkgraph_tuple

    for x, y, z in product(range(2), range(2), range(2)):
        verts = [to_label(cgraph, 1, x, y, z, k) for k in range(8)]
        create_chunk(cgraph,
                     vertices=verts,
                     edges=[(verts[k], verts[k + 1], .5) for k in range(7)])

    cgraph.add_layer(3, np.array(
        [[x, y, z] for x, y, z in product(range(2), range(2), range(2))]))

    return cgraph


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


def test_get_synapses_involving_root(client, test_dataset, test_cg, example_synapse):
    root_id = to_label(test_cg, 3, 0, 0, 0, 1)
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, root_id)
    response = client.get(url)
    assert(response.status_code == 200)
    assert(len(response.json.keys()) == 1)


def test_get_synapses_involving_root_spatial(client, test_dataset, test_cg, example_synapse):
    root_id = to_label(test_cg, 3, 0, 0, 0, 1)
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, root_id)
    # this should get the synapse because the bounding box is big enough
    j = [[0, 0, 0], [200, 200, 200]]
    response = client.post(url, json=j)
    assert(response.status_code == 200)
    assert(len(response.json.keys()) == 1)

    # if we restrict ourselves to the opposite corner of 64x64x64 cube we should get none
    j = [[33, 33, 33], [64, 64, 64]]
    response = client.post(url, json=j)
    assert(response.status_code == 200)
    assert(len(response.json.keys()) == 0)

def test_bad_bounding_box(client, test_dataset, test_cg, example_synapse):
    root_id = to_label(test_cg, 3, 0, 0, 0, 1)
    url = '/chunked_annotation/dataset/{}/rootid/{}/synapse'
    url = url.format(test_dataset, root_id)
    j = [[0, 0], [200, 200]]
    response = client.post(url, json=j)
    assert(response.status_code == 422)
