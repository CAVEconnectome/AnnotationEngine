import requests
import os
import numpy as np
import cloudvolume
import json
import time

from emannotationschemas.utils import get_flattened_bsp_keys_from_schema
from emannotationschemas import get_schema


class AnnotationClient(object):
    def __init__(self, endpoint=None):
        """

        :param endpoint: str or None
            server url
        """
        if endpoint is None:
            endpoint = os.environ.get('ANNOTATION_ENGINE_ENDPOINT', None)
        assert(endpoint is not None)

        self.endpoint = endpoint
        self.session = requests.session()

    def get_datasets(self):
        """ Returns existing datasets

        :return: list
        """
        url = "{}/dataset".format(self.endpoint)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response

    def get_dataset(self, dataset_name):
        """ Returns information about the dataset

        :return: dict
        """
        url = "{}/dataset/{}".format(self.endpoint, dataset_name)
        response = self.session.get(url, verify=False)
        assert(response.status_code == 200)
        return response

    def bulk_import_df(self, dataset_name, annotation_type, data_df,
                       block_size=10000):
        """ Imports all annotations from a single dataframe in one go

        :param dataset_name: str
        :param annotation_type: str
        :param data_df: pandas DataFrame
        :return:
        """
        dataset_info = json.loads(self.get_dataset(dataset_name).content)
        cv = cloudvolume.CloudVolume(dataset_info["CV_SEGMENTATION_PATH"])
        chunk_size = np.array(cv.info["scales"][0]["chunk_sizes"][0]) * 8
        bounds = np.array(cv.bounds.to_list()).reshape(2, 3)

        Schema = get_schema(annotation_type)
        schema = Schema()

        rel_column_keys = get_flattened_bsp_keys_from_schema(schema)

        data_df = data_df.reset_index(drop=True)

        bspf_coords = []
        for rel_column_key in rel_column_keys:
            bspf_coords.append(
                np.array(data_df[rel_column_key].values.tolist())[:, None, :])

        bspf_coords = np.concatenate(bspf_coords, axis=1)
        bspf_coords -= bounds[0]
        bspf_coords = (bspf_coords / chunk_size).astype(np.int)

        bspf_coords = bspf_coords[:, 0]
        ind = np.lexsort(
            (bspf_coords[:, 0], bspf_coords[:, 1], bspf_coords[:, 2]))

        data_df = data_df.reindex(ind)

        url = "{}/annotation/dataset/{}/{}?bulk=true".format(self.endpoint,
                                                             dataset_name,
                                                             annotation_type)
        n_blocks = int(np.ceil(len(data_df) / block_size))

        print("Number of blocks: %d" % n_blocks)
        time_start = time.time()

        responses = []
        for i_block in range(0, len(data_df), block_size):
            if i_block > 0:
                dt = time.time() - time_start
                eta = dt / i_block * len(data_df) - dt
                print("%d / %d - dt = %.2fs - eta = %.2fs" %
                      (i_block, len(data_df), dt, eta))

            data_block = data_df[i_block: i_block + block_size].to_json()
            response = self.session.post(url, json=data_block, verify=False)
            assert(response.status_code == 200)
            responses.append(response.json)

        return responses
