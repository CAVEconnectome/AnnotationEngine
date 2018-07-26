import requests
import os


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

    def bulk_import_df(self, dataset_name, annotation_type, data_df):
        """ Imports all annotations from a single dataframe in one go

        :param dataset_name: str
        :param annotation_type: str
        :param data_df: pandas DataFrame
        :return:
        """
        url = "{}/annotation/dataset/{}/{}?bulk=true".format(self.endpoint,
                                                             dataset_name,
                                                             annotation_type)
        response = self.session.post(url, json=data_df.to_json())
        assert(response.status_code == 200)
        return response.json