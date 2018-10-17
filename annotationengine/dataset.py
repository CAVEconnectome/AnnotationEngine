from annotationengine.errors import DataSetNotFoundException
from flask import current_app
import requests
import os

ds_cache = {}


class DataSetStore():
    def __init__(self, infoservice):
        url = os.path.join(infoservice, "api/datasets")
        r = requests.get(url)
        dataset_names = r.json()

        self.cvd = {}
        self.scale_factors = {}
        self.datasets = {}
        for dataset in dataset_names:
            url = os.path.join(infoservice, "api/dataset/{}".format(dataset))
            r = requests.get(url)
            d = r.json()
            self.datasets[dataset] = d
            
    def get_dataset_names(self):
        return [d for d in self.datasets.keys()]

    def get_dataset(self, dataset):
        try:
            return self.datasets[dataset]
        except KeyError:
            msg = 'dataset {} not found'.format(dataset)
            raise DataSetNotFoundException(msg)


def get_dataset_db():
    if 'dataset_db' not in ds_cache:
        ds_cache['dataset_db'] = DataSetStore(current_app.config['INFOSERVICE_ENDPOINT'])
    return ds_cache['dataset_db']
