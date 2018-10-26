from annotationengine.errors import DataSetNotFoundException
from flask import current_app
import requests
import os


def get_datasets():
    infoservice = current_app.config['INFOSERVICE_ENDPOINT']
    url = os.path.join(infoservice, "api/datasets")
    r = requests.get(url)
    dataset_names = r.json()
    return dataset_names


def get_dataset(dataset):
    infoservice = current_app.config['INFOSERVICE_ENDPOINT']
    url = os.path.join(infoservice, "api/dataset/{}".format(dataset))
    r = requests.get(url)
    if r.status_code != 200:
        raise DataSetNotFoundException('dataset {} not found'.format(dataset))
    else:
        return r.json()
