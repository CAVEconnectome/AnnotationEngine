from flask import Blueprint, jsonify, abort
from annotationengine.errors import DataSetNotFoundException
from flask import current_app, g
import cloudvolume
import requests
import os
bp = Blueprint("dataset", __name__, url_prefix="/dataset")

ds_cache = {}


class DataSetStore():
    def __init__(self, infoservice):
        url = os.path.join(infoservice, "api/datasets")
        r = requests.get(url)
        print(r.status_code)
        dataset_names = r.json()

        self.cvd = {}
        self.scale_factors = {}
        self.datasets = {}
        for dataset in dataset_names:
            url = os.path.join(infoservice, "api/dataset/{}".format(dataset))
            r = requests.get(url)
            print(r.status_code)
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




@bp.route("")
def get_datasets():
    db = get_dataset_db()
    return jsonify(db.get_dataset_names())


@bp.route("/<dataset>")
def get_dataset(dataset):
    db = get_dataset_db()
    try:
        return jsonify(db.get_dataset(dataset))
    except DataSetNotFoundException:
        abort(404)


def get_dataset_db():
    if 'dataset_db' not in ds_cache:
        ds_cache['dataset_db'] = DataSetStore(current_app.config['INFOSERVICE_ENDPOINT'])
    return ds_cache['dataset_db']
