from flask import Blueprint, jsonify, abort
from annotationengine.errors import DataSetNotFoundException
from flask import current_app, g
import cloudvolume
import requests
import os
bp = Blueprint("dataset", __name__, url_prefix="/dataset")

ds_cache = {}

class MyCloudVolume(cloudvolume.CloudVolume):
    def lookup_supervoxel(self, x, y, z, scale_factor=(1, 1, 1)):
        voxel = self[int(x*scale_factor[0]),
                     int(y*scale_factor[1]),
                     int(z*scale_factor[2])]
        return int(voxel[0, 0, 0, 0])


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
            d= r.json()
            self.datasets[dataset] = d
            path = d['pychunkgraph_segmentation_source']
            vol_path = d['image_source']
            try:
                img_cv = MyCloudVolume(vol_path, mip=0)

                self.cvd[dataset] = MyCloudVolume(path, mip=0,
                                                  fill_missing=True,
                                                  cache=True)
                scale_factor = img_cv.resolution / self.cvd[dataset].resolution
                self.scale_factors[dataset] = scale_factor
            except Exception as e:
                print('dataset {} failed to load because {}'.format(dataset, e))
                self.datasets.pop(dataset)

    def get_dataset_names(self):
        return [d for d in self.datasets.keys()]

    def get_cloudvolume(self, dataset):
        try:
            return self.cvd[dataset]
        except KeyError:
            msg = 'dataset {} not found'.format(dataset)
            raise DataSetNotFoundException(msg)

    def get_scale_factor(self, dataset):
        try:
            return self.scale_factors[dataset]
        except KeyError:
            msg = 'dataset {} not found'.format(dataset)
            raise DataSetNotFoundException(msg)

    def get_dataset(self, dataset):
        try:
            return self.datasets[dataset]
        except KeyError:
            msg = 'dataset {} not found'.format(dataset)
            raise DataSetNotFoundException(msg)

    def lookup_supervoxel(self, dataset, x, y, z):
        cv = self.get_cloudvolume(dataset)
        sf = self.get_scale_factor(dataset)
        return cv.lookup_supervoxel(x, y, z,sf)


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
