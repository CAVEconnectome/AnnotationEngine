from flask import Blueprint, jsonify, abort
from annotationengine.errors import DataSetNotFoundException
from flask import current_app, g
import cloudvolume

bp = Blueprint("dataset", __name__, url_prefix="/dataset")


class MyCloudVolume(cloudvolume.CloudVolume):

    def lookup_supervoxel(self, x, y, z):
        voxel = self[x, y, z]
        return int(voxel[0, 0, 0, 0])


class DataSetStore():

    def __init__(self, datasets):
        self.datasets = datasets
        self.cvd = {}
        for dataset in datasets:
            path = dataset['CV_SEGMENTATION_PATH']
            self.cvd[dataset['name']] = MyCloudVolume(path,
                                                      mip=0,
                                                      fill_missing=True)

    def get_dataset_names(self):
        return [d['name'] for d in self.datasets]

    def get_cloudvolume(self, dataset):
        try:
            return self.cvd[dataset]
        except KeyError:
            msg = 'dataset {} not found'.format(dataset)
            raise DataSetNotFoundException(msg)

    def get_dataset(self, dataset):
        try:
            return next(d for d in self.datasets if d['name'] == dataset)
        except StopIteration:
            msg = 'dataset {} not found'.format(dataset)
            raise DataSetNotFoundException(msg)

    def lookup_supervoxel(self, dataset, x, y, z):
        cv = self.get_cloudvolume(dataset)
        return cv.lookup_supervoxel(x, y, z)


@bp.route("")
def get_datasets():
    db = get_dataset_db()
    return jsonify(db.get_dataset_names())


@bp.route("<dataset>")
def get_dataset(dataset):
    db = get_dataset_db()
    try:
        return jsonify(db.get_dataset(dataset))
    except DataSetNotFoundException:
        abort(404)


def get_dataset_db():
    if 'dataset_db' not in g:
        g.dataset_db = DataSetStore(current_app.config['DATASETS'])
    return g.dataset_db
