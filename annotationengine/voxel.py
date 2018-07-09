from flask import Blueprint, abort, jsonify
from annotationengine.dataset import get_dataset_db
from annotationengine.errors import DataSetNotFoundException

bp = Blueprint("voxel", __name__, url_prefix="/voxel")


@bp.route("/dataset/<dataset>/<x>_<y>_<z>")
def lookup_supervoxel(dataset, x, y, z):
    cv = get_dataset_db()
    try:
        return jsonify(cv.lookup_supervoxel(dataset,
                                            int(x),
                                            int(y),
                                            int(z)))
    except DataSetNotFoundException:
        abort(404)
