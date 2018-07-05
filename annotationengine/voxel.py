from flask import Blueprint
from annotationengine.cv import get_cv

bp = Blueprint("voxel", __name__, url_prefix="/voxel")


@bp.route("/voxel/<x>_<y>_<z>")
def lookup_supervoxel(x, y, z):
    cv = get_cv()
    return cv.lookup_supervoxel(x, y, z)
