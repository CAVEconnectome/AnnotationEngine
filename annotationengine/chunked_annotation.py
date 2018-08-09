from flask import Blueprint, jsonify, abort, request
from annotationengine.anno_database import get_db
from pychunkedgraph.app.app_utils import get_cg
from emannotationschemas import get_schema
from emannotationschemas.errors import UnknownAnnotationTypeException
import json

bp = Blueprint("chunked_annotation", __name__,
               url_prefix="/chunked_annotation")


@bp.route("/dataset/<dataset>/rootid/<root_id>/<annotation_type>",
          methods=["GET", "POST"])
def get_annotations_of_rootid(dataset, root_id, annotation_type):
    '''get all annotations from a root id'''
    anno_db = get_db()
    cg = get_cg()
    try:
        schema = get_schema(annotation_type)
    except UnknownAnnotationTypeException:
        abort(404, "annotation type {} not known".format(annotation_type))
    bb = None
    if request.method == "POST":
        bb = request.json
        if (len(bb) != 2):
            bad_box = True
        elif(len(bb[0]) != 3) | (len(bb[1]) != 3):
            bad_box = True
        else:
            bad_box = False
        if bad_box:
            error_msg = '''badly formed bounding box {}
                           [[minx,miny,minz],[maxx,maxy,maxz]]'''
            abort(422, error_msg.format(bb))

    atomic_ids = cg.get_subgraph(int(root_id), bounding_box=bb,
                                 bb_is_coordinate=True)
    annotations = anno_db.get_annotations_from_sv_ids(dataset, annotation_type,
                                                      atomic_ids)
    return jsonify({str(k): json.loads(v) for k, v in annotations.items()})
