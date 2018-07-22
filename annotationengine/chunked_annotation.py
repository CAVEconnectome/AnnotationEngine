from flask import Blueprint, jsonify, abort, request
from annotationengine.anno_database import get_db
from pychunkedgraph.master.chunkedgraph_backend import get_cg
from emannotationschemas import get_schema
from emannotationschemas.errors import UnknownAnnotationTypeException
import json

bp = Blueprint("chunkedannotation", __name__, url_prefix="/chunkedannotation")


@bp.route("/dataset/<dataset>/rootid/<root_id>/<annotation_type>",
          methods=["GET", "POST"])
def get_annotations_of_rootid(dataset, root_id, annotation_type):
    '''get all annotations from a root id'''
    anno_db = get_db()
    cg = get_cg()
    if request.method == "POST":
        bb = request.json

        if (len(bb) != 2):
            bad_box = True
        elif(len(bb[0] != 3)) | (len(bb[1] != 3)):
            bad_box = True
        else:
            bad_box = False
        if bad_box:
            error_msg = '''badly formed bounding box {}
                           [[minx,miny,minz],[maxx,maxy,maxz]]'''
            abort(420, error_msg.format(bb))

    atomic_ids = cg.get_subgraph(root_id, bounding_box=bb,
                                 bb_is_coordinate=True)
    annotations = anno_db.get_annotations_by_sv_ids(dataset, annotation_type,
                                                    atomic_ids)
    try:
        schema = get_schema(annotation_type, dataset)
    except UnknownAnnotationTypeException:
        abort(404, "annotation type {} not known".format(annotation_type))

    ann = json.loads(annotations)
    return jsonify(schema.dump(ann, many=True))


# @bp.route("/dataset/<dataset>/rootid/<rootid>/<annotation_type>",
#           methods=["POST"])
# def get_annotations_from_atomic_ids(dataset, rootid, annotation_type):
#     pass
