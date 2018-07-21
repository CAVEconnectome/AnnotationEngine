from flask import Blueprint, jsonify, abort
from annotationengine.anno_database import get_db
from flask import current_app, g
import cloudvolume

bp = Blueprint("chunkedannotation", __name__, url_prefix="/chunkedannotation")


@bp.route("/dataset/<dataset>/rootid/<rootid>/<annotation_type>",
          methods=["POST"])
def get_annotations_of_rootid(dataset, rootid, annotation_type):
    '''get all annotations from a root id'''
    anno_db = get_db()
    cg = None
    bb = None

    atomic_ids = cg.get_subgraph(root_id, bounding_box=bb,
                                 bb_is_coordinate=True)
    annotations = anno_db.get_annotations_by_sv_ids(dataset, annotation_type,
                                                    atomic_ids)

    schema = get_schema_with_context(annotation_type, dataset)
    ann = json.loads(ann)
    return jsonify(schema.dump(ann, many=True))


@bp.route("/dataset/<dataset>/rootid/<rootid>/<annotation_type>", methods=["POST"])
def get_annotations_from_atomic_ids(dataset, rootid, annotation_type):
    pass
