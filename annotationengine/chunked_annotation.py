from flask import Blueprint, jsonify, abort, request, current_app
from annotationengine.anno_database import get_db
from emannotationschemas import get_types
import json
import requests

bp = Blueprint("chunked_annotation", __name__,
               url_prefix="/chunked_annotation")


@bp.route("/dataset/<dataset>/rootid/<root_id>/<annotation_type>",
          methods=["GET", "POST"])
def get_annotations_of_rootid(dataset, root_id, annotation_type):
    '''get all annotations from a root id'''
    anno_db = get_db()
    if annotation_type not in get_types():
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
    pychunkgraph_url = current_app.config['PYCHUNKEDGRAPH_ENDPOINT']
    url = pychunkgraph_url + '/1.0/segment/{}/leaves'.format(root_id)
    params = {}
    if bb is not None:
        params['bounds'] = bb
    r = requests.post(url, params=params)

    assert(r.status_code == 200)
    atomic_ids = r.json()
    annotations = anno_db.get_annotations_from_sv_ids(dataset, annotation_type,
                                                      atomic_ids)
    return jsonify({str(k): json.loads(v) for k, v
                    in annotations.items()})
