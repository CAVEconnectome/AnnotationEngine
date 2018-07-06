from flask import Blueprint, jsonify, request, abort
from annotationengine.schemas import get_schema, get_schemas
from annotationengine.database import get_db
from annotationengine.dataset import get_datasets
from annotationengine.errors import AnnotationNotFoundException, \
    UnknownAnnotationTypeException

import json

bp = Blueprint("annotation", __name__, url_prefix="/annotation")


def collect_supervoxels(d, svids=None):
    if svids is None:
        svids = set()
    for k, v in d.items():
        if k == 'supervoxel_id':
            svids.add(v)
        if type(v) is dict:
            svids = collect_supervoxels(v, svids)
    return svids


@bp.route("/dataset")
def get_annotation_datasets():
    return get_datasets()


def get_schema_with_context(annotation_type, cv):
    context = {'cv': cv}
    Schema = get_schema(annotation_type)
    schema = Schema(context=context)
    return schema


@bp.route("/dataset/<dataset>")
def get_annotation_types(dataset):
    return get_schemas()


@bp.route("/dataset/<dataset>/<annotation_type>", methods=["GET", "POST"])
def import_annotations(dataset, annotation_type):
    db = get_db()

    if request.method == "GET":
        ids = db.get_annotation_ids(dataset, annotation_type)
        return jsonify(ids)

    if request.method == "POST":
        # iterate through annotations in json posted
        try:
            schema = get_schema_with_context(annotation_type, dataset)
        except UnknownAnnotationTypeException as m:
            abort(404, str(m))
        d = json.loads(request.data)
        result = schema.load(d, many=True)
        if len(result.errors) > 0:
            abort(422, result.errors)

        user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                     request.remote_addr))
        annotations = [(collect_supervoxels(ann),
                        json.dumps(schema.dump(ann))) for ann in result.data]

        uids = db.insert_annotations(dataset,
                                     user_id,
                                     annotation_type,
                                     annotations)

        return jsonify(uids.tolist())


@bp.route("/dataset/<dataset>/<annotation_type>/<oid>",
          methods=["GET", "PUT", "DELETE"])
def get_annotation(dataset, annotation_type, oid):
    db = get_db()
    if request.method == "PUT":
        json_d = json.loads(request.data)
        try:
            schema = get_schema_with_context(annotation_type, dataset)
        except UnknownAnnotationTypeException:
            abort(404)

        result = schema.load(json_d)
        if len(result.errors) > 0:
            abort(422, result.errors)

        user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                     request.remote_addr))
        ann = result.data
        ann.pop('oid', None)
        annotations = [(collect_supervoxels(result.data),
                        oid,
                        json.dumps(schema.dump(ann).data))]

        success = db.update_annotations(dataset,
                                        user_id,
                                        annotation_type,
                                        annotations)

        return jsonify(success)

    if request.method == "DELETE":
        success = db.delete_annotations(dataset,
                                        annotation_type,
                                        [int(oid)])
        if success[0]:
            return jsonify(success[0])
        else:
            abort(404)

    if request.method == "GET":
        try:
            ann = db.get_annotation(dataset,
                                    annotation_type,
                                    int(oid))
            if ann is None:
                msg = 'annotation {} ({}) not in {}'
                msg = msg.format(oid, annotation_type, dataset)
                abort(404, msg)
            schema = get_schema_with_context(annotation_type, dataset)
            ann = json.loads(ann)
            ann['oid'] = oid
            return jsonify(schema.dump(ann)[0])
        except AnnotationNotFoundException:
            abort(404)
