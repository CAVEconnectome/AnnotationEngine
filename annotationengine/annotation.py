from flask import Blueprint, jsonify, request, abort
from annotationengine.schemas import get_schema, get_types
from annotationengine.database import get_db, DBMSAnnotationNotFound
from marshmallow_jsonschema import JSONSchema
import json

bp = Blueprint("annotation", __name__, url_prefix="/annotation")


@bp.route("")
def get_valid_types():
    return jsonify(get_types())


@bp.route("/<annotation_type>", methods=["POST"])
def import_annotations(annotation_type):
    if request.method == "POST":
        db = get_db()
        # iterate through annotations in json posted
        schema = get_schema(annotation_type)
        result = schema.load(json.loads(request.data), many=True)
        assert(len(result.errors) == 0)
        for annotation in result.data:
            # get a new unique ID for this annotation
            annotation['oid'] = db.get_new_id(annotation_type)
            # put it in the database
            db.save_annotation(annotation)

        return jsonify(result.data)


@bp.route("/<annotation_type>/<oid>", methods=["GET", "PUT", "DELETE"])
def get_annotation(annotation_type, oid):
    db = get_db()
    if request.method == "PUT":
        json_d = json.loads(request.data)
        schema = get_schema(annotation_type)
        result = schema.load(json_d)
        print(result)
        assert(len(result.errors) == 0)
        result.data['oid'] = int(oid)
        db.save_annotation(result.data)
        return jsonify(schema.dump(result.data))

    if request.method == "DELETE":
        db.delete_annotation(annotation_type, int(oid))
        return "deleted: {}".format(oid)

    if request.method == "GET":
        try:
            ann = db.get_annotation(annotation_type, int(oid))
            schema = get_schema(annotation_type)
            return jsonify(schema.dump(ann)[0])
        except DBMSAnnotationNotFound:
            abort(404)


@bp.route("/<annotation_type>/schema")
def get_type_schema(annotation_type):
    schema = get_schema(annotation_type)
    json_schema = JSONSchema()
    js = json_schema.dump(schema)
    return jsonify(js.data)
