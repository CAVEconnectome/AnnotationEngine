from flask import Blueprint, jsonify, request
from annotationengine.schemas import get_schema, get_types
from annotationengine.database import get_db
from marshmallow_jsonschema import JSONSchema

bp = Blueprint("annotation", __name__, url_prefix="/annotation")


@bp.route("/")
def get_valid_types():
    return jsonify(get_types())


@bp.route("/<annotation_type>", methods=["POST"])
def import_annotations(annotation_type):
    if request.method == "POST":
        db = get_db()
        # iterate through annotations in json posted
        for annotation in request.data:
            assert(annotation['type'] == annotation_type)
            schema = get_schema(annotation['type'])
            result = schema.load(annotation)
            assert(len(result.errors) == 0)

            # get a new unique ID for this annotation
            result.data['id'] = db.get_new_id()

            # put it in the database
            db.save_annotation(result.data)

        return jsonify(result.data)


@bp.route("/<annotation_type>/<oid>", methods=["GET", "PUT", "DELETE"])
def get_annotation(annotation_type, oid):
    db = get_db()
    if request.method == "PUT":
        json_d = request.data
        schema = get_schema(annotation_type)
        result = schema.load(json_d)
        assert(len(result.errors) == 0)
        assert(result.data['type'] == annotation_type)
        result.data['oid'] = oid
        db.save_annotation(result.data)
        return jsonify(schema.dump(result))

    if request.method == "DELETE":
        db.delete_annotation(annotation_type, oid)
        return "deleted: {}".format(oid)

    if request.method == "GET":
        ann = db.get_annotation(annotation_type, oid)
        schema = get_schema(annotation_type)
        return jsonify(schema.dump(ann))
        return "get: {}".format(id)


@bp.route("/<annotation_type>/schema")
def get_type_schema(annotation_type):
    schema = get_schema(annotation_type)
    json_schema = JSONSchema()
    js = json_schema.dump(schema)
    return jsonify(js.data)
