from .synapse import SynapseSchema
from annotationengine.errors import UnknownAnnotationTypeException
from flask import Blueprint, jsonify, abort
from marshmallow_jsonschema import JSONSchema

type_mapping = {
    'synapse': SynapseSchema
}

bp = Blueprint("schema", __name__, url_prefix="/schema")


@bp.route("")
def get_schemas():
    return jsonify(get_types())


@bp.route("/<annotation_type>")
def get_type_schema(annotation_type):
    try:
        Schema = get_schema(annotation_type)
    except UnknownAnnotationTypeException:
        abort(404)
    json_schema = JSONSchema()
    js = json_schema.dump(Schema())
    return jsonify(js.data)


def get_types():
    return [k for k in type_mapping.keys()]


def get_schema(type):
    try:
        return type_mapping[type]
    except KeyError:
        msg = 'type {} is not a known annotation type'.format(type)
        raise UnknownAnnotationTypeException(msg)
