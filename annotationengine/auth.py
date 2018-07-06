from flask import Blueprint, jsonify, request, abort
from annotationengine.schemas import get_schema, get_types
from annotationengine.database import get_db
from annotationengine.errors import AnnotationNotFoundException, \
                                    UnknownAnnotationTypeException
from marshmallow_jsonschema import JSONSchema
import json

bp = Blueprint("auth", __name__, url_prefix="/auth")

