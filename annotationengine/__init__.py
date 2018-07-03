from flask import Flask, request, jsonify  # , render_template  # , render_template
from annotationengine.config import configure_app
from annotationengine.utils import get_instance_folder_path
from marshmallow_jsonschema import JSONSchema

__version__ = "0.0.1"

# Define the Flask Object
app = Flask(__name__,
            instance_path=get_instance_folder_path(),
            instance_relative_config=True)
app = configure_app(app)

from annotationengine.schemas import get_schema, get_types


@app.route("/")
def index():
    return "hello world"


@app.route("/")
def types():
    return "hello annotations"


@app.route("/annotation", methods=["POST"])
def import_annotations():
    if request.method == "PUT":
        # iterate through annotations in json posted
        for annotation in request.data:
            schema = get_schema(annotation['type'])
            result = schema.load(annotation)
            assert(len(result.errors) == 0)

    return "posted! {}".format(request.data)


@app.route("/annotation/<id>", methods=["GET", "PUT", "DELETE"])
def get_annotation(id):
    if request.method == "PUT":
        return "put: {}".format(id)
    if request.method == "DELETE":
        return "deleted: {}".format(id)
    if request.method == "GET":
        return "get: {}".format(id)


@app.route("/types/", methods=["GET"])
def get_valid_types():
    return jsonify(get_types())

@app.route("/types/<type>/schema")
def get_type_schema(type):
    schema = get_schema(type)
    print(schema)
    json_schema = JSONSchema()
    return jsonify(json_schema.dump(schema))
