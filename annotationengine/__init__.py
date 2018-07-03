from flask import Flask, request, jsonify
# , render_template  # , render_template
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

            # get a new unique ID for this annotation
            result['id'] = 99999

            # put it in the database

    return jsonify(result.data)


@app.route("/annotation/<id>", methods=["GET", "PUT", "DELETE"])
def get_annotation(id):
    if request.method == "PUT":
        json_d = request.data
        schema = get_schema(json_d['type'])()
        result = schema.load(json_d)
        assert(len(result.errors) == 0)
        result['id'] = id
        # TODO put this updated annotation into data into the database
        return jsonify(schema.dump(result))

    if request.method == "DELETE":
        # TODO delete this annotation from the database
        return "deleted: {}".format(id)
    if request.method == "GET":
        # TODO retrieve this annotation from the database
        # ann = ?????
        # schema = get_schema(ann['type'])()
        # return jsonify(schema.dump(ann))
        return "get: {}".format(id)


@app.route("/types/", methods=["GET"])
def get_valid_types():
    return jsonify(get_types())


@app.route("/types/<type>/schema")
def get_type_schema(type):
    schema = get_schema(type)()
    json_schema = JSONSchema()
    js = json_schema.dump(schema)
    return jsonify(js.data)
