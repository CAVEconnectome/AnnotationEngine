from flask import Blueprint, request
# , render_template, url_for, request, redirect

mod_annotations = Blueprint('annotations', __name__, url_prefix='/annotations')


@mod_annotations.route("/")
def types():
    return "hello annotations"


@mod_annotations.route("/import", methods=["POST"])
def import_annotations():
    return "posted! {}".format(request.data)


@mod_annotations.route("<id>", methods=["GET", "PUT", "DELETE"])
def get_annotation(id):
    if request.method == "PUT":
        return "put: {}".format(id)
    if request.method == "DELETE":
        return "deleted: {}".format(id)
    if request.method == "GET":
        return "get: {}".format(id)
