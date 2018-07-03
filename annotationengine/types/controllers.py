from flask import Blueprint  # , render_template, url_for, request, redirect

mod_types = Blueprint('types', __name__, url_prefix='/types')


@mod_types.route("/")
def types():
    return "hello types"
