from flask import Flask, jsonify, url_for, redirect, Blueprint
import json
import numpy as np
from annotationengine.config import configure_app
from annotationengine.utils import get_instance_folder_path
from annotationengine.api import api_bp
from flask_sqlalchemy import SQLAlchemy
from flask_restx import Api
import logging

__version__ = "1.0.6"


class AEEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint64):
            return int(obj)
        return json.JSONEncoder.default(self, obj)


def create_app(test_config=None):
    # Define the Flask Object
    app = Flask(__name__,
                instance_path=get_instance_folder_path(),
                instance_relative_config=True)
    app.json_encoder = AEEncoder
    
    logging.basicConfig(level=logging.DEBUG)

    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)

    apibp = Blueprint('api', __name__, url_prefix='/annotation/api')

    with app.app_context():
        api = Api(apibp, title="Annotation Engine API", version=__version__, doc="/doc")
        api.add_namespace(apibp, path='/v2')

    @app.route("/info/health")
    def health():
        return jsonify("healthy"), 200
   
    return app
