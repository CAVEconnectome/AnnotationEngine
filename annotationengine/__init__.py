from flask import Flask  # , render_template  # , render_template
from annotationengine.config import configure_app
from annotationengine.utils import get_instance_folder_path

__version__ = "0.0.1"

# Define the Flask Object
app = Flask(__name__,
            instance_path=get_instance_folder_path(),
            instance_relative_config=True)
app = configure_app(app)

from annotationengine.annotations.controllers import mod_annotations as annotations  # noQA: E402,E501
from annotationengine.types.controllers import mod_types as types  # noQA: E402,E501
# Register blueprint(s)
app.register_blueprint(annotations)
app.register_blueprint(types)


@app.route("/")
def index():
    return "hello world"
