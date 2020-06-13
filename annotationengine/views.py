from flask import jsonify, render_template, current_app, make_response, Blueprint
from annotationengine.dataset import get_datasets
from anno_database import get_db

__version__ = "1.0.6"

views_bp = Blueprint('views', __name__, url_prefix='/views')


@views_bp.route("/")
def index():
    datasets = get_datasets()
    return render_template('datasets.html',
                            datasets=datasets,
                            version=__version__)


@views_bp.route("/dataset/<datasetname>")
def dataset_view(datasetname):
    
    db = get_db()
    tables = db._client.get_dataset_tables(datasetname)
    
    return render_template('dataset.html',
                            tables=tables,
                            version=__version__)

