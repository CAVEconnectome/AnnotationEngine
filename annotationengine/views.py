from flask import jsonify, render_template, current_app, make_response, Blueprint
from annotationengine.dataset import get_datasets
from annotationengine.anno_database import get_db
from dynamicannotationdb.models import Metadata
import pandas as pd
import os
__version__ = "1.0.6"

views_bp = Blueprint('views', __name__, url_prefix='/annotation/views')


@views_bp.route("/")
def index():
    datasets = get_datasets()
    return render_template('datasets.html',
                            datasets=datasets,
                            version=__version__)


@views_bp.route("/dataset/<dataset_name>")
def dataset_view(dataset_name):
    
    db = get_db()
    tables = db.get_dataset_tables(dataset_name)
    query=db.session.query(Metadata).filter(Metadata.dataset_name==dataset_name).\
        filter(Metadata.deleted == None)
    df = pd.read_sql(query.statement, db._client.engine)
    base_user_url = "https://{auth_uri}/api/v1/user/{user_id}"
    auth_uri =os.environ['AUTH_URI']

    df['user_id']=df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(base_user_url.format(auth_uri=auth_uri,
                                                                         user_id=x.user_id),
                                                    x.user_id),
                       axis=1)

    return render_template('dataset.html',
                            df_table=df.to_html(escape=False),
                            tables=tables,
                            dataset_name=dataset_name,
                            version=__version__)

@views_bp.route("/dataset/<dataset_name>/table/<table_name>")
def table_view(dataset_name, table_name):
    return render_template('table.html',
                           dataset_name=dataset_name,
                           table_name=table_name)