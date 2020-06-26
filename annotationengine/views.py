from flask import jsonify, render_template, current_app, make_response, Blueprint
from .aligned_volume import get_aligned_volumes
from .anno_database import get_db
from dynamicannotationdb.models import Metadata
import pandas as pd
import os
__version__ = "1.0.6"

views_bp = Blueprint('views', __name__, url_prefix='/annotation/views')


@views_bp.route("/")
def index():
    volumes = get_aligned_volumes()
    return render_template('aligned_volumes.html',
                            volumes=volumes,
                            version=__version__)


@views_bp.route("/aligned_volume/<aligned_volume_name>")
def aligned_volume_view(aligned_volume_name):
    
    db = get_db(aligned_volume_name)
    tables = db.get_existing_tables()
    query=db.session.query(Metadata).filter(Metadata.deleted == None)
    df = pd.read_sql(query.statement, db._client.engine)
    base_user_url = "https://{auth_uri}/api/v1/user/{user_id}"
    auth_uri =os.environ['AUTH_URI']

    df['user_id']=df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(base_user_url.format(auth_uri=auth_uri,
                                                                         user_id=x.user_id),
                                                    x.user_id),
                       axis=1)
    df['table_name']=df['table_name'].map(lambda x: x.split('__')[-1])
    return render_template('aligned_volume.html',
                            df_table=df.to_html(escape=False),
                            tables=tables,
                            aligned_volume_name=aligned_volume_name,
                            version=__version__)

@views_bp.route("/aligned_volume/<aligned_volume_name>/table/<table_name>")
def table_view(aligned_volume_name, table_name):
    return render_template('table.html',
                           aligned_volume_name=aligned_volume_name,
                           table_name=table_name)