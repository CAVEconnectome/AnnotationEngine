from flask import jsonify, render_template, current_app, make_response, Blueprint, url_for
from .aligned_volume import get_aligned_volumes
from .anno_database import get_db
from dynamicannotationdb.models import Metadata
from dynamicannotationdb.key_utils import get_table_name_from_table_id
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
    table_names = db.get_existing_table_names()
    query=db.cached_session.query(Metadata).filter(Metadata.deleted == None)
    df = pd.read_sql(query.statement, db.engine)
    base_user_url = "https://{auth_uri}/api/v1/user/{user_id}"
    auth_uri =os.environ['AUTH_URI']
    base_schema_url = current_app.config['SCHEMA_SERVICE_ENDPOINT'] + "views/type/{schema_type}/view"
    df['user_id']=df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(base_user_url.format(auth_uri=auth_uri,
                                                                         user_id=x.user_id),
                                                    x.user_id),
                       axis=1)
    df['schema_type']=df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(base_schema_url.format(schema_type=x.schema_type),
                                                    x.schema_type),
                       axis=1)
    
    df['table_name']=df['table_name'].map(lambda x: get_table_name_from_table_id(x))
    df['table_name']=df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(url_for('views.table_view',
                                                            aligned_volume_name=aligned_volume_name,
                                                            table_name=x.table_name), x.table_name),
                       axis=1)

    return render_template('aligned_volume.html',
                            df_table=df.to_html(escape=False),
                            tables=table_names,
                            aligned_volume_name=aligned_volume_name,
                            version=__version__)

@views_bp.route("/aligned_volume/<aligned_volume_name>/table/<table_name>")
def table_view(aligned_volume_name, table_name):
    db = get_db(aligned_volume_name)
    table_size = db.get_annotation_table_size(aligned_volume_name, table_name)
    md = db.get_table_metadata(aligned_volume_name, table_name)
    return render_template('table.html',
                           aligned_volume_name=aligned_volume_name,
                           table_name=table_name,
                           table_size=table_size,
                           table_description=md['description'])