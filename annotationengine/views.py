from flask import (
    render_template,
    current_app,
    Blueprint,
    url_for,
)
from .aligned_volume import get_aligned_volumes
from .anno_database import get_db, check_read_permission
from dynamicannotationdb.models import AnnoMetadata as Metadata
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKBElement
from middle_auth_client import auth_requires_permission
import pandas as pd
import numpy as np
import os

__version__ = "4.12.9"

views_bp = Blueprint("views", __name__, url_prefix="/annotation/views")


def wkb_to_numpy(wkb, convert_to_nm=None):
    """ Fixes single geometry column """
    shp = to_shape(wkb)
    xyz_voxel = np.array([shp.xy[0][0], shp.xy[1][0], shp.z], dtype=np.int)
    if convert_to_nm is not None:
        return xyz_voxel * convert_to_nm
    else:
        return xyz_voxel


def fix_wkb_columns(df, convert_to_nm=None):
    """Fixes geometry columns

    Parameters
    ----------
    df : pd.DataFrame
        dataframe of results to fix columns with WKB postgis columns
    convert_to_nm : len(3) iterable
        the x,y,z conversion factor to convert the coordinate system to nm
        default None leaves it as voxel resolution
    """
    if convert_to_nm is not None:
        if len(convert_to_nm) != 3:
            raise ValueError("convert_to_nm must be length 3")
    if len(df) > 0:
        for colname in df.columns:
            if isinstance(df.at[0, colname], WKBElement):
                df[colname] = df[colname].apply(wkb_to_numpy, convert_to_nm)
    return df


@views_bp.route("/")
def index():
    volumes = get_aligned_volumes()
    return render_template("aligned_volumes.html", volumes=volumes, version=__version__)


@views_bp.route("/aligned_volume/<aligned_volume_name>")
@auth_requires_permission(
    "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
)
def aligned_volume_view(aligned_volume_name):

    db = get_db(aligned_volume_name)
    table_names = db.database._get_existing_table_names()
    query = (
        db.database.cached_session.query(Metadata)
        .filter(Metadata.deleted == None)
        .filter(Metadata.valid == True)
    )
    df = pd.read_sql(query.statement, db.database.engine)
    base_user_url = "https://{auth_uri}/api/v1/user/{user_id}"
    auth_uri = os.environ["AUTH_URI"]
    base_schema_url = (
        current_app.config["SCHEMA_SERVICE_ENDPOINT"] + "/views/type/{schema_type}/view"
    )
    df["user_id"] = df.apply(
        lambda x: "<a href='{}'>{}</a>".format(
            base_user_url.format(auth_uri=auth_uri, user_id=x.user_id), x.user_id
        ),
        axis=1,
    )
    df["schema_type"] = df.apply(
        lambda x: "<a href='{}'>{}</a>".format(
            base_schema_url.format(schema_type=x.schema_type), x.schema_type
        ),
        axis=1,
    )

    df["table_name"] = df.apply(
        lambda x: "<a href='{}'>{}</a>".format(
            url_for(
                "views.table_view",
                aligned_volume_name=aligned_volume_name,
                table_name=x.table_name,
            ),
            x.table_name,
        ),
        axis=1,
    )
    return render_template(
        "aligned_volume.html",
        df_table=df.to_html(escape=False),
        tables=table_names,
        aligned_volume_name=aligned_volume_name,
        version=__version__,
    )


@views_bp.route("/aligned_volume/<aligned_volume_name>/table/<table_name>")
@auth_requires_permission(
    "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
)
def table_view(aligned_volume_name, table_name):
    db = get_db(aligned_volume_name)
    check_read_permission(db, table_name)
    md = db.database.get_table_metadata(table_name)
    if md['reference_table']:
        RefModel = db.database.cached_table(md['reference_table'])
    Model = db.database._get_model_from_table_name(table_name)
    table_size = db.database.get_annotation_table_size(table_name)
    query = db.database.cached_session.query(Model).limit(15)
    top15_df = pd.read_sql(query.statement, db.database.engine)
    top15_df = fix_wkb_columns(top15_df)
    return render_template(
        "table.html",
        aligned_volume_name=aligned_volume_name,
        table_name=table_name,
        table_size=table_size,
        df_table=top15_df.to_html(escape=False),
        table_description=md["description"],
    )
