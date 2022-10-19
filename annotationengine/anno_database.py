from flask import current_app, abort
from dynamicannotationdb import DynamicAnnotationInterface
from middle_auth_client import (
    users_share_common_group
)
cache = {}


def get_db(aligned_volume) -> DynamicAnnotationInterface:
    if aligned_volume not in cache:
        sql_url = current_app.config["SQLALCHEMY_DATABASE_URI"]
        cache[aligned_volume] = DynamicAnnotationInterface(sql_url, aligned_volume)
    return cache[aligned_volume]

def check_write_permission(db, table_name):
    metadata = db.database.get_table_metadata(table_name)
    if metadata["user_id"] != str(g.auth_user["id"]):
        if metadata["write_permission"]=="GROUP":
            if not users_share_common_group(metadata["user_id"]):
                abort(401, 
                        "Unauthorized: You cannot write because you do not share a common group with the creator of this table.")
            else:
                abort(401, "Unauthorized: The table can only be written to by owner")
    return metadata

def check_read_permission(db, table_name):
    metadata = db.database.get_table_metadata(table_name)
    if metadata["read_permission"]=="GROUP":
        if not users_share_common_group(metadata["user_id"]):
            abort(401, 
                  "Unauthorized: You cannot read this table because you do not share a common group with the creator of this table.")
    elif metadata["read_permission"]=="PRIVATE":
        if metadata["user_id"] != str(g.auth_user["id"]):
            abort(401, "Unauthorized: The table can only be read by its owner")
    return metadata

def check_ownership(db, table_name):
    metadata = db.database.get_table_metadata(table_name)
    if metadata["user_id"] != str(g.auth_user["id"]):
        abort(401, "You cannot do this because you are not the owner of this table")