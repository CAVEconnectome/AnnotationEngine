from flask import current_app
from dynamicannotationdb.client import AnnotationDBMeta
cache = {}

def get_db(aligned_volume)->AnnotationDBMeta:
    if aligned_volume not in cache:
        sql_uri_config = current_app.config['SQLALCHEMY_DATABASE_URI']
        cache[aligned_volume] = AnnotationDBMeta(aligned_volume=aligned_volume,
                                                 sql_base_uri=sql_uri_config)

    return cache[aligned_volume]
