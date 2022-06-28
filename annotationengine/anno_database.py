from flask import current_app
from dynamicannotationdb import DynamicAnnotationInterface

cache = {}


def get_db(aligned_volume) -> DynamicAnnotationInterface:
    if aligned_volume not in cache:
        sql_url = current_app.config["SQLALCHEMY_DATABASE_URI"]
        cache[aligned_volume] = DynamicAnnotationInterface(sql_url, aligned_volume)
    return cache[aligned_volume]
