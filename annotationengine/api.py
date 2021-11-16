from flask import Blueprint, config, jsonify, request, abort, current_app, g, Response
from flask_restx import Namespace, Resource, reqparse, fields
from flask_accepts import accepts, responds
from annotationengine.anno_database import get_db
from annotationengine.aligned_volume import get_aligned_volumes
from annotationengine.errors import UnknownAnnotationTypeException
from annotationengine.errors import SchemaServiceError
from annotationengine.schemas import (
    CreateTableSchema,
    DeleteAnnotationSchema,
    PutAnnotationSchema,
    FullMetadataSchema,
)
from annotationengine.api_examples import synapse_table_example
from dynamicannotationdb.errors import TableAlreadyExists, TableNameNotFound
from middle_auth_client import (
    auth_required,
    auth_requires_permission,
    auth_requires_admin,
)
from jsonschema import validate, ValidationError
import numpy as np
import json
import pandas as pd
from multiwrapper import multiprocessing_utils as mu
import time
import collections
import os
import requests
import logging
from enum import Enum
from typing import List

__version__ = "3.4.1"

authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

api_bp = Namespace(
    "Annotation Engine", authorizations=authorizations, description="Annotation Engine"
)

annotation_parser = reqparse.RequestParser()
annotation_parser.add_argument(
    "annotation_ids", type=int, action="split", help="list of annotation ids"
)


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")


def get_schema_from_service(annotation_type, endpoint):
    url = endpoint + "/type/" + annotation_type
    r = requests.get(url)
    if r.status_code != 200:
        raise (SchemaServiceError(r.text))
    return r.json()


@api_bp.route("/aligned_volume/<string:aligned_volume_name>/table")
class Table(Resource):
    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("create_table", security="apikey", example=synapse_table_example)
    @accepts("CreateTableSchema", schema=CreateTableSchema, api=api_bp)
    def post(self, aligned_volume_name: str):
        """Create a new annotation table"""
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        db = get_db(aligned_volume_name)
        metadata_dict = data.get("metadata")
        logging.info(metadata_dict)
        description = metadata_dict.get("description")
        if metadata_dict.get("user_id", None) is None:
            metadata_dict["user_id"] = str(g.auth_user["id"])
        if description is None:
            msg = "Table description required"
            abort(404, msg)
        else:
            table_name = data.get("table_name")
            headers = None
            if not table_name.islower():
                headers = {
                    "Warning": f'201 - "Table name "{table_name}" needs to be lower case. Table will be posted to the database as "{table_name.lower()}"'
                }
                table_name = table_name.lower()
            schema_type = data.get("schema_type")
            table_name = table_name.lower().replace(" ", "_")
            try:
                table_info = db.create_annotation_table(
                    table_name, schema_type, **metadata_dict
                )
            except (TableAlreadyExists):
                abort(400, f"Table {table_name} already exists")
        return Response(table_info, headers=headers)

    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("get_aligned_volume_tables", security="apikey")
    def get(self, aligned_volume_name: str):
        """Get list of annotation tables for a aligned_volume"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        tables = db._get_existing_table_names()
        return tables, 200


@api_bp.route("/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>")
@api_bp.param("aligned_volume_name", "AlignedVolume Name")
@api_bp.param("table_name", "Name of table")
class AnnotationTable(Resource):
    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc(description="get table metadata", security="apikey")
    def get(self, aligned_volume_name: str, table_name: str) -> FullMetadataSchema:
        """Get metadata for a given table"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        return db.get_table_metadata(table_name), 200

    @auth_requires_admin
    @api_bp.doc(
        description="mark an annotation table for deletion (admin only)",
        security="apikey",
    )
    def delete(self, aligned_volume_name: str, table_name: str) -> bool:
        """Delete an annotation table (marks for deletion, will suspend materialization, admin only)"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        is_deleted = db.delete_table(table_name)
        return is_deleted, 200


@api_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/count"
)
class TableInfo(Resource):
    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc(description="get_table_size", security="apikey")
    def get(self, aligned_volume_name: str, table_name: str) -> int:
        """Get count of rows of an annotation table"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        return db.get_annotation_table_size(table_name), 200


@api_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/annotations"
)
class Annotations(Resource):
    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("get annotations", security="apikey")
    @api_bp.expect(annotation_parser)
    def get(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Get annotations by list of IDs"""
        check_aligned_volume(aligned_volume_name)
        args = annotation_parser.parse_args()

        annotation_ids = args["annotation_ids"]

        db = get_db(aligned_volume_name)

        annotations = db.get_annotations(table_name, annotation_ids)

        if annotations is None:
            msg = f"annotation_id {annotation_ids} not in {table_name}"
            abort(404, msg)

        return annotations, 200

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("post annotation", security="apikey")
    @accepts("PutAnnotationSchema", schema=PutAnnotationSchema, api=api_bp)
    def post(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Insert annotations"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        metadata = db.get_table_metadata(table_name)
        if metadata["user_id"] != str(g.auth_user["id"]):
            resp = Response("Unauthorized: You did not create this table", 401)
            return resp
        data = request.parsed_obj
        annotations = data.get("annotations")

        try:
            db.insert_annotations(table_name, annotations)
        except Exception as error:
            logging.error(f"INSERT FAILED {annotations}")
            abort(404, error)

        return f"Inserted {len(annotations)} annotations", 200

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("update annotation", security="apikey")
    @accepts("PutAnnotationSchema", schema=PutAnnotationSchema, api=api_bp)
    def put(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Update annotations"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        metadata = db.get_table_metadata(table_name)
        if metadata["user_id"] != str(g.auth_user["id"]):
            resp = Response("Unauthorized: You did not create this table", 401)
            return resp
        data = request.parsed_obj

        annotations = data.get("annotations")

        new_ids = []

        for annotation in annotations:
            updated_id = db.update_annotation(table_name, annotation)
            new_ids.append(updated_id)

        return f"{new_ids}", 200

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("delete annotation", security="apikey")
    @accepts("DeleteAnnotationSchema", schema=DeleteAnnotationSchema, api=api_bp)
    def delete(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Delete annotations"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        metadata = db.get_table_metadata(table_name)
        if metadata["user_id"] != str(g.auth_user["id"]):
            resp = Response("Unauthorized: You did not create this table", 401)
            return resp
        data = request.parsed_obj

        ids = data.get("annotation_ids")

        db = get_db(aligned_volume_name)

        ann = db.delete_annotation(table_name, ids)

        if ann is None:
            return f"annotation_id {ids} not in table {table_name}", 404

        return f"{len(ids)} annotations marked for deletion", 200
