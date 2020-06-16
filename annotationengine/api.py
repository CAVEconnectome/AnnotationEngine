from flask import Blueprint, jsonify, request, abort, current_app, g
from flask_restx import Namespace, Resource, reqparse, fields
from flask_accepts import accepts, responds

from annotationengine.anno_database import get_db
from annotationengine.dataset import get_datasets
from annotationengine.errors import UnknownAnnotationTypeException
from annotationengine.errors import SchemaServiceError
from annotationengine.schemas import CreateTableSchema, DeleteAnnotationSchema, PutAnnotationSchema

from middle_auth_client import auth_required, auth_requires_permission
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

__version__ = "1.0.6"

api_bp = Namespace("Annotation Engine", description="Annotation Engine")

annotation_parser = reqparse.RequestParser()
annotation_parser.add_argument('em_dataset', type=str, help='Name of EM Dataset')
annotation_parser.add_argument('table_name', type=str, help='Name of annotation table')
annotation_parser.add_argument('annotation_ids', type=int, action='split', help='list of annotation ids')    


@api_bp.route("/datasets")
class DatasetResource(Resource):
    """Dataset Info"""
    
    @auth_required
    def get(self):
        """Get all Datasets """
        return get_datasets(), 200


def get_schema_from_service(annotation_type, endpoint):
    url = endpoint + "/type/" + annotation_type
    r = requests.get(url)
    if (r.status_code != 200):
        raise(SchemaServiceError(r.text))
    return r.json()


@api_bp.route("/dataset/table/<string:em_dataset>")
class EMDataSetTables(Resource):
    @auth_required   
    @api_bp.doc('get_em_dataset_tables')
    def get(self, em_dataset:str):
        """ Get list of annotation tables for a dataset"""
        db = get_db()
        tables = db._client.get_dataset_tables(em_dataset)
        return tables, 200


@api_bp.route("/dataset/table")
class Table(Resource):   
    
    @auth_required
    @api_bp.doc('create_table')
    @accepts("CreateTableSchema", schema=CreateTableSchema, api=api_bp)
    def post(self):
        """ Create a new annotation table"""
        data = request.parsed_obj
        db = get_db()
        metadata_dict = data.get('metadata')
        logging.info(metadata_dict)
        decription = metadata_dict.get('description')
        if metadata_dict.get('user_id', None) is None:
            metadata_dict['user_id']=str(g.auth_user["id"])
        if decription is None:
            msg = "Table description required"
            abort(404, msg)
        else:
            em_dataset = data.get('em_dataset')
            table_name = data.get('table_name')
            schema_type = data.get('schema_type')

            table_info = db.create_table(em_dataset,
                                         table_name,
                                         schema_type,
                                         metadata_dict)

        return table_info, 200

    @auth_required   
    @api_bp.doc('get_tables')
    def get(self):
        """ Get list of annotation tables"""
        db = get_db()
        table_info = db.get_existing_tables()

        return table_info, 200

@api_bp.route("/dataset/count/<string:em_dataset>/<string:table_name>")
class TableInfo(Resource):

    @auth_required
    @api_bp.doc(description="get_table_size")
    def get(self, em_dataset:str, table_name: str) -> int:
        """ Get count of rows of an annotation table"""
        table_id = f"{em_dataset}_{table_name}"
        
        db = get_db()
        return db.get_annotation_table_length(table_id), 200

@api_bp.route("/dataset/annotations")
class Annotations(Resource):

    @auth_required
    @api_bp.doc('get annotations')
    @api_bp.expect(annotation_parser)
    def get(self, **kwargs):
        """ Get annotations by list of IDs"""
        args = annotation_parser.parse_args()
        em_dataset = args['em_dataset']
        table_name = args['table_name']
        ids = args['annotation_ids']
       
        db = get_db()

        table_id = f"{em_dataset}_{table_name}"
        metadata = db.get_table_metadata(table_id)
        schema = metadata.get('schema_type')
        ann = db.get_annotation_data(table_id, schema, ids)
        
        if ann is None:
            msg = f"annotation_id {ids} not in {table_id}"
            abort(404, msg)

        return ann, 200
    
    @auth_required
    @api_bp.doc('post annotation')
    @accepts("PutAnnotationSchema", schema=PutAnnotationSchema, api=api_bp)
    def post(self, **kwargs):
        """ Insert annotations """
        data = request.parsed_obj
        em_dataset = data.get('em_dataset')
        table_name = data.get('table_name')
        annotations = data.get('annotations')

        db = get_db()
        table_id = f"{em_dataset}_{table_name}"
        metadata = db.get_table_metadata(table_id)
        schema = metadata.get('schema_type')

        if schema:
            try:
                db.insert_annotations(table_id,
                                      schema,
                                      annotations)
            except Exception as error:
                logging.error(f"INSERT FAILED {annotations}")
                abort(404, error)
        
        return f"Inserted {len(annotations)} annotations", 200
        
    @auth_required
    @api_bp.doc('update annotation')
    @accepts("PutAnnotationSchema", schema=PutAnnotationSchema, api=api_bp)
    def put(self, **kwargs):
        """ Update annotations """
        data = request.parsed_obj
        em_dataset = data.get('em_dataset')
        table_name = data.get('table_name')
        annotations = data.get('annotations')

        db = get_db()
        table_id = f"{em_dataset}_{table_name}"
        metadata = db.get_table_metadata(table_id)
        schema = metadata.get('schema_type')

        if schema:
            new_data = [json.loads(annotation) for annotation in annotations]
            for data in new_data:
                db.update_annotation_data(table_id,
                                          schema,
                                          data)

        return f"Updated {len(data)} annotations", 200

    @auth_required
    @api_bp.doc('delete annotation')
    @accepts("DeleteAnnotationSchema", schema=DeleteAnnotationSchema, api=api_bp)
    def delete(self, **kwargs):
        """ Delete annotations """
        data = request.parsed_obj
        em_dataset = data.get('em_dataset')
        table_name = data.get('table_name')
        ids = data.get('annotation_ids')

        db = get_db()

        table_id = f"{em_dataset}_{table_name}"
        for anno_id in ids:
            ann = db.delete_annotation(table_id, anno_id)
        
        if ann is None:
            msg = f"annotation_id {ids} not in {table_id}"
            abort(404, msg)

        return ann, 200

