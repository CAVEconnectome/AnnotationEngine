from flask import Blueprint, jsonify, request, abort, current_app
from flask_restx import Namespace, Resource, reqparse, fields
from flask_accepts import accepts, responds

from annotationengine.anno_database import get_db
from annotationengine.dataset import get_datasets
from annotationengine.errors import UnknownAnnotationTypeException
from annotationengine.errors import SchemaServiceError
from annotationengine.schemas import TableSchema
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
annotation_parser.add_argument('annotations', action='append', help='dict of annotations')    


@api_bp.route("/datasets")
class DatasetResource(Resource):
    """Dataset Info"""

    def get(self):
        """Get all Datasets """
        return jsonify(get_datasets())


def get_schema_from_service(annotation_type, endpoint):
    url = endpoint + "/type/" + annotation_type
    r = requests.get(url)
    if (r.status_code != 200):
        raise(SchemaServiceError(r.text))
    return r.json()


@api_bp.route("/dataset/create_table")
class Table(Resource):   
    
    @api_bp.doc('create_table')
    @accepts("TableSchema", schema=TableSchema, api=api_bp)
    def post(self):
        """ Create a new annotation table"""
        data = request.parsed_obj
        db = get_db()
        metadata_dict = data.get('metadata')
        logging.info(metadata_dict)
        decription = metadata_dict.get('description')
        if decription is None:
            msg = "Table description required"
            abort(404, msg)
        else:
            em_dataset = data.get('em_dataset_name')
            table_name = data.get('table_name')
            schema_type = data.get('schema_type')

            table_info = db.create_table(em_dataset,
                                         table_name,
                                         schema_type,
                                         metadata_dict)

        return table_info, 200


@api_bp.route("/dataset/count/<string:em_dataset>/<string:table_name>")
class TableInfo(Resource):

    @api_bp.doc(description="get_table_size")
    def get(self, em_dataset:str, table_name: str) -> int:
        """ Get count of rows of an annotation table"""
        table_id = f"{em_dataset}_{table_name}"
        
        db = get_db()
        return db.get_annotation_table_length(table_id), 200

@api_bp.expect(annotation_parser)
@api_bp.route("/dataset/annotations")
@api_bp.response(404, 'ID not found')
class Annotations(Resource):

    @api_bp.doc('get_annotations')
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
    
    @api_bp.doc('post_annotation')
    def post(self, **kwargs):
        """ Insert annotations """
        args = annotation_parser.parse_args()
        em_dataset = args['em_dataset']
        table_name = args['table_name']
        annotations = args['annotations']

        db = get_db()
        table_id = f"{em_dataset}_{table_name}"
        metadata = db.get_table_metadata(table_id)
        schema = metadata.get('schema_type')

        if schema:
            data = [json.loads(annotation) for annotation in annotations]
            db.insert_annotations(table_id,
                                  schema,
                                  data)

        return {f"Inserted {len(data)} annotations"}, 200
