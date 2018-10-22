from flask import Blueprint, jsonify, request, abort, current_app
from annotationengine.anno_database import get_db
from annotationengine.dataset import get_datasets
from emannotationschemas.errors import UnknownAnnotationTypeException
from annotationengine.errors import SchemaServiceError
import jsonschema
import numpy as np
import json
import pandas as pd
from multiwrapper import multiprocessing_utils as mu
import time
import collections
import os
import requests

bp = Blueprint("annotation", __name__, url_prefix="/annotation")

__version__ = "0.0.48"


def collect_bound_spatial_points(d, bsps=None, path=None):
    if bsps is None:
        bsps = []
    if path is None:
        path = []
    if 'supervoxel_id' in d.keys():
        if 'position' in d.keys():
            bsps.append({'position': d['position'], 'path': path})
    for k, v in d.items():
        if type(v) is dict:
            bsps = collect_bound_spatial_points(v, bsps, path.append(k))
    return bsps


@bp.route("/sleep/<int:sleep>")
def sleep_me(sleep):
    time.sleep(sleep)
    return "zzz... {} ... awake".format(sleep)


@bp.route("/")
def index():
    return "Annotation Engine -- version " + __version__


@bp.route("/datasets")
def get_annotation_datasets():
    return get_datasets()


def get_schemas(endpoint):
    url = os.path.join(endpoint, "type")
    r = requests.get(url)
    if (r.status_code != 200):
        raise(SchemaServiceError(r.text))
    schema_d = r.json()

    return schema_d


def get_schema(annotation_type, endpoint):
    url = os.path.join(endpoint, "/type/", annotation_type)
    r = requests.get(url)
    if (r.status_code != 200):
        raise(SchemaServiceError(r.text))
    schema_d = r.json()

    return schema_d


def get_schema_with_context(annotation_type, endpoint, flatten=False):
    context = {'flatten': flatten}
    Schema = get_schema(annotation_type)
    schema = Schema(context=context)
    return schema


def validate_ann(d, schema, schema_name):
    try:
        if type(d) == list:
            for d_i in d:
                jsonschema.validate(d_i, schema)
        else:
            jsonschema(d, schema)
    except jsonschema.ValidationError as ve:
        abort(422, ve)
    d['type'] = schema_name

    # d['type']=schema_name
    # result = schema.load(d)
    # if len(result.errors) > 0:
    #     abort(422, result.errors)
    # ann = result.data
    return d


@bp.route("/dataset/<dataset>", methods=["GET", "POST"])
def get_annotation_types(dataset):
    db = get_db()

    # make a new table
    if request.method == "POST":
        # first validate this is a valid dataset
        valid_datasets = get_datasets()
        if dataset not in valid_datasets:
            abort(404)

        # then validate the schema is a valid one
        d = request.json
        table_name = d['table_name']
        schema_name = d['schema_name']
        if schema_name not in get_schemas():
            abort(404)

        # if table already exists return 200
        types = db.get_existing_tables(dataset)

        for type_ in types:
            if type_['schema_name'] == schema_name:
                return jsonify[type_]

        user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                     request.remote_addr))
        # TODO sven make the accept both a table name and a schema name,
        # and also userid please raise an exception if this table doesn't exist
        db.create_table(dataset, d['table_name'], d['schema_name'], user_id)
        return jsonify(db.get_table_metadata(dataset, table_name))

    if request.method == "GET":

        # TODO sven make this return both the table name
        # and the schema name as a dict
        # i.e.
        # [{
        #  "table_name": "table_name",
        #  "schema_name": "schema_name"
        # }]
        types = db.get_existing_tables(dataset)
        return jsonify(types)


def nest_dictionary(d, key_path=None, sep="."):
    if key_path is None:
        key_path = []
    d_new = {}
    for k, v in d.items():
        sub_keys = k.split(sep)
        ds = d_new
        for sub_key in sub_keys[:-1]:
            if sub_key not in ds.keys():
                ds[sub_key] = {}
            ds = ds[sub_key]
        ds[sub_keys[-1]] = v
    return d_new


def _import_dataframe_thread(args):
    ind, df, schema_name, dataset, user_id, schema = args

    time_start = time.time()

    time_dict = collections.defaultdict(list)

    annotations = []

    time_dict["schema"].append(time.time() - time_start)
    for k, row in df.iterrows():
        time_start = time.time()

        d = nest_dictionary(dict(row))

        ann = validate_ann(d, schema, schema_name)

        time_dict["data"].append(time.time() - time_start)
        time_start = time.time()

        bsps = collect_bound_spatial_points(ann)

        time_dict["bound_spatial_point"].append(time.time() - time_start)
        time_start = time.time()

        blob = json.dumps(schema.dump(ann).data)

        time_dict["blob"].append(time.time() - time_start)

        annotations.append((bsps, blob))

    for k in time_dict.keys():
        print("%s - mean: %.6fs - median: %.6fs - std: %.6fs - first: %.6fs -"
              " last: %.6fs" % (k, np.mean(time_dict[k]),
                                np.median(time_dict[k]), np.std(time_dict[k]),
                                time_dict[k][0], time_dict[k][-1]))

    return ind, annotations


def import_dataframe(db, dataset, table_name, schema_name, df, user_id, schema,
                     block_size=100, n_threads=1):
    multi_args = []
    for i_start in range(0, len(df), block_size):
        multi_args.append([i_start, df[i_start: i_start + block_size],
                           schema_name, dataset, user_id, schema])

    results = mu.multiprocess_func(_import_dataframe_thread, multi_args,
                                   n_threads=n_threads)

    ind = []
    u_ids_lists = []
    for result in results:
        ind.append(result[0])
        uids = db.insert_annotations(user_id,
                                     dataset,
                                     table_name,
                                     result[1])

        u_ids_lists.append(uids)

    u_ids_lists = np.array(u_ids_lists)

    ids = np.concatenate(u_ids_lists[np.argsort(ind)])
    return ids


@bp.route("/dataset/<dataset>/<table_name>", methods=["GET", "POST"])
def import_annotations(dataset, table_name):
    db = get_db()
    is_bulk = request.args.get('bulk', 'false') == 'true'
    # TODO sven have this return the metadata dictionary
    # ... table_name, schema_name
    md = db.get_table_metadata(dataset, table_name)
    annotation_type = md['schema_name']

    if request.method == "GET":
        return jsonify(md)

    if request.method == "POST":
        schema_endpoint = current_app.config['SCHEMA_SERVICE_ENDPOINT']
        try:
            schema = get_schema(annotation_type, schema_endpoint)
        except SchemaServiceError as sse:
            abort(502, sse)
        except UnknownAnnotationTypeException as uate:
            abort(502, uate)
        user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                     request.remote_addr))

        # iterate through annotations in json posted
        d = request.json

        if is_bulk:
            df = pd.read_json(d)
            uids = import_dataframe(db, dataset, table_name, table_name, df,
                                    user_id, schema)
        else:
            anns = validate_ann(d, schema, annotation_type)
            annotations = []
            for ann in anns:
                bsps = collect_bound_spatial_points(ann)
                blob = json.dumps(schema.dump(ann).data)
                annotations.append((bsps, blob))
            # TODO you should be expecting annotations to be a list of tuples
            # with bound spatial point lists of dictionaries and blobs
            uids = db.insert_annotations(user_id,
                                         dataset,
                                         table_name,
                                         annotations)

        return jsonify(np.uint64(uids).tolist())


@bp.route("/dataset/<dataset>/<table_name>/<oid>",
          methods=["GET", "PUT", "DELETE"])
def get_annotation(dataset, table_name, oid):
    db = get_db()
    schema_endpoint = current_app.config['SCHEMA_SERVICE_ENDPOINT']
    md = db.get_table_metadata(dataset, table_name)
    annotation_type = md['schema_name']

    user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                 request.remote_addr))

    if request.method == "PUT":
        try:
            schema = get_schema(annotation_type, schema_endpoint)
        except UnknownAnnotationTypeException:
            abort(404)
        ann = validate_ann(request.json, schema, annotation_type)
        annotations = [(np.uint64(oid),
                        collect_bound_spatial_points(ann),
                        json.dumps(ann))]
        # TODO sven this like insert needs to change what it expects
        success = db.update_annotations(dataset,
                                        table_name,
                                        annotations,
                                        user_id)

        return jsonify(success)

    if request.method == "DELETE":

        success = db.delete_annotations(user_id,
                                        dataset,
                                        table_name,
                                        np.array([int(oid)], np.uint64))
        if success[0]:
            return jsonify(success[0])
        else:
            abort(404)

    if request.method == "GET":
        ann = db.get_annotation_data(dataset,
                                     table_name,
                                     int(oid))
        if ann is None:
            msg = 'annotation {} ({}) not in {}'
            msg = msg.format(oid, table_name, dataset)
            abort(404, msg)
        schema = get_schema(annotation_type, schema_endpoint)
        ann = json.loads(ann)
        ann['oid'] = oid
        return jsonify(ann)
