from flask import Blueprint, jsonify, request, abort
from annotationengine.schemas import get_schema, get_schemas
from annotationengine.anno_database import get_db
from annotationengine.dataset import get_datasets
from emannotationschemas.errors import UnknownAnnotationTypeException

import numpy as np
import json
import pandas as pd
from multiwrapper import multiprocessing_utils as mu
import time
import collections

bp = Blueprint("annotation", __name__, url_prefix="/annotation")

__version__ = "0.0.36"


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


@bp.route("/")
def index():
    return "Annotation Engine -- version " + __version__


@bp.route("/datasets")
def get_annotation_datasets():
    return get_datasets()


def get_schema_with_context(annotation_type, flatten=False):
    context = {'flatten': flatten}
    Schema = get_schema(annotation_type)
    schema = Schema(context=context)
    return schema


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
        # TODO sven make the accept both a table name and a schema name, and also userid
        # please raise an exception if this table doesn't exist
        db.create_table(dataset, d['table_name'], d['schema_name'], user_id)
        return jsonify(db.get_table_metadata(dataset, table_name))

    if request.method == "GET":

        # TODO sven make this return both the table name and the schema name as a dict
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
    ind, df, schema_name, dataset, user_id = args

    time_start = time.time()

    time_dict = collections.defaultdict(list)

    schema = get_schema_with_context(schema_name)

    annotations = []

    time_dict["schema"].append(time.time() - time_start)
    for k, row in df.iterrows():
        time_start = time.time()

        d = nest_dictionary(dict(row))
        d['type'] = schema_name
        result = schema.load(d)

        time_dict["load_schema"].append(time.time() - time_start)
        time_start = time.time()

        if len(result.errors) > 0:
            abort(422, result.errors)
        ann = result.data

        time_dict["data"].append(time.time() - time_start)
        time_start = time.time()

        bsps = collect_bound_spatial_points(ann)

        time_dict["bound_spatial_point"].append(time.time() - time_start)
        time_start = time.time()

        blob = json.dumps(schema.dump(ann).data)

        time_dict["blob"].append(time.time() - time_start)

        annotations.append((bsps, blob))

    for k in time_dict.keys():
        print("%s - mean: %.6fs - median: %.6fs - std: %.6fs - first: %.6fs - last: %.6fs" %
              (k, np.mean(time_dict[k]), np.median(time_dict[k]), np.std(time_dict[k]),
               time_dict[k][0], time_dict[k][-1]))

    return ind, annotations


def import_dataframe(db, dataset, table_name, schema_name, df, user_id,
                     block_size=100, n_threads=1):
    multi_args = []
    for i_start in range(0, len(df), block_size):
        multi_args.append([i_start, df[i_start: i_start + block_size],
                           schema_name, dataset, user_id])

    results = mu.multiprocess_func(_import_dataframe_thread, multi_args,
                                   n_threads=n_threads)

    ind = []
    u_ids_lists = []
    for result in results:
        ind.append(result[0])
        uids = db.insert_annotations(dataset,
                                     table_name,
                                     result[1],
                                     user_id)

        u_ids_lists.append(uids)

    u_ids_lists = np.array(u_ids_lists)

    ids = np.concatenate(u_ids_lists[np.argsort(ind)])
    return ids


@bp.route("/dataset/<dataset>/<table_name>", methods=["GET", "POST"])
def import_annotations(dataset, table_name):
    db = get_db()
    is_bulk = request.args.get('bulk', 'false') == 'true'
    # TODO sven have this return the metadata dictionary ... table_name, schema_name
    md = db.get_table_metadata(dataset, table_name)
    annotation_type = md['schema_name']
    if request.method == "GET":
        return jsonify(md)

    if request.method == "POST":
        user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                     request.remote_addr))

        # iterate through annotations in json posted
        d = request.json

        if is_bulk:
            df = pd.read_json(d)
            uids = import_dataframe(db, dataset, table_name, table_name, df,
                                    user_id)
        else:
            try:
                schema = get_schema_with_context(annotation_type,
                                                 dataset)
            except UnknownAnnotationTypeException as m:
                print("ABORT 404")
                abort(404, str(m))

            result = schema.load(d, many=True)
            if len(result.errors) > 0:
                abort(422, result.errors)

            annotations = []
            for ann in result.data:
                bsps = collect_bound_spatial_points(ann)
                blob = json.dumps(schema.dump(ann).data)
                annotations.append((bsps, blob))
            # TODO you should be expecting annotations to be a list of tuples
            # with bound spatial point lists of dictionaries and annotation blobs
            uids = db.insert_annotations(dataset,
                                         table_name,
                                         annotations,
                                         user_id)

        return jsonify(np.uint64(uids).tolist())


@bp.route("/dataset/<dataset>/<table_name>/<oid>",
          methods=["GET", "PUT", "DELETE"])
def get_annotation(dataset, table_name, oid):
    db = get_db()

    md = db.get_table_metadata(dataset, table_name)
    annotation_type = md['schema_name']

    user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                 request.remote_addr))

    if request.method == "PUT":
        try:
            schema = get_schema_with_context(annotation_type, dataset)
        except UnknownAnnotationTypeException:
            abort(404)

        result = schema.load(request.json)
        if len(result.errors) > 0:
            abort(422, result.errors)

        ann = result.data
        annotations = [(np.uint64(oid),
                        collect_bound_spatial_points(result.data),
                        json.dumps(schema.dump(ann).data))]
        # TODO sven this like insert needs to change what it expects
        success = db.update_annotations(dataset,
                                        table_name,
                                        annotations,
                                        user_id)

        return jsonify(success)

    if request.method == "DELETE":

        success = db.delete_annotations(dataset,
                                        table_name,
                                        np.array([int(oid)], np.uint64),
                                        user_id)
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
        schema = get_schema_with_context(annotation_type)
        ann = json.loads(ann)
        ann['oid'] = oid
        return jsonify(schema.dump(ann)[0])
