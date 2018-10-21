from flask import Blueprint, jsonify, request, abort
from annotationengine.schemas import get_schema, get_schemas
from annotationengine.anno_database import get_db
from annotationengine.dataset import get_datasets, get_dataset_db
from emannotationschemas.errors import UnknownAnnotationTypeException
import numpy as np
import json
from functools import partial
import pandas as pd
from multiwrapper import multiprocessing_utils as mu
import time
import collections

bp = Blueprint("annotation", __name__, url_prefix="/annotation")

__version__ = "0.0.46"

def collect_supervoxels(d):
    svid_set = collect_supervoxels_recursive(d)
    return np.array(list(svid_set), dtype=np.uint64)


def collect_supervoxels_recursive(d, svids=None):
    if svids is None:
        svids = set()
    for k, v in d.items():
        if k == 'supervoxel_id':
            svids.add(v)
        if type(v) is dict:
            svids = collect_supervoxels_recursive(v, svids)
    return svids

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


def bsp_import_fn(cv, scale_factor, item):
    item.pop('root_id', None)
    svid = cv.lookup_supervoxel(*item['position'], scale_factor)
    item['supervoxel_id'] = svid


def get_schema_with_context(annotation_type, dataset, flatten=False):
    dataset_db = get_dataset_db()
    cv = dataset_db.get_cloudvolume(dataset)
    scale_factor = dataset_db.get_scale_factor(dataset)
    myp = partial(bsp_import_fn, cv, scale_factor)

    context = {'bsp_fn': myp, 'flatten': flatten}
    Schema = get_schema(annotation_type)
    schema = Schema(context=context)
    return schema


@bp.route("/dataset/<dataset>")
def get_annotation_types(dataset):
    return get_schemas()


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
    ind, df, annotation_type, dataset, user_id = args

    time_start = time.time()

    time_dict = collections.defaultdict(list)

    schema = get_schema_with_context(annotation_type,
                                     dataset)

    annotations = []

    time_dict["schema"].append(time.time() - time_start)
    for k, row in df.iterrows():
        time_start = time.time()

        d = nest_dictionary(dict(row))
        d['type'] = annotation_type
        result = schema.load(d)

        time_dict["load_schema"].append(time.time() - time_start)
        time_start = time.time()

        if len(result.errors) > 0:
            abort(422, result.errors)
        ann = result.data

        time_dict["data"].append(time.time() - time_start)
        time_start = time.time()

        supervoxels = collect_supervoxels(ann)


        time_dict["supervoxel"].append(time.time() - time_start)
        time_start = time.time()

        blob = json.dumps(schema.dump(ann).data)


        time_dict["blob"].append(time.time() - time_start)

        annotations.append((supervoxels, blob))

    for k in time_dict.keys():
        print("%s - mean: %.6fs - median: %.6fs - std: %.6fs - first: %.6fs -"
              " last: %.6fs" % (k, np.mean(time_dict[k]),
                                np.median(time_dict[k]), np.std(time_dict[k]),
                                time_dict[k][0], time_dict[k][-1]))

    return ind, annotations


def import_dataframe(db, dataset, annotation_type, df, user_id,
                     block_size=100, n_threads=1):
    multi_args = []
    for i_start in range(0, len(df), block_size):
        multi_args.append([i_start, df[i_start: i_start + block_size],
                           annotation_type, dataset, user_id])

    results = mu.multiprocess_func(_import_dataframe_thread, multi_args,
                                   n_threads=n_threads)

    ind = []
    u_ids_lists = []
    for result in results:
        ind.append(result[0])
        uids = db.insert_annotations(dataset,
                                     annotation_type,
                                     result[1],
                                     user_id)

        u_ids_lists.append(uids)

    u_ids_lists = np.array(u_ids_lists)

    ids = np.concatenate(u_ids_lists[np.argsort(ind)])
    return ids


@bp.route("/dataset/<dataset>/<annotation_type>", methods=["POST"])
def import_annotations(dataset, annotation_type):
    db = get_db()
    is_bulk = request.args.get('bulk', 'false') == 'true'

    print("BULK", is_bulk)
    print(request.method)
    if request.method == "POST":
        user_id = jsonify(origin=request.headers.get('X-Forwarded-For',
                                                     request.remote_addr))

        # iterate through annotations in json posted
        d = request.json

        if is_bulk:
            df = pd.read_json(d)
            uids = import_dataframe(db, dataset, annotation_type, df,
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
                supervoxels = collect_supervoxels(ann)
                blob = json.dumps(schema.dump(ann).data)
                annotations.append((supervoxels, blob))

            uids = db.insert_annotations(dataset,
                                         annotation_type,
                                         annotations,
                                         user_id)

        return jsonify(np.uint64(uids).tolist())


@bp.route("/dataset/<dataset>/<annotation_type>/<oid>",
          methods=["GET", "PUT", "DELETE"])
def get_annotation(dataset, annotation_type, oid):
    db = get_db()
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
                        collect_supervoxels(result.data),
                        json.dumps(schema.dump(ann).data))]

        success = db.update_annotations(dataset,
                                        annotation_type,
                                        annotations,
                                        user_id)

        return jsonify(success)

    if request.method == "DELETE":

        success = db.delete_annotations(dataset,
                                        annotation_type,
                                        np.array([int(oid)], np.uint64),
                                        user_id)
        if success[0]:
            return jsonify(success[0])
        else:
            abort(404)

    if request.method == "GET":
        ann = db.get_annotation_data(dataset,
                                     annotation_type,
                                     int(oid))
        if ann is None:
            msg = 'annotation {} ({}) not in {}'
            msg = msg.format(oid, annotation_type, dataset)
            abort(404, msg)
        schema = get_schema_with_context(annotation_type, dataset)
        ann = json.loads(ann)
        ann['oid'] = oid
        return jsonify(schema.dump(ann)[0])
