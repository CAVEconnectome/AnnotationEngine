from concurrent.futures import ProcessPoolExecutor
from emannotationschemas import get_schema
from emannotationschemas.models import split_annotation_schema
from multiwrapper import multiprocessing_utils as mu
import time
from datetime import datetime
from marshmallow import Schema, EXCLUDE, INCLUDE
from dynamicannotationdb.interface import AnnotationDB
import pandas as pd
from geoalchemy2 import Geometry 
from io import StringIO
import csv

def _process_dataframe_worker(args):
    ind, data, schema, schema_name, data_mapping, sql_uri = args
    annotations, segmentations = [], []   
    insert_time = datetime.now()
    
    flat_annotation_schema, flat_segmentation_schema = split_annotation_schema(schema)

    for index, row in data.iterrows():
        ann = dict(row)
        annotation_data, segmentation_data = map(lambda keys: {x: ann[x] for x in keys}, 
                                        [data_mapping['anno_cols'], data_mapping['seg_cols']])
        position_data = {key: f"POINTZ({val[0]} {val[1]} {val[2]})" for key, val in annotation_data.items() if 'position' in key}
        
        annotation_data.update(position_data)
        annotation_data.update({
             'id': index,
             'created': insert_time,
             'valid': True,
             'type': schema_name
        })
        segmentation_data.update({
             'annotation_id': index,            
        })
        annotations.append(annotation_data)
        segmentations.append(segmentation_data)

    formatted_annotations = flat_segmentation_schema().load(annotations, many=True, unknown=INCLUDE)
    formatted_segmentations = flat_segmentation_schema().load(segmentations, many=True, unknown=INCLUDE)
    return ind, formatted_annotations, formatted_segmentations

def common_column_set(columns_a, columns_b):
    columns_set_a = set(columns_a)
    columns_set_b = set(columns_b)
    if (columns_set_a & columns_set_b):
        return columns_set_a & columns_set_b
    else:
        return ("There are no common elements")

def import_dataframe(sql_uri, table_id: str, 
                              schema, 
                              schema_name: str, 
                              data, 
                              data_mapping: dict, 
                              chunksize: int, 
                              n_threads: int=16):

    timing = []
    multi_args = []
    
    schema = schema(context={'postgis': True})

    for i_start in range(0, len(data), chunksize):
        multi_args.append([i_start, data[i_start: i_start + chunksize],
                           schema, schema_name, data_mapping, sql_uri])       

    multi_start = time.time()
    print("START")

    results = mu.multiprocess_func(_process_dataframe_worker, multi_args, 
                                   n_threads=n_threads, debug=n_threads==1)

    multi_end = time.time()

    timing.append(f"Timing: {[multi_end-multi_start]}")

    print(f"Multiprocces timing: {timing}")
    return results


def test_sqlalchemy_orm_bulk_insert(sql_uri, table_id, 
                                             schema_name,
                                             data,
                                             chunksize):
    
    client = AnnotationDB(sql_uri)
    schema = get_schema(schema_name)

    AnnotationModel = client.cached_table(table_id)
    SegmentationModel = client.cached_table(f"{table_id}_segmentation")
    anno_cols = AnnotationModel.__table__.columns.keys()
    seg_cols = SegmentationModel.__table__.columns.keys()
    column_map = {}

    if isinstance(data, pd.DataFrame):
        data_cols = data.columns.tolist()
        anno_matching = common_column_set(anno_cols, data_cols)
        seg_matching = common_column_set(seg_cols, data_cols)
        column_map['anno_cols'] = anno_matching
        column_map['seg_cols'] = seg_matching

        
    results = import_dataframe(sql_uri, 
                               table_id,
                               schema,
                               schema_name,
                               data,
                               column_map,
                               chunksize)

    client_engine = client.engine
 
    for data in results:
        client_engine.execute(AnnotationModel.__table__.insert(), data[1])
        client_engine.execute(SegmentationModel.__table__.insert(), data[2])
        # client.cached_session.bulk_insert_mappings(AnnoModel, data[1])
        # client.cached_session.bulk_insert_mappings(SegModel, data[2])
    client.commit_session()


# def psql_insert_copy(table, conn, keys, data_iter):
#     # gets a DBAPI connection that can provide a cursor
#     dbapi_conn = conn.connection
#     with dbapi_conn.cursor() as cur:
#         s_buf = StringIO()
#         writer = csv.writer(s_buf)
#         writer.writerows(data_iter)
#         s_buf.seek(0)

#         columns = ', '.join('"{}"'.format(k) for k in keys)
#         if table.schema:
#             table_name = '{}.{}'.format(table.schema, table.name)
#         else:
#             table_name = table.name

#         sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
#             table_name, columns)
#         cur.copy_expert(sql=sql, file=s_buf)

if __name__ == "__main__":

    #####   GENERATE DATA 
    import pandas as pd
    import numpy as np

    df = pd.DataFrame()

    n_samples = 1_000_000 # number of synapses to make

    pre_coords = np.random.randint(0,1000, size=(n_samples, 3))
    ctr_coords = np.random.randint(0,1000, size=(n_samples, 3))
    post_coords = np.random.randint(0,1000, size=(n_samples, 3))

    df['pre_pt_position'] = list(pre_coords.tolist())
    df['ctr_pt_position'] = list(ctr_coords.tolist())
    df['post_pt_position'] = list(post_coords.tolist())

    pre_root_id = np.repeat(np.arange(100),10000)
    post_root_id = np.repeat(np.arange(100),10000)
    df['pre_pt_root_id'] = pre_root_id
    df['post_pt_root_id'] = post_root_id

    df.to_csv('million_synapses_data.csv', index=False)

    synapse_df = pd.read_csv("C:\\workspace\\scratch\\test_dev\\Notebooks\\million_synapses_data.csv")

    from ast import literal_eval
    
    # CONVERT csv strings to lists
    synapse_df['pre_pt_position'] = synapse_df.pre_pt_position.apply(literal_eval)
    synapse_df['ctr_pt_position'] = synapse_df.ctr_pt_position.apply(literal_eval)
    synapse_df['post_pt_position'] = synapse_df.post_pt_position.apply(literal_eval)


    sql_uri = "postgres://postgres:annodb@localhost:5432/annodb"
    d = test_sqlalchemy_orm_bulk_insert(sql_uri, 'minnie_synapse_test', 'synapse', synapse_df, 100)

