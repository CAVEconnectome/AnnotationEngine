from typing import Iterator, Dict, Any, Optional
from datetime import datetime
from io import StringIO, TextIOBase
from emannotationschemas import get_schema
from emannotationschemas.models import split_annotation_schema
from multiwrapper import multiprocessing_utils as mu
from dynamicannotationdb.interface import AnnotationDB
import pandas as pd
from collections import OrderedDict
import time


def _process_dataframe_worker(args):
    ind, dataframe, schema, schema_name, data_mapping, sql_uri = args
    annotations = []
    segmentations = []


    insert_time = datetime.now()
    flat_annotation_schema, flat_segmentation_schema = split_annotation_schema(schema)
    for index, row in dataframe.iterrows():
        ann = dict(row)

        ordered_anno_data = OrderedDict.fromkeys(data_mapping['anno_model'])
        ordered_seg_data = OrderedDict.fromkeys(data_mapping['seg_model'])

        annotation_data, segmentation_data = map(lambda keys: {x: ann[x] for x in keys}, 
                                        [data_mapping['position_keys'], data_mapping['segmentation_keys']])
        
        position_data = {key: f"POINTZ({val[0]} {val[1]} {val[2]})" for key, val in annotation_data.items() if 'position' in key}                                       
        
        ordered_anno_data.update({
             'id': index,
             'created': insert_time,
             'deleted': None,
             'superceded_id': None,
             'type': schema_name,
             'valid': True,
             'size': None
             }
        )

        ordered_anno_data.update(position_data)

        ordered_seg_data.update({
             'id': index,
             'annotation_id': index,            
        })

        annotations.append(ordered_anno_data)
        segmentations.append(ordered_seg_data)
        
    return ind, annotations, segmentations

def common_column_set(columns_a, columns_b):
    columns_set_a = set(columns_a)
    columns_set_b = set(columns_b)
    if (columns_set_a & columns_set_b):
        return columns_set_a & columns_set_b
    else:
        return ("There are no common elements")

def process_dataframe(sql_uri, table_id: str, 
                              schema, 
                              schema_name: str, 
                              dataframe, 
                              data_mapping: dict, 
                              chunksize: int, 
                              n_threads: int=16):

    timing = []
    multi_args = []
    
    schema = schema(context={'postgis': True})

    for i_start in range(0, len(dataframe), chunksize):
        multi_args.append([i_start, dataframe[i_start: i_start + chunksize],
                           schema, schema_name, data_mapping, sql_uri])       

    multi_start = time.time()
    print("START")

    results = mu.multiprocess_func(_process_dataframe_worker, multi_args, 
                                   n_threads=n_threads, debug=n_threads==1)

    multi_end = time.time()
    print(f"MULTI PROCESSING TIME: {multi_end-multi_start}")
    return results


def test_sqlalchemy_orm_bulk_insert(sql_uri, table_id, 
                                             schema_name,
                                             dataframe,
                                             chunksize):
    data_mapping = {}

    client = AnnotationDB(sql_uri)
    schema = get_schema(schema_name)

    AnnotationModel = client.cached_table(table_id)
    SegmentationModel = client.cached_table(f"{table_id}_segmentation")
    
    anno_cols = AnnotationModel.__table__.columns.keys()
    seg_cols = SegmentationModel.__table__.columns.keys()
    
    if isinstance(dataframe, pd.DataFrame):
        data_cols = dataframe.columns.tolist()
        anno_matching = common_column_set(anno_cols, data_cols)
        seg_matching = common_column_set(seg_cols, data_cols)
        data_mapping['position_keys'] = anno_matching
        data_mapping['segmentation_keys'] = seg_matching

    data_mapping.update({
        'anno_model': anno_cols,
        'seg_model': seg_cols
        })
    results = process_dataframe(sql_uri, 
                                table_id,
                                schema,
                                schema_name,
                                dataframe,
                                data_mapping,
                                chunksize)

    client_engine = client.engine
    insert_start = time.time()    
    for data in results:
        copy_string_iterator(client_engine,
                             str(AnnotationModel.__table__),
                             anno_cols, 
                             data[1])

        copy_string_iterator(client_engine,
                        str(SegmentationModel.__table__),
                        seg_cols, 
                        data[2])
    insert_end = time.time()
    print(f"INSERT TIME {insert_end-insert_start}")


class StringIteratorIO(TextIOBase):
    """https://stackoverflow.com/a/12604375/2221667
    """
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buffer = ''

    def readable(self) -> bool:
        return True

    def _read_single_line(self, n: Optional[int] = None) -> str:
        while not self._buffer:
            try:
                self._buffer = next(self._iter)
            except StopIteration:
                break
        ret = self._buffer[:n]
        self._buffer = self._buffer[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read_single_line()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read_single_line(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)

def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def copy_string_iterator(engine, table_id: str,
                                 columns,
                                 annotations: Iterator[Dict[str, Any]],
                                 size: int = 8192) -> None:
    """Inspired by https://hakibenita.com/fast-load-data-python-postgresql"""
    connection = engine.connect().connection
    with connection.cursor() as cursor:

        data_iterator = StringIteratorIO((','.join(map(clean_csv_value,(
                                            data.values()
                                            ))) + '\n'
                                            for data in annotations
                                        ))

        cursor.copy_from(data_iterator, table_id, sep=',', size=size, columns=columns)

    connection.commit()

if __name__ == "__main__":

    #####   GENERATE DATA 
    import pandas as pd
    import numpy as np

    df = pd.DataFrame()

    n_samples = 1_000 # number of synapses to make

    pre_coords = np.random.randint(0,1000, size=(n_samples, 3))
    ctr_coords = np.random.randint(0,1000, size=(n_samples, 3))
    post_coords = np.random.randint(0,1000, size=(n_samples, 3))

    df['pre_pt_position'] = list(pre_coords.tolist())
    df['ctr_pt_position'] = list(ctr_coords.tolist())
    df['post_pt_position'] = list(post_coords.tolist())

    pre_root_id = np.repeat(np.arange(100),10)
    post_root_id = np.repeat(np.arange(100),10)
    df['pre_pt_root_id'] = pre_root_id
    df['post_pt_root_id'] = post_root_id

    df.to_csv('synapses_data.csv', index=False)

    synapse_df = pd.read_csv("synapses_data.csv")

    from ast import literal_eval
    
    # CONVERT csv strings to lists
    synapse_df['pre_pt_position'] = synapse_df.pre_pt_position.apply(literal_eval)
    synapse_df['ctr_pt_position'] = synapse_df.ctr_pt_position.apply(literal_eval)
    synapse_df['post_pt_position'] = synapse_df.post_pt_position.apply(literal_eval)


    sql_uri = "postgres://postgres:annodb@localhost:5432/annodb"

    client = AnnotationDB(sql_uri=sql_uri)
    
    dataset_name = 'minnie'
    table_name = 'synapse_test'
    schema_name = 'synapse'
    table_id = f"{dataset_name}_{table_name}"

    example_table_description = "This is an example description for this table"

    new_table = client.create_table(dataset_name, 
                                    table_name, 
                                    schema_name,
                                    description=example_table_description,
                                    user_id='foo@bar.com')
    tables = client.get_dataset_tables(dataset_name)
    print(tables)

    test_sqlalchemy_orm_bulk_insert(sql_uri, 'minnie_synapse_test', 'synapse', synapse_df, 100)

