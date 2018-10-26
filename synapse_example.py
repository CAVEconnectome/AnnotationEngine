import pandas as pd
import os
import numpy as np
import time

from annotationengine import annotation, anno_database
from dynamicannotationdb.annodb_meta import AnnotationMetaDB

HOME = os.path.expanduser("~")


def load_synapses(path=HOME + "/Downloads/pinky100_final.df",
                  scaling=(1, 1, 1)):
    """ Test scenario using real synapses """

    scaling = np.array(list(scaling))

    df = pd.read_csv(path)

    locs = np.array(df[["presyn_x", "centroid_x", "postsyn_x"]])

    mask = ~np.any(np.isnan(locs), axis=1)

    df = df[mask]

    df['pre_pt.position'] = list((np.array(df[['presyn_x', 'presyn_y', 'presyn_z']]) / scaling).astype(np.int))
    df['ctr_pt.position'] = list((np.array(df[['centroid_x', 'centroid_y', 'centroid_z']]) / scaling).astype(np.int))
    df['post_pt.position'] = list((np.array(df[['postsyn_x', 'postsyn_y', 'postsyn_z']]) / scaling).astype(np.int))

    df = df[['pre_pt.position', 'ctr_pt.position', 'post_pt.position', 'size']]

    return df


def insert_synases_no_endpoint(syn_df, dataset_name='pinky100',
                               schema_name="synapse",
                               table_name="pni_synapses",
                               user_id="PNI"):
    schema_endpoint = "https://www.dynamicannotationframework.com/schema"

    schema = annotation.get_schema_from_service(schema_name,
                                                schema_endpoint)

    amdb = AnnotationMetaDB()
    amdb._reset_table(user_id, dataset_name, table_name, schema_name)

    annotation.import_dataframe(amdb, dataset_name, table_name, schema_name,
                                syn_df, user_id, schema, n_threads=1)

if __name__ == "__main__":

    print("LOADING synapses")

    time_start = time.time()
    syn_df = load_synapses()
    print("Time for loading: %.2fmin" % ((time.time() - time_start) / 60))

    time_start = time.time()
    insert_synases_no_endpoint(syn_df)
    print("Time for inserting: %.2fmin" % ((time.time() - time_start) / 60))