from flask import g
import numpy as np
# this are things I am imaging sven will implement
# they are simply stubbed here
# perhaps this will remain as a wrapper around his interface


class AnnotationMetaDB(object):
    def __init__(self, instance_id="pychunkedgraph",
                 project_id="neuromancer-seung-import",
                 credentials=None):

        self._annotation_tables = {}
        self.ID = 0

    def get_existing_annotation_types(self, dataset_name):
        """ Collects annotation_types of existing tables

        Annotation tables start with `anno`

        :return: list
        """

        return self._annotation_tables[dataset_name].keys()

    def has_table(self, dataset_name, annotation_type):
        """Checks whether a table exists in the database

        :param dataset_name: str
        :param annotation_type: str
        :return: bool
            whether table already exists
        """
        if dataset_name not in self._annotation_tables.keys():
            return False
        if annotation_type not in self._annotation_tables[dataset_name].keys():
            return False
        return True

    def create_table(self, dataset_name, annotation_type):
        """ Creates new table

        :param dataset_name: str
        :param annotation_type: str
        :return: bool
            success
        """
        if dataset_name not in self._annotation_tables.keys():
            self._annotation_tables[dataset_name] = {}
        if annotation_type not in self._annotation_tables[dataset_name].keys():
            self._annotation_tables[dataset_name][annotation_type] = {}
            return True
        else:
            return False

    def insert_annotations(self,
                           dataset_name,
                           user_id,
                           annotation_type,
                           annotations):
        """ Inserts new annotations into the database and returns assigned ids

        :param dataset_name: str
        :param annotation_type: str
        :param user_id: str
        :param annotations: list of tuples
             [(sv_ids, serialized data), ...]
        :return: list of uint64
            assigned ids (in same order as `annotations`)
        """
        if dataset_name not in self._annotation_tables.keys():
            self.create_table(dataset_name, annotation_type)
        if annotation_type not in self._annotation_tables[dataset_name].keys():
            self.create_table(dataset_name, annotation_type)
        ids = np.arange(self.ID,
                        self.ID + len(annotations),
                        dtype=np.uint64)
        table = self._annotation_tables[dataset_name][annotation_type]
        print('annotations',annotations)
        for oid, annotation in zip(ids, annotations):
            table[oid] = annotation[1]
        print(table)
        return ids

    def delete_annotations(self,
                           dataset_name,
                           annotation_type,
                           annotation_ids):
        """ Deletes annotations from the database

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_ids: list of uint64s
        :return: list[bool]
            success
        """
        if not self.has_table(dataset_name, annotation_type):
            return False

        success = []
        for annotation_id in annotation_ids:
            try:
                table = self._annotation_tables[dataset_name][annotation_type]
                table.pop(annotation_id)
                success.append(True)
            except KeyError:
                success.append(False)

        return success

    def update_annotations(self,
                           dataset_name,
                           user_id,
                           annotation_type,
                           annotations):
        """ Updates existing annotations

        :param dataset_name: str
        :param user_id: str
        :param annotation_type: str
        :param annotations: list of tuples
             [(sv_ids, annotation_id, serialized_data), ...]
        :return: list[bool]
            success of each insertion
        """

        if not self.has_table(dataset_name, annotation_type):
            return False

        success = []
        for (sv_ids, oid, data) in zip(annotations):
            try:
                table = self._annotation_tables[dataset_name][annotation_type]
                table.pop(oid)
                table[oid] = data
                success.append(True)
            except KeyError:
                success.append(False)

        return success

    def get_annotation_ids(self,
                           dataset_name,
                           annotation_type):
        if not self.has_table(dataset_name, annotation_type):
            return None
        else:
            table = self._annotation_tables[dataset_name][annotation_type]
            return [k for k in table.keys()]

    def get_annotation(self,
                       dataset_name,
                       annotation_type,
                       annotation_id,
                       time_stamp=None):
        """ Reads the data of a single annotation object

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_id: uint64
        :param time_stamp: None or datetime
        :return: blob
        """
        if not self.has_table(dataset_name, annotation_type):
            return None
        table = self._annotation_tables[dataset_name][annotation_type]
        ann = table.get(annotation_id, None)
        return ann


def get_db():
    if 'db' not in g:
        g.db = AnnotationMetaDB()
    return g.db


# class DBMS(object):
#     annotations = {}
#     id = 0

#     def has_table(self, table):
#         return table in self.annotations.keys()

#     def get_tables(self):
#         return self.annotations.keys()

#     def add_table(self, table):
#         if table not in self.annotations.keys():
#             self.annotations[table] = {}


#     def delete_annotation(self, table, oid):
#         try:
#             self.annotations[table].pop(oid)
#         except KeyError:
#             raise AnnotationNotFoundException

#     def save_annotation(self, annotation):
#         self.annotations[annotation['type']][annotation['oid']] = annotation

#     def get_annotation(self, table, oid):
#         try:
#             return self.annotations[table][oid]
#         except KeyError:
#             raise AnnotationNotFoundException
