from flask import g
# this are things I am imaging sven will implement
# they are simply stubbed here


class DBMSException(Exception):
    ''' generic database exception '''


class DBMSAnnotationNotFound(DBMSException):
    ''' annotation not in database '''


class DBMS(object):
    annotations = {}
    id = 0

    def has_table(self, table):
        return table in self.annotations.keys()

    def get_tables(self):
        return self.annotations.keys()

    def add_table(self, table):
        if table not in self.annotations.keys():
            self.annotations[table] = {}

    def get_new_id(self, table):
        self.id += 1
        return self.id

    def delete_annotation(self, table, oid):
        try:
            self.annotations[table].pop(oid)
        except KeyError:
            raise DBMSAnnotationNotFound

    def save_annotation(self, annotation):
        self.annotations[annotation['type']][annotation['oid']] = annotation

    def get_annotation(self, table, oid):
        try:
            return self.annotations[table][oid]
        except KeyError:
            raise DBMSAnnotationNotFound


def get_db():
    if 'db' not in g:
        g.db = DBMS()
    return g.db
