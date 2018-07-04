from flask import g
# this are things I am imaging sven will implement
# they are simply stubbed here


class DBMS(object):
    annotations = {}
    id = 0

    def get_tables(self):
        return self.annotations.keys()

    def add_table(self, table):
        if table not in self.annotations.keys():
            self.annotations[table] = {}

    def get_new_id(self, table):
        self.id += 1
        return self.id

    def delete_annotation(self, table, id):
        self.annotations[table].pop(id)

    def save_annotation(self, annotation):
        self.annotations[annotation['type']][annotation['id']] = annotation

    def get_annotation(self, table, id):
        return self.annotations[table][id]


def get_db():
    if 'db' not in g:
        g.db = DBMS()
    return g.db
