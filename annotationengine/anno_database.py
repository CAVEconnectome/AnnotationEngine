from flask import g, current_app
from dynamicannotationdb.annodb import AnnotationMetaDB
from google.auth import credentials, default as default_creds
from google.cloud import bigtable

cache = {}

class DoNothingCreds(credentials.Credentials):
    def refresh(self, request):
        pass


def get_client(config):
    project_id = config.get('project_id', 'pychunkedgraph')

    if config.get('emulate', False):
        credentials = DoNothingCreds()
    else:
        credentials, project_id = default_creds()

    client = bigtable.Client(admin=True,
                             project=project_id,
                             credentials=credentials)
    return client


def get_db():
    if 'db' not in cache:
        cred_config = current_app.config['BIGTABLE_CONFIG']
        client = get_client(cred_config)
        cache["db"] = AnnotationMetaDB(client=client)

    return cache["db"]
