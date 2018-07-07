from flask import g, current_app
from dynamicannotationdb.annodb import AnnotationMetaDB
from google.auth import credentials, default as default_creds
from google.oauth2 import service_account


class DoNothingCreds(credentials.Credentials):
    def refresh(self, request):
        pass


def get_project_credentials(config):
    instance_id = config.get('instance_id', 'pychunkedgraph')

    if config.get('emulate', False):
        return instance_id, 'emulated', DoNothingCreds()

    servicefile = config.get('service_account_file', None)
    if servicefile:
        credentials = service_account(servicefile)
        project_id = config.get('project_id', 'neuromancer-seung-import')
        return instance_id, project_id, credentials
    else:
        project_id, credentials = default_creds()
        return instance_id, project_id, credentials


def get_db():
    if 'db' not in g:
        cred_config = current_app.config['BIGTABLE_CONFIG']
        instance_id, project_id, creds = get_project_credentials(cred_config)
        print('instance', instance_id, 'project_id', project_id)
        g.db = AnnotationMetaDB(credentials=creds,
                                project_id=project_id,
                                instance_id=instance_id)
    return g.db
