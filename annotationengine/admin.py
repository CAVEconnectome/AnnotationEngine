from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from dynamicannotationdb.models import Metadata


def setup_admin(app, db):
    admin = Admin(app, name="annotationengine")
    admin.add_view(ModelView(Metadata, db.session))
    return admin
