from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from dynamicannotationdb.models import Metadata
from middle_auth_client import auth_required
from flask import g, redirect, url_for

class SuperAdminView(ModelView):
     @auth_required
     def is_accessible(self):
        return g.auth_user['admin']
               
     def inaccessible_callback(self, name, **kwargs):
        # redirect to login page if user doesn't have access
        return redirect(url_for('admin.index'))


def setup_admin(app, db):
    admin = Admin(app, name="annotationengine")
    admin.add_view(ModelView(Metadata, db.session))
    return admin
