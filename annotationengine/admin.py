from flask_admin import Admin, AdminIndexView, expose
from flask_admin.contrib.sqla import ModelView
from dynamicannotationdb.models import Metadata
from middle_auth_client import auth_required
from flask import g, redirect, url_for
from middle_auth_client import auth_requires_admin, auth_required


class SuperAdminView(ModelView):
     @auth_required
     def is_accessible(self):
        return g.auth_user['admin']
               
     def inaccessible_callback(self, name, **kwargs):
        # redirect to login page if user doesn't have access
        return redirect(url_for('admin.index'))

# Create customized index view class that handles login & registration
class MyAdminIndexView(AdminIndexView):

     
     @expose('/', methods=["GET"])
     @auth_required
     def index(self):
          return super(MyAdminIndexView, self).index()

     @auth_required
     def is_accessible(self):
        return True

def setup_admin(app, db, aligned_volume):
    index_view = MyAdminIndexView(name=f"{aligned_volume}_admin",
                                  endpoint=f'{aligned_volume}',
                                  url=f'/info/admin/{aligned_volume}')
    admin = Admin(name = f'{aligned_volume}_admin2',endpoint=f'{aligned_volume}',index_view=index_view)
    admin.add_view(SuperAdminView(Metadata, db.session, name=f'{aligned_volume}_metadata'))
    admin.init_app(app)
    return admin
