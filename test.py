from emannotationschemas.models import make_all_models, Base
from emannotationschemas.base import flatten_dict
from emannotationschemas import get_schema
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time

# example of initializing mapping of database
DATABASE_URI = "postgresql://postgres:welcometothematrix@35.196.105.34/postgres"
engine = create_engine(DATABASE_URI, echo=False)
model_dict = make_all_models(['pinky40'])
# assures that all the tables are created
# would be done as a db management task in general
#Base.metadata.create_all(engine)

# create a session class
# this will produce session objects to manage a single transaction
Session = sessionmaker(bind=engine)
session = Session()

# get the appropriate sqlalchemy model
# for the annotation type and dataset
SynapseModel = model_dict['pinky40']['synapse']

#print(session.query(SynapseModel).count())
before = time.time()
print(session.query(SynapseModel).filter(SynapseModel.pre_pt_root_id == SynapseModel.post_pt_root_id).count())
after = time.time()
print(after-before)
