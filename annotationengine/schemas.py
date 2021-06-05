from marshmallow import fields, Schema, post_load
from flask_marshmallow import Marshmallow
from dynamicannotationdb import models 

ma = Marshmallow()

class FullMetadataSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = models.AnnoMetadata

class MetadataSchema(Schema):
    user_id = fields.Str(required=False)
    description = fields.Str(required=True)
    reference_table = fields.Str(required=False)
    flat_segmentation_source = fields.Str(required=False)
    voxel_resolution_x = fields.Float(required=True)
    voxel_resolution_y = fields.Float(required=True)
    voxel_resolution_z = fields.Float(required=True)
    
class TableSchema(Schema):
    table_name = fields.Str(order=0, required=True)   
    schema_type = fields.Str(order=1, required=True)

class CreateTableSchema(TableSchema):
    metadata = fields.Nested(MetadataSchema, order=3, required=True, example={'description': "my description"})


class DeleteAnnotationSchema(Schema):
    annotation_ids = fields.List(fields.Int, required=True)
    

class PutAnnotationSchema(Schema):
    annotations = fields.List(fields.Dict, required=True)