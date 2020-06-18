from marshmallow import fields, Schema, post_load


class Metadata(Schema):
    user_id = fields.Str(required=False)
    description = fields.Str(required=True)
    reference_table = fields.Str(required=False)


class TableSchema(Schema):
    em_dataset = fields.Str(order=0, required=True)
    table_name = fields.Str(order=1, required=True)   
    schema_type = fields.Str(order=2, required=True)

class CreateTableSchema(TableSchema):
    metadata = fields.Nested(Metadata, order=3, required=True)


class DeleteAnnotationSchema(TableSchema):
    annotation_ids = fields.List(fields.Int, required=True)
    

class PutAnnotationSchema(TableSchema):
    annotations = fields.List(fields.Dict, required=True)