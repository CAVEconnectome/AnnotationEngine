from marshmallow import fields, Schema, post_load


class Metadata(Schema):
    user_id = fields.Str(required=False)
    description = fields.Str(required=True)
    reference_table = fields.Str(required=False)


class TableSchema(Schema):
    table_name = fields.Str(order=0, required=True)   
    schema_type = fields.Str(order=1, required=True)

class CreateTableSchema(TableSchema):
    metadata = fields.Nested(Metadata, order=3, required=True, example={'description': "my description"})


class DeleteAnnotationSchema(Schema):
    annotation_ids = fields.List(fields.Int, required=True)
    

class PutAnnotationSchema(Schema):
    annotations = fields.List(fields.Dict, required=True)