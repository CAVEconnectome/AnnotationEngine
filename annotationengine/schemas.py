from marshmallow import fields, Schema, post_load


class Metadata(Schema):
    user_id = fields.Str(required=True)
    description = fields.Str(required=True)
    reference_table = fields.Str(required=False)


class TableSchema(Schema):
    em_dataset_name = fields.Str(order=0, required=True)
    table_name = fields.Str(order=1, required=True)   
    schema_type = fields.Str(order=2, required=True)
    metadata = fields.Nested(Metadata, order=3, required=True)