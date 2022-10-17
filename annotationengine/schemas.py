from dynamicannotationdb import models
from flask_marshmallow import Marshmallow
from marshmallow import Schema, fields

ma = Marshmallow()


class OptionalTableMetadataSchema(Schema):
    reference_table = fields.Str(required=False, example="synapse_table")
    track_target_id_updates = fields.Bool(required=False)


class FullMetadataSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = models.AnnoMetadata


class UMetadataSchema(Schema):
    user_id = fields.Str(required=False, example="1")
    description = fields.Str(
        required=False, example="my annotation table to track cells made by John Doe"
    )
    flat_segmentation_source = fields.Str(
        required=False, example="precomputed://gs://my_cloud_bucket/image"
    )
    read_permission = fields.Bool(required=False)
    write_permission = fields.Str(required=False)


class MetadataSchema(Schema):
    user_id = fields.Str(required=False, example="1")
    description = fields.Str(
        required=True, example="my annotation table to track cells made by John Doe"
    )
    table_metadata = fields.Nested(
        OptionalTableMetadataSchema,
        required=False,
        example='{"reference_table":"synapse_table", "track_target_id_updates": False}',
    )
    flat_segmentation_source = fields.Str(
        required=False, example="precomputed://gs://my_cloud_bucket/image"
    )
    voxel_resolution_x = fields.Float(required=True, example=1.0)
    voxel_resolution_y = fields.Float(required=True, example=1.0)
    voxel_resolution_z = fields.Float(required=True, example=1.0)
    read_permission = fields.Bool(required=False, default="PUBLIC")
    write_permission = fields.Str(required=False, default="PRIVATE")


class TableSchema(Schema):
    table_name = fields.Str(order=0, required=True, example="my_cell_type_table")
    schema_type = fields.Str(order=1, required=True, example="cell_type_local")

class UpdateMetadataSchema(Schema):
    table_name = fields.Str(order=0, required=True, example="my_cell_type_table")
    metadata = fields.Nested(
        UMetadataSchema,
        order=1,
        required=True,
        example={"description": "my description"},
    )

class CreateTableSchema(TableSchema):
    metadata = fields.Nested(
        MetadataSchema,
        order=3,
        required=True,
        example={"description": "my description"},
    )


class DeleteAnnotationSchema(Schema):
    annotation_ids = fields.List(fields.Int, required=True, example=[1, 2, 5])


class PutAnnotationSchema(Schema):
    annotations = fields.List(fields.Dict, required=True)
