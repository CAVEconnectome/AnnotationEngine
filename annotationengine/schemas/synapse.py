from annotationengine.schemas.base import BoundSpatialPoint, \
    SpatialPoint, \
    AnnotationSchema
import marshmallow as mm


class SynapseSchema(AnnotationSchema):
    pre_pt = mm.fields.Nested(BoundSpatialPoint,
                              description="presynaptic point")
    ctr_pt = mm.fields.Nested(SpatialPoint,
                              description="central point")
    post_pt = mm.fields.Nested(BoundSpatialPoint,
                               description="presynaptic point")

    @mm.post_load
    def validate_type(self, item):
        assert item['type'] == 'synapse'
