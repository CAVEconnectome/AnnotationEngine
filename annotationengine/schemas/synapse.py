from annotationengine.schemas.base import SpatialAnnotation
import marshmallow as mm

class SynapseSchema(SpatialAnnotation):

    @mm.post_load
    def validate_synapse(item):
        if len(item['points'] != 3):
            raise mm.ValidationError('A synapse must contain 3 points\
                                      (pre, center, post)')
