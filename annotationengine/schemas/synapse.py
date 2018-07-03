from annotationengine.schemas.base import SpatialPoint, SpatialAnnotation
import marshmallow as mm
from marshmallow import validate


class SynapseSchema(SpatialAnnotation):
    points = mm.fields.Nested(SpatialPoint,
                              validate=validate.Length(equal=3),
                              many=True,
                              description="spatial points for this annotation")

