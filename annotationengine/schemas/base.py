import marshmallow as mm
from annotationengine.cloudvolume import lookup_supervoxel


class AnnotationSchema(mm.Schema):
    id = mm.fields.Str(
        required=True, description='identifier for annotation, unique in type')
    type = mm.fields.Str(
        required=True, description='type of annotation')


class ReferenceAnnotation(mm.Schema):
    target_id = mm.fields.Str(
        required=True, description='annotation this references')


class TagAnnotation(mm.Schema):
    tag = mm.fields.Str(
        required=True, description="tag to attach to annoation")


class ReferenceTagAnnotation(ReferenceAnnotation, TagAnnotation):
    '''A tag attached to another annotation'''


class SpatialPoint(mm.Schema):
    position = mm.fields.List(mm.fields.Float,
                              required=True,
                              description="spatial position \
                                          x,y,z of annotation")
    supervoxel_id = mm.fields.Int(missing=mm.missing,
                                  description="supervoxel id of this point")

    @mm.post_load
    def convert_point(self, item):
        if item['supervoxel_id'] == mm.missing:
            item['supervoxel_id'] = lookup_supervoxel(*item['position'])


class SpatialAnnotation(AnnotationSchema):
    points = mm.fields.Nested(SpatialPoint,
                              many=True,
                              description="spatial points for this annotation")


class TaggedSpatialAnnotation(SpatialAnnotation, TagAnnotation):
    ''' spatial annotation with a tag '''
