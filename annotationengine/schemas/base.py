import marshmallow as mm
from annotationengine.cloudvolume import lookup_supervoxel


class IdSchema(mm.Schema):
    id = mm.fields.Str(description='identifier for annotation, unique in type')


class AnnotationSchema(mm.Schema):
    type = mm.fields.Str(
        required=True, description='type of annotation')


class IdAnnotationSchema(IdSchema, AnnotationSchema):
    pass


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
                              validate=mm.validate.Length(equal=3),
                              description='spatial position '
                                          'x,y,z of annotation')
    supervoxel_id = mm.fields.Int(missing=mm.missing,
                                  description="supervoxel id of this point")

    root_id = mm.fields.Int(missing=mm.missing,
                            description='root id associated with'
                                        'this supervoxel')

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
