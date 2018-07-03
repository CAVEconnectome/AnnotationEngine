import marshmallow as mm


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
    supervoxel_id = mm.fields.Int(description="supervoxel id of this point")


class SpatialAnnotation(AnnotationSchema):
    points = mm.fields.Nested(SpatialPoint,
                              many=True,
                              description="spatial points for this annotation")


class TaggedSpatialAnnotation(SpatialAnnotation, TagAnnotation):
    ''' spatial annotation with a tag '''
