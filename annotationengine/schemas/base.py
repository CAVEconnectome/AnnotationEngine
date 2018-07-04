import marshmallow as mm
from annotationengine.voxel import lookup_supervoxel


class IdSchema(mm.Schema):
    '''schema with a unique identifier'''
    oid = mm.fields.Int(description='identifier for annotation, '
                                    'unique in type')


class AnnotationSchema(mm.Schema):
    '''schema with the type of annotation'''
    type = mm.fields.Str(
        required=True, description='type of annotation')


class IdAnnotationSchema(IdSchema, AnnotationSchema):
    '''base schema for annotations'''
    pass


class ReferenceAnnotation(mm.Schema):
    '''a annoation that references another annotation'''
    target_id = mm.fields.Int(
        required=True, description='annotation this references')


class TagAnnotation(mm.Schema):
    '''a simple tagged annotation'''
    tag = mm.fields.Str(
        required=True, description="tag to attach to annoation")


class ReferenceTagAnnotation(ReferenceAnnotation, TagAnnotation):
    '''A tag attached to another annotation'''


class SpatialPoint(mm.Schema):
    '''a position in the segmented volume with an associated supervoxel id'''
    position = mm.fields.List(mm.fields.Float,
                              required=True,
                              validate=mm.validate.Length(equal=3),
                              description='spatial position '
                                          'x,y,z of annotation')
    supervoxel_id = mm.fields.Int(missing=mm.missing,
                                  description="supervoxel id of this point")

    @mm.post_load
    def convert_point(self, item):
        if item['supervoxel_id'] == mm.missing:
            item['supervoxel_id'] = lookup_supervoxel(*item['position'])


class SpatialAnnotation(IdAnnotationSchema):
    ''' a superclass of all annotations that involve 1 or more points'''
    points = mm.fields.Nested(SpatialPoint,
                              many=True,
                              description="spatial points for this annotation")


class TaggedSpatialAnnotation(SpatialAnnotation, TagAnnotation):
    ''' spatial annotation with a tag '''


# root_id = mm.fields.Int(missing=mm.missing,
#                         description='root id associated with'
#                                     'this supervoxel')
