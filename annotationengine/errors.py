class AnnotationEngineException(Exception):
    ''' generic error in annotation engine '''
    pass


class UnknownAnnotationTypeException(AnnotationEngineException):
    ''' error raised when an annotation type is not known '''
    pass


class AnnotationNotFoundException(AnnotationEngineException):
    ''' error raised when an annotation is not found '''
    pass
