class AnnotationEngineException(Exception):
    ''' generic error in annotation engine '''
    pass


class AnnotationNotFoundException(AnnotationEngineException):
    ''' error raised when an annotation is not found '''
    pass


class DataSetNotFoundException(AnnotationEngineException):
    ''' error raised when a dataset is not found '''
