from .synapse import SynapseSchema
from annotationengine.errors import UnknownTypeException

type_mapping = {
    'synapse': SynapseSchema
}


def get_types():
    return [k for k in type_mapping.keys()]


def get_schema(type):
    try:
        return type_mapping[type]()
    except KeyError:
        msg = 'type {} is not a known annotation type'.format(type)
        raise UnknownTypeException(msg)
