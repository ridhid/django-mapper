import sys
from django.conf import settings
from json import JsonMapperBackend
from xml import XmlMapperBackend


BACKENDS = {
    'xml': XmlMapperBackend,
    'json': JsonMapperBackend
}


def load_backend(backend=None):
    if backend is None:
        backend = settings.XML_MAPPER_DEFAULT_BACKEND

    if backend in BACKENDS:
        backend_cls = BACKENDS[backend]
        return backend_cls()

    raise ValueError('backend not found')


def is_test_environment():
    return 'test' in sys.argv