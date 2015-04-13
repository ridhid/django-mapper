from django.test import TestCase
from .utils import load_source_abs_path
from ..utils import load_backend


class XmlMapperTestSuite(TestCase):
    source_file = 'source/hardware.rss'
    schema = {
        'mapper.News': {
            'query': 'channel.item',
            'fields': {
                'title': 'title',
                'link': 'link',
                'description': 'description',
                'publication_date': {
                    'query': 'pubDate',
                    'hook': 'simple_hook'
                },
                'guid': {
                    'query': 'guid',
                    'model': 'mapper.GUID',
                    'field': 'link',
                }
            },
            'rels': {
                'places': {
                    'query': 'place',
                    'model': 'mapper.Place',
                    'field': 'events',
                    'through': 'mapper.NewsThroughPlace',
                    'left_field': 'events',
                    'right_field': 'places',
                    'hook': 'simple_hook',
                    'fields': {
                        'date': {
                            'query': 'pubDate',
                            'hook': 'simple_hook'
                        }
                    }
                }
            }
        }
    }
    backend = 'xml'

    def setUp(self):
        self.backend = load_backend(self.backend)
        self.assertTrue(self.backend, 'backend not load')

    def test_load_source(self):
        source = load_source_abs_path(self.source_file)
        self.assertTrue(source, 'fail not load')

        source = self.backend.load_source(source)
        self.assertTrue(source, 'fail not load in backend')

    def test_load_schema(self):
        schema = self.schema
        self.assertTrue(schema, 'schema not load')

        status = self.backend.load_parsers(self.schema)
        self.assertTrue(status, 'schema not load in backend')

    def test_load(self):
        self.backend.load(load_source_abs_path(self.source_file), self.schema)

        self.assertTrue(self.backend.loaded)
        self.assertFalse(self.backend.errors)
        self.assertEquals(self.backend.readed,
                          self.backend.loaded)