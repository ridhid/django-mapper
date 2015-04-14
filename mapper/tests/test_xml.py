# coding: utf-8

from lxml import etree

from django.test import TestCase

from .utils import load_source_abs_path
from ..tests.models import News, GUID
from ..utils import load_backend
from ..utils.xml import XmlFieldParser


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


class XmlFieldParserTest(TestCase):
    source_file = load_source_abs_path('source/hardware.rss')

    def setUp(self):
        self.created = []

    def tearDown(self):
        for item in self.created:
            item.delete()

    def test_atomic_parse(self):
        source = etree.fromstring("""
        <item>
            <title>Технология Toshiba</title>
            <link>https://news.yandex.ru/yandsearch?</link>
            <guid>cl4url=www.3dnews.ru%2F912388</guid>
        </item>""")

        parser = XmlFieldParser(News, 'title', 'title')
        value = parser.parse(source)

        self.assertEqual(value,
                         'Технология Toshiba',
                         'error when get atomic value')

    def test_atomic_model_instance(self):
        guid = GUID.objects.create(link='https://news.yandex.ru/yandsearch?')
        self.created.append(guid)

        source = etree.fromstring("""
        <item>
            <title>Технология Toshiba</title>
            <link>https://news.yandex.ru/yandsearch?</link>
            <guid>cl4url=www.3dnews.ru%2F912388</guid>
        </item>""")

        parser = XmlFieldParser(News, 'title', {'model': 'mapper.GUID',
                                                'field': 'link'})
        value = parser.parse(source)
        self.assertTrue(value, 'model instance not found')
        self.assertIsInstance(value, GUID, 'founded is not model instance')
        self.assertTrue(value.pk, "founded has'not pk attribute")
        self.assertEqual(value.pk, guid.pk, "founded not needed element")