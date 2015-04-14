# coding: utf-8

from lxml import etree

from django.test import TestCase

from .utils import load_source_abs_path
from ..utils import load_backend
from ..utils.xml import XmlFieldParser
from ..tests.models import Event, Organizer, Place, Owner, EventDate


class XmlMapperTestSuite(TestCase):
    source_file = 'source/hardware.rss'
    schema = {
        'mapper.Event': {
            'query': 'channel.events',
            'fields': {
                'title': 'title',
                'organizer': {
                    'query': 'organizer',
                    'model': 'mapper.Organizer',
                    'field': 'title',
                },
            },
            'rels': {
                'places': {
                    'query': 'place',
                    'model': 'mapper.Place',
                    'field': 'event_set',
                    'through': 'mapper.EventDate',
                    'left_field': 'event',
                    'right_field': 'place',
                    'hook': 'capfirst',
                    'fields': {
                        'date': {
                            'query': 'date',
                            'hook': 'date'
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
        <event>
            <title>some event</title>
            <place>some place<place>
        </item>""")

        parser = XmlFieldParser(Event, 'title', 'title')
        value = parser.parse(source)

        self.assertEqual(value,
                         'some event',
                         'error when get atomic value')

    def test_atomic_model_instance(self):
        guid = Place.objects.create(title='some place')
        self.created.append(guid)

        source = etree.fromstring("""
        <event>
            <title>some event</title>
            <place>some place<place>
        </item>""")

        parser = XmlFieldParser(Event, 'title', {'model': 'mapper.Place',
                                                 'field': 'title'})
        value = parser.parse(source)
        self.assertTrue(value, 'model instance not found')
        self.assertIsInstance(value, Event, 'founded is not model instance')
        self.assertTrue(value.pk, "founded has'not pk attribute")
        self.assertEqual(value.pk, guid.pk, "founded not needed element")