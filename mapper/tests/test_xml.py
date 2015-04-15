# coding: utf-8
from datetime import date, datetime

from lxml import etree

from django.test import TestCase
from mapper.utils.base import HookRegistry

from .utils import load_source_abs_path
from ..utils import load_backend
from ..utils.xml import XmlFieldParser, XmlManyToManyFieldParser
from ..tests.models import Event, Place, EventDate, Owner


class XmlMapperTestSuite(TestCase):
    source_file = 'source/events.rss'
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
                    'field': 'events',
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
    source_file = load_source_abs_path('source/events.rss')

    def setUp(self):
        self.created = []

    def tearDown(self):
        for item in self.created:
            item.delete()

    def test_field_parse(self):
        source = etree.fromstring("""
        <event>
            <title>some event</title>
            <place>some place</place>
        </event>""")

        parser = XmlFieldParser(Event, 'title', 'title')
        value = parser.parse(source)

        self.assertEqual(value,
                         'some event',
                         'error when get atomic value')

    def test_field_model_instance(self):
        place = Place.objects.create(title='some place')
        self.created.append(place)

        source = etree.fromstring("""
        <event>
            <title>some event</title>
            <place>some place</place>
        </event>""")

        parser = XmlFieldParser(Event, 'title', {'model': 'mapper.Place',
                                                 'field': 'title',
                                                 'query': 'place'})
        value = parser.parse(source)
        self.assertTrue(value, 'model instance not found')
        self.assertIsInstance(value, Place, 'founded is not model instance')
        self.assertTrue(value.pk, "founded has'not pk attribute")
        self.assertEqual(value.pk, place.pk, "founded not needed element")

    def test_field_model_instance_multiple(self):
        place = Place.objects.create(title='some place')
        self.created.append(place)

        source = etree.fromstring("""
        <event>
            <title>some event</title>
            <place>some place</place>
            <place>some place dublicate</place>
        </event>""")

        parser = XmlFieldParser(Event, 'title', {'model': 'mapper.Place',
                                                 'field': 'title',
                                                 'query': 'place'})
        exception = None
        try:
            parser.parse(source)
        except parser.ParseMultipleData as e:
            exception = e

        self.assertTrue(exception, 'multiple data non raised')

    def test_field_model_instance_not_created(self):
        source = etree.fromstring("""
        <event>
            <title>some event</title>
            <place>some place</place>
        </event>""")

        parser = XmlFieldParser(Event, 'title', {'model': 'mapper.Place',
                                                 'field': 'title',
                                                 'query': 'place'})
        value = parser.parse(source)
        self.assertTrue(value, 'model instance not found')
        self.assertIsInstance(value, Place, 'founded is not model instance')
        self.assertTrue(value.pk, "founded not created")

    def test_field_m2m(self):
        place = Place.objects.create(title='some event')
        self.created.append(place)

        source = etree.fromstring("""
        <place>
            <title>some event</title>
            <owner>some owner</owner>
        </place>""")

        parser = XmlManyToManyFieldParser(
            Place, 'owners',
            {'query': 'owner', 'model': 'mapper.Owner', 'field': 'title'}
        )
        owner = parser.parse(source)
        self.assertIsInstance(
            owner, Owner,
            'value is not instance of m2m rel model'
        )
        place.owners.add(owner)

        self.assertTrue(place.owners.count(), 'm2m is empty')
        self.assertEqual(place.owners.count(), 1, 'm2m too many')

        owner = place.owners.first()
        self.assertEqual(owner.title,
                         'some owner',
                         'm2m non equals for test data')
        self.created.append(owner)

    def test_field_m2m_through(self):
        event = Event.objects.create(title='some event 1')
        self.created.append(event)

        source = etree.fromstring("""
        <event>
            <title>some event</title>
            <description>some description</description>
            <place>some place</place>
        </event>""")

        parser = XmlManyToManyFieldParser(
            Event, 'places',
            {'query': 'place',
             'model': 'mapper.Place',
             'field': 'title',
             'through': 'mapper.EventDate',
             'left_field': 'event',
             'right_field': 'place',
             'fields': {
                 'description': 'description'
             }}
        )
        place = parser.parse(source)
        self.assertIsInstance(
            place, Place,
            'value is not instance of m2m rel model'
        )

        event_date = parser.get_through_instance(source)
        self.assertTrue(event_date, 'through instance not ')

        event_date.place = place
        event_date.event = event
        event_date.save()

        self.assertTrue(event.place_set.count(), 'm2m is empty')
        self.assertEqual(event.place_set.count(), 1,
                         'm2m too many')

        place = event.place_set.first()
        self.assertEqual(place.title,
                         'some place',
                         'm2m non equals for test data')

        try:
            through = EventDate.objects.get(event=event, place=place)
        except EventDate.DoesNotExists:
            self.assertTrue(False, 'through not found')
        except EventDate.MultipleObjectsReturned:
            self.assertTrue(False, 'through too many')
        else:
            self.assertEqual(through.description, 'some description')
            self.created.append(through)

    def test_hook(self):
        date_format = '%d-%m-%Y'
        date_hook = lambda x: datetime.strptime(x, date_format).date()
        HookRegistry.registry('date_hook', date_hook)

        source = etree.fromstring("""
        <event>
            <title>some event</title>
            <date>15-01-2014</date>
            <place>some place</place>
        </event>""")

        parser = XmlFieldParser(
            EventDate,
            'description',
            {'query': 'date', 'hook': 'date_hook'}
        )
        value = parser.parse(source)

        self.assertTrue(value, 'parser return None')
        self.assertIsInstance(value, date, 'value is not instance of date')
        self.assertEqual(value.strftime(date_format), '15-01-2014')