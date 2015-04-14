from django.core.management import call_command
from django.db import models

from ..utils import is_test_environment


class News(models.Model):

    title = models.CharField(max_length=256)
    link = models.CharField(max_length=256)
    description = models.CharField(max_length=256)
    publication_date = models.DateTimeField()

    guid = models.ForeignKey('mapper.GUID')

    class Meta:
        app_label = 'mapper'


class GUID(models.Model):
    link = models.CharField(max_length=256, unique=True)

    class Meta:
        app_label = 'mapper'


class Place(models.Model):
    news = models.ManyToManyField(to='mapper.News', related_name='places')

    class Meta:
        app_label = 'mapper'


class NewsThroughPlace(models.Model):
    events = models.ForeignKey('mapper.News', related_name='through')
    places = models.ForeignKey('mapper.Place', related_name='through')

    class Meta:
        app_label = 'mapper'


def sync_test_models():
    if is_test_environment():
        call_command('syncdb', '--noinput')

sync_test_models()