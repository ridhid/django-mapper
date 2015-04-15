from django.db import models


__ALL__ = ('Event', 'Organizer', 'EventDate', 'Place', 'Owner')


class Event(models.Model):
    title = models.CharField(max_length=128)
    organizer = models.ForeignKey('mapper.Organizer', null=True, blank=True)

    class Meta:
        app_label = 'mapper'


class Organizer(models.Model):
    title = models.CharField(max_length=128)

    class Meta:
        app_label = 'mapper'


class EventDate(models.Model):
    event = models.ForeignKey('mapper.Event', null=True, blank=True)
    place = models.ForeignKey('mapper.Place', null=True, blank=True)

    date = models.DateField(null=True, blank=True)
    description = models.CharField(max_length=256)

    class Meta:
        app_label = 'mapper'


class Place(models.Model):
    title = models.CharField(max_length=128)
    owners = models.ManyToManyField('mapper.Owner')
    events = models.ManyToManyField(
        'mapper.Event',
        through='mapper.EventDate'
    )

    class Meta:
        app_label = 'mapper'


class Owner(models.Model):
    title = models.CharField(max_length=128)

    class Meta:
        app_label = 'mapper'