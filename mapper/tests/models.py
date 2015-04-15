from django.db import models


class Event(models.Model):
    title = models.CharField(max_length=128)
    organizer = models.ForeignKey('mapper.Organizer')

    class Meta:
        app_label = 'mapper'


class Organizer(models.Model):
    title = models.CharField(max_length=128)

    class Meta:
        app_label = 'mapper'


class EventDate(models.Model):
    event = models.ForeignKey('mapper.Event')
    place = models.ForeignKey('mapper.Place')

    date = models.DateField()
    description = models.CharField()

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