from django.db import models


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