from django.apps import AppConfig
from django.utils.text import capfirst
from django.utils.translation import ugettext_lazy as _


class MapperConfig(AppConfig):
    name = 'mapper'
    verbose_name = capfirst(_('django object mapper'))


