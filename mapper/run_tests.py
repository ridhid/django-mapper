#!/usr/bin/env python
import sys
from django import setup
from django.conf import settings
from django.core.management import call_command
from django.core.management import execute_from_command_line

if not settings.configured:
    settings.configure(
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': ':memory:',
            },
        },
        INSTALLED_APPS=(
            'django.contrib.admin',
            'django.contrib.auth',
            'django.contrib.contenttypes',
            'django.contrib.sessions',
            'django.contrib.staticfiles',
            'mapper.apps.MapperConfig',
        ),
        MIDDLEWARE_CLASSES=(
            'django.contrib.sessions.middleware.SessionMiddleware',
            'django.middleware.common.CommonMiddleware',
            'django.contrib.auth.middleware.AuthenticationMiddleware'
        ),
        ROOT_URLCONF=None,
        USE_TZ=True,
        SECRET_KEY='foobar',
    )


def runtests():
    setup()
    call_command('syncdb', '--noinput')

    argv = sys.argv[:1] + ['test'] + sys.argv[1:]
    execute_from_command_line(argv)


if __name__ == '__main__':
    runtests()
