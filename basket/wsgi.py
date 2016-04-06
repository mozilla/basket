import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "basket.settings")

from django.core.wsgi import get_wsgi_application  # noqa

from whitenoise.django import DjangoWhiteNoise  # noqa

try:
    import newrelic.agent  # noqa
except ImportError:
    newrelic = False


if newrelic:
    newrelic_ini = os.getenv('NEWRELIC_INI_FILE', False)
    if newrelic_ini:
        newrelic.agent.initialize(newrelic_ini)
    else:
        newrelic = False

application = get_wsgi_application()
application = DjangoWhiteNoise(application)

if newrelic:
    application = newrelic.agent.WSGIApplicationWrapper(application)
