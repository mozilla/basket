from __future__ import absolute_import
import datetime
import logging
from email.utils import formatdate
from functools import wraps
from time import mktime, time

from django.conf import settings
from django.core.cache import get_cache
from django_statsd.clients import statsd

import requests
import user_agents
from celery import Task

from news.backends.common import NewsletterException, NewsletterNoResultsException
from news.backends.exacttarget_rest import ETRestError, ExactTargetRest
from news.backends.sfdc import sfdc
from news.backends.sfmc import sfmc
from news.celery import app as celery_app
from news.models import FailedTask, Newsletter, Interest, QueuedTask
from news.newsletters import get_sms_messages, is_supported_newsletter_language
from news.utils import (generate_token, get_user_data, MSG_USER_NOT_FOUND,
                        parse_newsletters, SUBSCRIBE)


log = logging.getLogger(__name__)

BAD_MESSAGE_ID_CACHE = get_cache('bad_message_ids')

# Base message ID for confirmation email
CONFIRMATION_MESSAGE = "confirmation_email"

# This is prefixed with the 2-letter language code + _ before sending,
# e.g. 'en_recovery_message', and '_T' if text, e.g. 'en_recovery_message_T'.
RECOVERY_MESSAGE_ID = 'recovery_message'
FXACCOUNT_WELCOME = 'FxAccounts_Welcome'

# don't propagate and don't retry if these are the error messages
IGNORE_ERROR_MSGS = [
    'InvalidEmailAddress',
    'An invalid phone number was provided',
]
# don't propagate after max retries if these are the error messages
IGNORE_ERROR_MSGS_POST_RETRY = [
    'There are no valid subscribers',
]
# tasks exempt from maintenance mode queuing
MAINTENANCE_EXEMPT = [
    'news.tasks.add_fxa_activity',
    'news.tasks.update_student_ambassadors',
    'news.tasks.add_sms_user',
    'news.tasks.add_sms_user_optin',
]


class BasketError(Exception):
    """Tasks can raise this when an error happens that we should not retry.
    E.g. if the error indicates we're passing bad parameters.
    (As opposed to an error connecting to ExactTarget at the moment,
    where we'd typically raise NewsletterException.)
    """
    def __init__(self, msg):
        super(BasketError, self).__init__(msg)


class ETTask(Task):
    abstract = True
    default_retry_delay = 60 * 5  # 5 minutes
    max_retries = 8  # ~ 30 min

    def on_success(self, retval, task_id, args, kwargs):
        """Success handler.

        Run by the worker if the task executes successfully.

        :param retval: The return value of the task.
        :param task_id: Unique id of the executed task.
        :param args: Original arguments for the executed task.
        :param kwargs: Original keyword arguments for the executed task.

        The return value of this handler is ignored.

        """
        statsd.incr(self.name + '.success')
        statsd.incr('news.tasks.success_total')

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Error handler.

        This is run by the worker when the task fails.

        :param exc: The exception raised by the task.
        :param task_id: Unique id of the failed task.
        :param args: Original arguments for the task that failed.
        :param kwargs: Original keyword arguments for the task
                       that failed.

        :keyword einfo: :class:`~celery.datastructures.ExceptionInfo`
                        instance, containing the traceback.

        The return value of this handler is ignored.

        """
        statsd.incr(self.name + '.failure')
        statsd.incr('news.tasks.failure_total')
        if settings.STORE_TASK_FAILURES:
            FailedTask.objects.create(
                task_id=task_id,
                name=self.name,
                args=args,
                kwargs=kwargs,
                exc=repr(exc),
                einfo=str(einfo),  # str() gives more info than repr() on celery.datastructures.ExceptionInfo
            )

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Retry handler.

        This is run by the worker when the task is to be retried.

        :param exc: The exception sent to :meth:`retry`.
        :param task_id: Unique id of the retried task.
        :param args: Original arguments for the retried task.
        :param kwargs: Original keyword arguments for the retried task.

        :keyword einfo: :class:`~celery.datastructures.ExceptionInfo`
                        instance, containing the traceback.

        The return value of this handler is ignored.

        """
        statsd.incr(self.name + '.retry')
        statsd.incr('news.tasks.retry_total')


def et_task(func):
    """Decorator to standardize ET Celery tasks."""
    @celery_app.task(base=ETTask)
    @wraps(func)
    def wrapped(*args, **kwargs):
        start_time = kwargs.pop('start_time', None)
        if start_time:
            total_time = int((time() - start_time) * 1000)
            statsd.timing(wrapped.name + '.timing', total_time)
        statsd.incr(wrapped.name + '.total')
        statsd.incr('news.tasks.all_total')
        if settings.MAINTENANCE_MODE and wrapped.name not in MAINTENANCE_EXEMPT:
            # record task for later
            QueuedTask.objects.create(
                name=wrapped.name,
                args=args,
                kwargs=kwargs,
            )
            statsd.incr(wrapped.name + '.queued')
            return

        try:
            return func(*args, **kwargs)
        except (IOError, NewsletterException, ETRestError) as e:
            # These could all be connection issues, so try again later.
            # IOError covers URLError and SSLError.
            exc_msg = str(e)
            # but don't retry for certain error messages
            for ignore_msg in IGNORE_ERROR_MSGS:
                if ignore_msg in exc_msg:
                    return

            try:
                wrapped.retry(args=args, kwargs=kwargs,
                              countdown=(2 ** wrapped.request.retries) * 60)
            except wrapped.MaxRetriesExceededError:
                statsd.incr(wrapped.name + '.retry_max')
                statsd.incr('news.tasks.retry_max_total')
                # don't bubble certain errors
                for ignore_msg in IGNORE_ERROR_MSGS_POST_RETRY:
                    if ignore_msg in exc_msg:
                        return

                raise e

    return wrapped


def gmttime():
    d = datetime.datetime.now() + datetime.timedelta(minutes=10)
    stamp = mktime(d.timetuple())
    return formatdate(timeval=stamp, localtime=False, usegmt=True)


def get_external_user_data(email=None, token=None, fields=None, database=None):
    database = database or settings.EXACTTARGET_DATA
    fields = fields or [
        'EMAIL_ADDRESS_',
        'EMAIL_FORMAT_',
        'COUNTRY_',
        'LANGUAGE_ISO2',
        'TOKEN',
    ]
    try:
        user = sfmc.get_row(database, fields, token, email)
    except NewsletterNoResultsException:
        return None

    user_data = {
        'email': user['EMAIL_ADDRESS_'],
        'format': user['EMAIL_FORMAT_'] or 'H',
        'country': user['COUNTRY_'] or '',
        'lang': user['LANGUAGE_ISO2'] or '',  # Never None
        'token': user['TOKEN'],
    }
    return user_data


@et_task
def add_fxa_activity(data):
    user_agent = user_agents.parse(data['user_agent'])
    device_type = 'D'
    if user_agent.is_mobile:
        device_type = 'M'
    elif user_agent.is_tablet:
        device_type = 'T'

    record = {
        'FXA_ID': data['fxa_id'],
        'LOGIN_DATE': gmttime(),
        'FIRST_DEVICE': 'y' if data['first_device'] else 'n',
        'OS': user_agent.os.family,
        'OS_VERSION': user_agent.os.version_string,
        'BROWSER': '{0} {1}'.format(user_agent.browser.family,
                                    user_agent.browser.version_string),
        'DEVICE_NAME': user_agent.device.family,
        'DEVICE_TYPE': device_type,
    }

    apply_updates('Sync_Device_Logins', record)


@et_task
def update_fxa_info(email, lang, fxa_id, source_url=None, skip_welcome=False):
    # TODO put this in a different data extension
    user = get_external_user_data(email=email)
    record = {
        'EMAIL_ADDRESS_': email,
        'FXA_ID': fxa_id,
        'MODIFIED_DATE_': gmttime(),
        'FXA_LANGUAGE_ISO2': lang,
    }
    if user:
        token = user['token']
    else:
        token = generate_token()
        # only want source url for first contact
        record['SOURCE_URL'] = source_url or 'https://accounts.firefox.com'

    record['TOKEN'] = token

    apply_updates(settings.EXACTTARGET_DATA, record)


@et_task
def update_get_involved(interest_id, lang, name, email, country, email_format,
                        subscribe, message, source_url):
    """Record a users interest and details for contribution."""
    try:
        interest = Interest.objects.get(interest_id=interest_id)
    except Interest.DoesNotExist:
        # invalid request; no need to raise exception and retry
        return

    email_format = 'T' if email_format.upper().startswith('T') else 'H'

    # Get the user's current settings from ET, if any
    user = get_user_data(email=email)

    record = {
        'EMAIL_ADDRESS_': email,
        'MODIFIED_DATE_': gmttime(),
        'LANGUAGE_ISO2': lang,
        'COUNTRY_': country,
        'GET_INVOLVED_FLG': 'Y',
    }
    if user:
        token = user['token']
        if 'get-involved' not in user.get('newsletters', []):
            record['GET_INVOLVED_DATE'] = gmttime()
    else:
        token = generate_token()
        record['EMAIL_FORMAT_'] = email_format
        record['GET_INVOLVED_DATE'] = gmttime()
        # only want source url for first contact
        if source_url:
            record['SOURCE_URL'] = source_url

    record['TOKEN'] = token
    if subscribe:
        # TODO: 'get-involved' not added to ET yet, so can't use it yet.
        # will go in this list when ready.
        newsletters = ['about-mozilla']
        if user:
            cur_newsletters = user.get('newsletters', None)
            if cur_newsletters is not None:
                cur_newsletters = set(cur_newsletters)
        else:
            cur_newsletters = None

        # Set the newsletter flags in the record by comparing to their
        # current subscriptions.
        to_subscribe, _ = parse_newsletters(record, SUBSCRIBE, newsletters, cur_newsletters)
    else:
        to_subscribe = None

    apply_updates(settings.EXACTTARGET_DATA, record)
    apply_updates(settings.EXACTTARGET_INTERESTS, {
        'TOKEN': token,
        'INTEREST': interest_id,
    })
    welcome_id = mogrify_message_id(interest.welcome_id, lang, email_format)
    send_message.delay(welcome_id, email, token, email_format)
    interest.notify_stewards(name, email, lang, message)

    if to_subscribe:
        if not user:
            user = {
                'email': email,
                'token': token,
                'lang': lang,
            }
        send_welcomes(user, to_subscribe, email_format)


FSA_FIELDS = {
    'EMAIL_ADDRESS': 'Email',
    'TOKEN': 'Token__c',
    'FIRST_NAME': 'FirstName',
    'LAST_NAME': 'LastName',
    'COUNTRY_': 'MailingCountryCode',
    'STUDENTS_SCHOOL': 'FSA_School__c',
    'STUDENTS_GRAD_YEAR': 'FSA_Grad_Year__c',
    'STUDENTS_MAJOR': 'FSA_Major__c',
    'STUDENTS_CITY': 'FSA_City__c',
    'STUDENTS_CURRENT_STATUS': 'FSA_Current_Status__c',
    'STUDENTS_ALLOW_SHARE': 'FSA_Allow_Info_Shared__c',
}


@et_task
def update_student_ambassadors(data, token):
    user_data = {'token': token}
    data['TOKEN'] = token
    update_data = {}
    for k, fn in FSA_FIELDS:
        if k in data:
            update_data[fn] = data[k]
            if k == 'STUDENTS_ALLOW_SHARE':
                # convert to boolean
                update_data[fn] = update_data[fn].lower().startswith('y')

    sfdc.update(user_data, update_data)


@et_task
def update_user(data, email, token, api_call_type, optin):
    """Legacy Task for updating user's preferences and newsletters.

    @param dict data: POST data from the form submission
    @param string email: User's email address
    @param string token: User's token. If None, the token will be
        looked up, and if no token is found, one will be created for the
        given email.
    @param int api_call_type: What kind of API call it was. Could be
        SUBSCRIBE, UNSUBSCRIBE, or SET.
    @param boolean optin: legacy option. it is now included in data. may be removed after
        initial deployment (required so that existing tasks in the queue won't fail for having
        too many arguments).

    @returns: None
    @raises: NewsletterException if there are any errors that would be
        worth retrying. Our task wrapper will retry in that case.

    TODO remove after initial deployment
    """
    # backward compat with existing items on the queue when deployed.
    if optin is not None:
        data['optin'] = optin

    upsert_contact(api_call_type, data, get_user_data(email=email, token=token))


@et_task
def upsert_user(api_call_type, data):
    """
    Update or insert (upsert) a contact record in SFDC

    @param int api_call_type: What kind of API call it was. Could be
        SUBSCRIBE, UNSUBSCRIBE, or SET.
    @param dict data: POST data from the form submission
    @return:
    """
    upsert_contact(api_call_type, data,
                   get_user_data(data.get('token'), data.get('email')))


def upsert_contact(api_call_type, data, user_data):
    """
    Update or insert (upsert) a contact record in SFDC

    @param int api_call_type: What kind of API call it was. Could be
        SUBSCRIBE, UNSUBSCRIBE, or SET.
    @param dict data: POST data from the form submission
    @param dict user_data: existing contact data from SFDC
    @return: token, created
    """
    if 'format' in data:
        data['format'] = 'T' if data['format'].upper().startswith('T') else 'H'
    newsletters = [x.strip() for x in data.get('newsletters', '').split(',')]
    if user_data:
        cur_newsletters = user_data.get('newsletters', None)
    else:
        cur_newsletters = None

    # Set the newsletter flags in the record by comparing to their
    # current subscriptions.
    data['newsletters'] = parse_newsletters(api_call_type, newsletters, cur_newsletters)

    if not (data.get('optin') or (user_data and user_data.get('optin'))):
        # Are they subscribing to any newsletters that don't require confirmation?
        # When including any newsletter that does not
        # require confirmation, user gets a pass on confirming and goes straight
        # to confirmed.
        to_subscribe = [nl for nl, sub in data['newsletters'].iteritems() if sub]
        exempt_from_confirmation = Newsletter.objects \
            .filter(slug__in=to_subscribe, requires_double_optin=False) \
            .exists()
        data['optin'] = exempt_from_confirmation

    if user_data is None:
        # no user found. create new one.
        data['token'] = generate_token()
        sfdc.add(data)
        created = True
    else:
        # update record
        if not user_data['token']:
            data['token'] = generate_token()

        sfdc.update(user_data, data)
        created = False

    return data['token'], created


def apply_updates(database, record):
    """Send the record data to ET to update the database named
    target_et.

    :param str database: Target database, e.g. settings.EXACTTARGET_DATA
        or settings.EXACTTARGET_CONFIRMATION.
    :param dict record: Data to send
    """
    sfmc.upsert_row(database, record)


@et_task
def send_message(message_id, email, token, format):
    """
    Ask ET to send a message.

    :param str message_id: ID of the message in ET
    :param str email: email to send it to
    :param str token: token of the email user
    :param str format: 'H' or 'T' - whether to send in HTML or Text
       (message_id should also be for a message in matching format)

    :raises: NewsletterException for retryable errors, BasketError for
        fatal errors.
    """

    if BAD_MESSAGE_ID_CACHE.get(message_id, False):
        return
    log.debug("Sending message %s to %s %s in %s" %
              (message_id, email, token, format))
    try:
        sfmc.send_mail(message_id, email, token, format)
    except NewsletterException as e:
        # Better error messages for some cases. Also there's no point in
        # retrying these
        if 'Invalid Customer Key' in e.message:
            # remember it's a bad message ID so we don't try again during this process.
            BAD_MESSAGE_ID_CACHE.set(message_id, True)
            return
        # we should retry
        raise


def mogrify_message_id(message_id, lang, format):
    """Given a bare message ID, a language code, and a format (T or H),
    return a message ID modified to specify that language and format.

    E.g. on input ('MESSAGE', 'fr', 'T') it returns 'fr_MESSAGE_T',
    or on input ('MESSAGE', 'pt', 'H') it returns 'pt_MESSAGE'

    If `lang` is None or empty, it skips prefixing the language.
    """
    if lang:
        result = "%s_%s" % (lang.lower()[:2], message_id)
    else:
        result = message_id
    if format == 'T':
        result += "_T"
    return result


def send_confirm_notice(email, token, lang, format, newsletter_slugs):
    """
    Send email to user with link to confirm their subscriptions.

    :param email: email address to send to
    :param token: user's token
    :param lang: language code to use
    :param format: format to use ('T' or 'H')
    :param newsletter_slugs: slugs of newsletters involved
    :raises: BasketError
    """

    if not lang:
        lang = 'en'   # If we don't know a language, use English

    # Is the language supported?
    if not is_supported_newsletter_language(lang):
        msg = "Cannot send confirmation in unsupported language '%s'." % lang
        raise BasketError(msg)

    # See if any newsletters have a custom confirmation message
    # We only need to find one; if so, we'll use the first we find.
    newsletters = Newsletter.objects.filter(slug__in=newsletter_slugs)\
        .exclude(confirm_message='')[:1]
    if newsletters:
        welcome = newsletters[0].confirm_message
    else:
        welcome = CONFIRMATION_MESSAGE

    welcome = mogrify_message_id(welcome, lang, format)
    send_message.delay(welcome, email, token, format)


def send_welcomes(user_data, newsletter_slugs, format):
    """
    Send welcome messages to the user for the specified newsletters.
    Don't send any duplicates.

    Also, if the newsletters listed include
    FIREFOX_OS, then send that welcome but not the firefox & you
    welcome.

    """
    if not newsletter_slugs:
        log.debug("send_welcomes(%r) called with no newsletters, returning"
                  % user_data)
        return

    newsletters = Newsletter.objects.filter(
        slug__in=newsletter_slugs
    )

    # We don't want any duplicate welcome messages, so make a set
    # of the ones to send, then send them
    welcomes_to_send = set()
    for nl in newsletters:
        welcome = nl.welcome.strip()
        if not welcome:
            continue
        languages = [lang[:2].lower() for lang in nl.language_list]
        lang_code = user_data.get('lang', 'en')[:2].lower()
        if lang_code not in languages:
            # Newsletter does not support their preferred language, so
            # it doesn't have a welcome in that language either. Settle
            # for English, same as they'll be getting the newsletter in.
            lang_code = 'en'
        welcome = mogrify_message_id(welcome, lang_code, format)
        welcomes_to_send.add(welcome)
    # Note: it's okay not to send a welcome if none of the newsletters
    # have one configured.
    for welcome in welcomes_to_send:
        log.debug("Sending welcome %s to user %s %s" %
                 (welcome, user_data['email'], user_data['token']))
        send_message.delay(welcome, user_data['email'], user_data['token'],
                           format)


@et_task
def confirm_user(token):
    """
    Confirm any pending subscriptions for the user with this token.

    If any of the subscribed newsletters have welcome messages,
    send them.

    :param token: User's token
    :param user_data: Dictionary with user's data from Exact Target,
        as returned by get_user_data(), or None if that wasn't available
        when this was called.
    :raises: BasketError for fatal errors, NewsletterException for retryable
        errors.
    """
    user_data = get_user_data(token=token)

    if user_data is None:
        raise BasketError(MSG_USER_NOT_FOUND)

    if user_data['optin']:
        # already confirmed
        return

    if not ('email' in user_data and user_data['email']):
        raise BasketError('token has no email in ET')

    sfdc.update(user_data, {'optin': True})


@et_task
def add_sms_user(send_name, mobile_number, optin):
    messages = get_sms_messages()
    if send_name not in messages:
        return
    et = ExactTargetRest()

    try:
        et.send_sms([mobile_number], messages[send_name])
    except ETRestError as error:
        return add_sms_user.retry(exc=error)

    if optin:
        add_sms_user_optin.delay(mobile_number)


@et_task
def add_sms_user_optin(mobile_number):
    record = {'Phone': mobile_number, 'SubscriberKey': mobile_number}
    sfmc.add_row('Mobile_Subscribers', record)


@et_task
def update_custom_unsub(token, reason):
    """Record a user's custom unsubscribe reason."""
    sfdc.update({'token': token}, {'Unsubscribe_Reason__c': reason})


def attempt_fix(database, record, task, e):
    # Sometimes a user is in basket's database but not in
    # ExactTarget because the API failed or something. If that's
    # the case, any future API call will error because basket
    # won't add the required CREATED_DATE field. Try to add them
    # with it here.
    if e.message.find('CREATED_DATE_') != -1:
        record['CREATED_DATE_'] = gmttime()
        sfmc.add_row(database, record)
    else:
        raise e


@et_task
def send_recovery_message_task(email):
    user_data = get_user_data(email=email)
    if not user_data:
        log.debug("In send_recovery_message_task, email not known: %s" % email)
        return

    # make sure we have a language and format, no matter what ET returned
    lang = user_data.get('lang', 'en') or 'en'
    format = user_data.get('format', 'H') or 'H'

    if lang not in settings.RECOVER_MSG_LANGS:
        lang = 'en'

    sfmc.upsert_row(settings.EXACTTARGET_DATA, {
        'TOKEN': user_data['token'],
        'EMAIL_ADDRESS_': user_data['email'],
        'EMAIL_FORMAT_': format,
    })
    message_id = mogrify_message_id(RECOVERY_MESSAGE_ID, lang, format)
    send_message.delay(message_id, email, user_data['token'], format)


@celery_app.task()
def snitch(start_time=None):
    if start_time is None:
        snitch.delay(time())
        return

    snitch_id = settings.SNITCH_ID
    totalms = int((time() - start_time) * 1000)
    statsd.timing('news.tasks.snitch.timing', totalms)
    requests.post('https://nosnch.in/{}'.format(snitch_id), data={
        'm': totalms,
    })
