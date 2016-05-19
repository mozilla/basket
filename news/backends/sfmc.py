from django.conf import settings

from FuelSDK import ET_Client, ET_DataExtension_Row, ET_TriggeredSend

from news.backends.common import get_timer_decorator, NewsletterException, \
                                 NewsletterNoResultsException


time_request = get_timer_decorator('news.backends.sfmc')


def assert_response(resp):
    if not resp.status:
        raise NewsletterException(str(resp.results))


def assert_results(resp):
    assert_response(resp)
    if not resp.results:
        raise NewsletterNoResultsException()


def build_attributes(data):
    return [{'Name': key, 'Value': value} for key, value in data.items()]


class SFMC(object):
    client = None

    def __init__(self):
        if 'clientid' in settings.SFMC_SETTINGS:
            self.client = ET_Client(False, settings.SFMC_DEBUG, settings.SFMC_SETTINGS)

    def _get_row_obj(self, de_name, props):
        row = ET_DataExtension_Row()
        row.auth_stub = self.client
        row.CustomerKey = row.Name = de_name
        row.props = props
        return row

    @time_request
    def get_row(self, de_name, fields, token=None, email=None):
        """
        Get the values of `fields` from a data extension. Either token or email is required.

        @param de_name: name of the data extension
        @param fields: list of column names
        @param token: the user's token
        @param email: the user's email address
        @return: dict of user data
        """
        assert token or email, 'token or email required'
        row = self._get_row_obj(de_name, fields)
        if token:
            row.search_filter = {
                'Property': 'TOKEN',
                'SimpleOperator': 'equals',
                'Value': token,
            }
        elif email:
            row.search_filter = {
                'Property': 'EMAIL_ADDRESS_',
                'SimpleOperator': 'equals',
                'Value': email,
            }

        resp = row.get()
        assert_results(resp)
        # TODO do something if more than 1 result is returned
        return dict((p.Name, p.Value)
                    for p in resp.results[0].Properties.Property)

    @time_request
    def add_row(self, de_name, values):
        """
        Add a row to a data extension.

        @param de_name: name of the data extension
        @param values: dict containing the COLUMN: VALUE pairs
        @return: None
        """
        row = self._get_row_obj(de_name, values)
        resp = row.post()
        assert_response(resp)

    @time_request
    def update_row(self, de_name, values):
        """
        Update a row in a data extension.

        @param de_name: name of the data extension
        @param values: dict containing the COLUMN: VALUE pairs.
            Must contain TOKEN or EMAIL_ADDRESS_.
        @return: None
        """
        row = self._get_row_obj(de_name, values)
        resp = row.patch()
        assert_response(resp)

    @time_request
    def upsert_row(self, de_name, values):
        """
        Add or update a row in a data extension.

        @param de_name: name of the data extension
        @param values: dict containing the COLUMN: VALUE pairs.
            Must contain TOKEN or EMAIL_ADDRESS_.
        @return: None
        """
        row = self._get_row_obj(de_name, values)
        resp = row.patch(True)
        assert_response(resp)

    @time_request
    def delete_row(self, de_name, token=None, email=None):
        """
        Delete a row from a data extension. Either token or email are required.

        @param de_name: name of the data extension
        @param token: user's token
        @param email: user's email address
        @return: None
        """
        assert token or email, 'token or email required'
        if token:
            values = {'TOKEN': token}
        else:
            values = {'EMAIL_ADDRESS_': email}

        row = self._get_row_obj(de_name, values)
        resp = row.delete()
        assert_response(resp)

    @time_request
    def send_mail(self, ts_name, email, token, format):
        """
        Send an email message to a user (Triggered Send).

        @param ts_name: the name of the message to send
        @param email: the email address of the user
        @return: None
        """
        ts = ET_TriggeredSend()
        ts.auth_stub = self.client
        ts.props = {'CustomerKey': ts_name}
        ts.attributes = build_attributes({
            'TOKEN': token,
            'EMAIL_FORMAT_': format,
        })
        ts.subscribers = [{
            'EmailAddress': email,
            'SubscriberKey': token,
            'EmailTypePreference': 'HTML' if format == 'H' else 'Text',
            'Attributes': ts.attributes,
        }]
        resp = ts.send()
        assert_response(resp)


sfmc = SFMC()
