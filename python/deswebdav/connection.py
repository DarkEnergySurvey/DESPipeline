"""
    Webdav connection for DES based on webdav-client-python3 (https://github.com/ezhov-evgeny/webdav-client-python-3), version 0.12
"""

from os.path import exists

import deswebdav.exceptions as exceptions
from deswebdav.urn import Urn


class ConnectionSettings(object):
    """ Base class for a connection
    """
    def is_valid(self):
        """ Placeholder for subclasses
        """
        pass

    def valid(self):
        """ Determine if the connection is valid

            Returns
            -------
            bool
                Whether the connection is valid or not.
        """
        try:
            self.is_valid()
        except exceptions.OptionNotValid:
            return False
        else:
            return True


class WebDAVSettings(ConnectionSettings):
    """ Settings for a Webdav connection

        Parameters
        ----------
        options : dict
            Dictionary of the arguments/options for the connection.
    """
    ns = "webdav:"
    prefix = "webdav_"
    keys = {'hostname', 'login', 'password', 'token', 'root', 'cert_path', 'key_path', 'recv_speed', 'send_speed',
            'verbose'}

    hostname = None
    login = None
    password = None
    token = None
    root = None
    cert_path = None
    key_path = None
    recv_speed = None
    send_speed = None
    verbose = None

    def __init__(self, options):

        self.options = dict()

        for key in self.keys:
            value = options.get(key, '')
            self.options[key] = value
            self.__dict__[key] = value

        self.root = Urn(self.root).quote() if self.root else ''
        self.root = self.root.rstrip(Urn.separate)

    def is_valid(self):
        """ Determine if all necessary parameters have been specified for a connection.

            Raises
            ------
            OptionNotValid
                If not all necessary parameters have been specified.
        """
        if not self.hostname:
            raise exceptions.OptionNotValid(name="hostname", value=self.hostname, ns=self.ns)

        if self.cert_path and not exists(self.cert_path):
            raise exceptions.OptionNotValid(name="cert_path", value=self.cert_path, ns=self.ns)

        if self.key_path and not exists(self.key_path):
            raise exceptions.OptionNotValid(name="key_path", value=self.key_path, ns=self.ns)

        if self.key_path and not self.cert_path:
            raise exceptions.OptionNotValid(name="cert_path", value=self.cert_path, ns=self.ns)

        if self.password and not self.login:
            raise exceptions.OptionNotValid(name="login", value=self.login, ns=self.ns)

        if not self.token and not self.login:
            raise exceptions.OptionNotValid(name="login", value=self.login, ns=self.ns)


class ProxySettings(ConnectionSettings):
    """ Class for proxy settings

        Parameters
        ----------
        options : dict
            Dictionary of the arguments/options for the connection.
    """
    ns = "proxy:"
    prefix = "proxy_"
    keys = {'hostname', 'login', 'password'}

    hostname = None
    login = None
    password = None

    def __init__(self, options):

        self.options = dict()

        for key in self.keys:
            value = options.get(key, '')
            self.options[key] = value
            self.__dict__[key] = value

    def is_valid(self):
        """ Determine if all necessary parameters have been specified for a connection.

            Raises
            ------
            OptionNotValid
                If not all necessary parameters have been specified.
        """
        if self.password and not self.login:
            raise exceptions.OptionNotValid(name="login", value=self.login, ns=self.ns)

        if self.login or self.password:
            if not self.hostname:
                raise exceptions.OptionNotValid(name="hostname", value=self.hostname, ns=self.ns)
