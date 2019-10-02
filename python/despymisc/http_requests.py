"""
    .. _despymisc-http-request:

    **http_request**
    ----------------
    Library module providing an easy-to-use API for http requests.

    Loads credentials from a :ref:`serviceaccessDescription` storing credentials
    ($HOME/.desservices.ini, by default).

    :author: michael h graber, michael.graber@fhnw.ch
"""

import os
import urllib
import urllib2
from base64 import b64encode

def get_credentials(desfile=os.path.join(os.environ['HOME'], '.desservices.ini'),
                    section='http-desarchive'):
    """
        Load the credentials using serviceaccess from a local .desservices file
        if possible.

        Parameters
        ----------
        desfile : str, optional
            The services access file to use, defaults to $HOME/.desservices.ini

        section : str, optional
            The section to read from `desfile`. Defaults to 'http-desarchive'.

        Returns
        -------
        tuple
            The user name, password, and base url from the file.
    """

    try:
        from despyserviceaccess import serviceaccess
        creds = serviceaccess.parse(desfile, section)
        username = creds['user']
        password = creds['passwd']
        url = creds.get('url', None)
    except Exception:
        username = None
        password = None
        url = None
        warning = """WARNING: could not load credentials from .desservices.ini file for section %s
        please make sure sections make sense""" % section
        print warning

    return username, password, url

def download_file_des(url, filename, desfile=None, section='http-desarchive'):
    """ Download a file using the services access file for credentials.

        Parameters
        ----------
        url : str
            The url for the file to be downloaded.

        filename : str
            The name of the file to create and write the data from `url` to.

        desfile : str, optional
            The name of the service access file to use, defaults to ``None``, which
            becomes $HOME/.desservices.ini

        section : str, optional
            The section to read from the services file. Defaults to 'http-desarchive'

        Examples
        --------
        >>> # download DES file from address:
        >>> http_requests.download_file_des('http://www.blabla.net/foo.xyz', 'blabla.xyz')
        >>> # will download http://www.blabla.net/foo.x to blabla.xyz locally.

    """
    # Get the credentials
    username, password, _ = get_credentials(desfile=desfile, section=section)
    auth = (username, password)
    req = Request(auth)
    req.download_file(url, filename)

class Request(object):
    """ Requests class for retrieving data via http.

        Parameters
        ----------
        auth : two element tuple
            The username and password to use for authentication.
    """

    def __init__(self, auth):

        # auth = (USERNAME, PASSWORD)
        self.auth = auth
        self.url = None
        self.response = None
        self.error_status = (False, '')
        self.data = None

    def POST(self, url, data=None):
        """ Send a POST to the given `url` with `data` as the body.

            Parameters
            ----------
            url : str
                The URL to send the POST message to

            data : dict
                The data to send in the POST message. It is encoded into
                a query string.

            Raises
            ------
            ValueError
                If `dict` is not a dictionary or if the url is not a non-empty
                string.
        """
        if not isinstance(data, dict):
            raise ValueError('The data kwarg needs to be set and of type '
                             'dictionary.')

        self.data = data
        if not url:
            raise ValueError('You need to provide an url kwarg.')
        else:
            self.url = url

        urllib_req = urllib2.Request(self.url)
        if any(self.auth):
            urllib_req.add_header('Authorization',
                                  'Basic ' + b64encode(self.auth[0] + ':' + self.auth[1]))
        try:
            self.response = urllib2.urlopen(urllib_req, urllib.urlencode(self.data))
        except Exception, exc:
            self.error_status = (True, str(exc))

    def get_read(self, url):
        """ Read a response from the given`url`.

            Parameters
            ----------
            url : str
                The URL to get the response from.

            Returns
            -------
            str
                The response from the URL.

            Raises
            ------
            ValueError
                If the url is not a non-empty string.
        """
        if not url:
            raise ValueError('You need to provide an url kwarg.')
        else:
            self.url = url

        urllib_req = urllib2.Request(self.url)
        if any(self.auth):
            urllib_req.add_header('Authorization',
                                  'Basic ' + b64encode(self.auth[0] + ':' + self.auth[1]))
        try:
            self.response = urllib2.urlopen(urllib_req)
            return self.response.read()
        except Exception, exc:
            self.error_status = (True, str(exc))

    def download_file(self, url, filename):
        """ Download the requested file.

            Parameters
            ----------
            url : str
                The URL of the file to download.

            filename : str
                The name of the file to create and place the contents of `url` into.
        """
        with open(filename, 'wb') as f:
            f.write(self.get_read(url))

    def GET(self, url, params={}):
        """ Perform a GET to the given `url` with the given `params`.

            Parameters
            ----------
            url : str
                The URL to which the GET request is sent.

            parameters : dict, optional
                The data to send as the body of the GET message. This is
                encoded into a query string. Default is an empty dictionary.

            Raises
            ------
            ValueError
                If the `url` is not a non-empty string.
        """
        if not url:
            raise ValueError('You need to provide an url kwarg.')
        else:
            self.url = url

        url_params = '?'+'&'.join([str(k) + '=' + str(v) for k, v in
                                   params.iteritems()])
        urllib_req = urllib2.Request(self.url+url_params)
        if any(self.auth):
            urllib_req.add_header('Authorization',
                                  'Basic ' + b64encode(self.auth[0] + ':' + self.auth[1]))
        try:
            self.response = urllib2.urlopen(urllib_req)
        except Exception, exc:
            self.error_status = (True, str(exc))
