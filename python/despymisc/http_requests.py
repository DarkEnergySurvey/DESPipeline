'''
Library module providing an easy-to-use API for http requests to DESDM services.

Loads credentials from a desfile storing credentials (.desservices.ini, by
default assumed to be in the users home directory).

USAGE:
``````
- download DES file from address:
    http_requests.download_file_des('http://www.blabla.net/foo.xyz', 'blabla.xyz')

:author: michael h graber, michael.graber@fhnw.ch
'''

import os
import urllib
import urllib2
from base64 import b64encode

def get_credentials(desfile=os.path.join(os.environ['HOME'], '.desservices.ini'),
                    section='http-desarchive'):

    """
    Load the credentials using serviceaccess from a local .desservices file
    if possible.
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

    ''' Download files using the DES services files.
    '''
    # Get the credentials
    username, password, _ = get_credentials(desfile=desfile, section=section)
    auth = (username, password)
    req = Request(auth)
    req.download_file(url, filename)

class Request(object):
    """ Requests class for retrieving data
    """

    def __init__(self, auth):

        # auth = (USERNAME, PASSWORD)
        self.auth = auth
        self.url = None
        self.response = None
        self.error_status = (False, '')
        self.data = None

    def POST(self, url, data=None):
        """ send a POST to the url
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
        """ read a response
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
        """ Download the requested file
        """
        with open(filename, 'wb') as f:
            f.write(self.get_read(url))

    def GET(self, url, params={}):
        """ perform a GET to the given url
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
