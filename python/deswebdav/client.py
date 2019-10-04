"""
    Webdav client for DES based on webdav-client-python3 (https://github.com/ezhov-evgeny/webdav-client-python-3), version 0.12
"""
#pylint: disable=c-extension-no-member,too-many-public-methods
import functools
import logging
import os
import shutil
import threading
import copy
from io import BytesIO
from re import sub

import lxml.etree as etree
import requests

from deswebdav.connection import WebDAVSettings, ProxySettings
import deswebdav.exceptions as exceptions
from deswebdav.urn import Urn

try:
    from urllib.parse import unquote, urlsplit
except ImportError:
    from urllib import unquote
    from urlparse import urlsplit

__version__ = "0.2"
log = logging.getLogger(__name__)


def listdir(directory):
    """ Returns list of nested files and directories for local directory by path

        Parameters
        ----------
        directory : str
            Absolute or relative path to local directory

        Returns
        -------
        list
            Nested list of file or directory names
    """
    file_names = list()
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isdir(file_path):
            filename = "{filename}{separate}".format(filename=filename, separate=os.path.sep)
        file_names.append(filename)
    return file_names


def get_options(option_type, from_options):
    """ Extract options for specified option type from all options

        Parameters
        ----------
        option_type : dict
            The object of specified type of options

        from_options : dict
            All options dictionary

        Returns
        -------
        dict
            The dictionary of options for specified type, each option can be filled by value from all options
            dictionary or blank in case the option for specified type is not exist in all options dictionary
    """
    _options = dict()

    for key in option_type.keys:
        key_with_prefix = "{prefix}{key}".format(prefix=option_type.prefix, key=key)
        if key not in from_options and key_with_prefix not in from_options:
            _options[key] = ""
        elif key in from_options:
            _options[key] = from_options.get(key)
        else:
            _options[key] = from_options.get(key_with_prefix)

    return _options


def wrap_connection_error(fn):
    """ Wrapper for a connection error

        Parameters
        ----------
        fn : function

    """
    @functools.wraps(fn)
    def _wrapper(self, *args, **kw):
        log.debug("Requesting %s(%s, %s)", fn, args, kw)
        try:
            res = fn(self, *args, **kw)
        except requests.ConnectionError:
            raise exceptions.NoConnection(self.webdav.hostname)
        except requests.RequestException as re:
            raise exceptions.ConnectionException(re)
        else:
            return res
    return _wrapper


class Client(object):
    """The client for WebDAV servers provides an ability to control files on remote WebDAV server.
    """
    # path to root directory of WebDAV
    root = '/'

    # request timeout in seconds
    timeout = 30

    # controls whether to verify the server's TLS certificate or not
    verify = True

    # HTTP headers for different actions
    http_header = {
        'list': ["Accept: */*", "Depth: 1"],
        'free': ["Accept: */*", "Depth: 0", "Content-Type: text/xml"],
        'copy': ["Accept: */*"],
        'move': ["Accept: */*"],
        'mkdir': ["Accept: */*", "Connection: Keep-Alive"],
        'clean': ["Accept: */*", "Connection: Keep-Alive"],
        'check': ["Accept: */*"],
        'info': ["Accept: */*", "Depth: 1"],
        'get_property': ["Accept: */*", "Depth: 1", "Content-Type: application/x-www-form-urlencoded"],
        'set_property': ["Accept: */*", "Depth: 1", "Content-Type: application/x-www-form-urlencoded"]
    }

    def get_headers(self, action, headers_ext=None):
        """ Returns HTTP headers of specified WebDAV actions.

            Parameters
            ----------
            action : str
                The identifier of action.

            headers_ext : dict, optional
                The addition headers list which should be added to basic HTTP headers for
                the specified action.

            Returns
            -------
            dict
                The dictionary of headers for specified action.
        """
        if action in Client.http_header:
            try:
                headers = Client.http_header[action].copy()
            except AttributeError:
                headers = Client.http_header[action][:]
        else:
            headers = list()

        if headers_ext:
            headers.extend(headers_ext)

        if self.webdav.token:
            webdav_token = "Authorization: OAuth {token}".format(token=self.webdav.token)
            headers.append(webdav_token)
        return dict([map(lambda s: s.strip(), i.split(':')) for i in headers])

    def get_url(self, path):
        """ Generates url by uri path.

            Parameters
            ----------
            path : str
                The uri path.

            Returns
            -------
            str
                The url string.
        """
        url = {'hostname': self.webdav.hostname, 'root': self.webdav.root, 'path': path}
        return "{hostname}{root}{path}".format(**url)

    def get_full_path(self, urn):
        """ Generates full path to remote resource exclude hostname.

            Parameters
            ----------
            urn : str
                The URN to resource.

            Returns
            -------
            str
                Full path to resource with root path.
        """
        return "{root}{path}".format(root=self.webdav.root, path=urn.path())

    def execute_request(self, action, path, data=None, headers_ext=None):
        """ Generate request to WebDAV server for specified action and path and execute it.

            Parameters
            ----------
            action : str
                The action for WebDAV server which should be executed.

            path : str
                The path to resource for action

            data : dict, optional
                Dictionary or list of tuples ``[(key, value)]`` (will be form-encoded), bytes,
                or file-like object to send in the body of the :class:`Request`.

            headers_ext : dict, optional
                The addition headers list witch should be added to basic HTTP headers for
                the specified action.

            Returns
            -------
            request.response
                HTTP response of request.
        """
        response = requests.request(
            method=Client.requests[action],
            url=self.get_url(path),
            auth=(self.webdav.login, self.webdav.password),
            headers=self.get_headers(action, headers_ext),
            timeout=self.timeout,
            data=data,
            stream=True,
            verify=self.verify
        )
        if response.status_code == 507:
            raise exceptions.NotEnoughSpace()
        self.lastResponse = copy.deepcopy(response)
        return response

    # mapping of actions to WebDAV methods
    requests = {
        'download': "GET",
        'upload': "PUT",
        'copy': "COPY",
        'move': "MOVE",
        'mkdir': "MKCOL",
        'clean': "DELETE",
        'check': "HEAD",
        'list': "PROPFIND",
        'free': "PROPFIND",
        'info': "PROPFIND",
        'publish': "PROPPATCH",
        'unpublish': "PROPPATCH",
        'published': "PROPPATCH",
        'get_property': "PROPFIND",
        'set_property': "PROPPATCH"
    }

    meta_xmlns = {
        'https://webdav.yandex.ru': "urn:yandex:disk:meta",
    }

    def __init__(self, options):
        """ Constructor of WebDAV client

            Parameters
            ----------
            options : dict
                The dictionary of connection options to WebDAV can include proxy server options.
                WebDev settings:
                * `webdav_hostname`: url for WebDAV server should contain protocol and ip address or domain name.
                               Example: `https://webdav.server.com`.
                * `webdav_login`: (optional) login name for WebDAV server can be empty in case using of token auth.
                * `webdav_password`: (optional) password for WebDAV server can be empty in case using of token auth.
                * `webdav_token': (optional) token for WebDAV server can be empty in case using of login/password auth.
                * `webdav_root`: (optional) root directory of WebDAV server. Defaults is `/`.
                * `webdav_cert_path`: (optional) path to certificate.
                * `webdav_key_path`: (optional) path to private key.
                * `webdav_recv_speed`: (optional) rate limit data download speed in Bytes per second.
                                 Defaults to unlimited speed.
                * `webdav_send_speed`: (optional) rate limit data upload speed in Bytes per second.
                                 Defaults to unlimited speed.
                * `webdav_verbose`: (optional) set verbose mode on.off. By default verbose mode is off.

                Proxy settings (optional):
                * `proxy_hostname`: url to proxy server should contain protocol and ip address or domain name and if needed
                               port. Example: `https://proxy.server.com:8383`.
                * `proxy_login`: login name for proxy server.
                * `proxy_password`: password for proxy server.
        """
        webdav_options = get_options(option_type=WebDAVSettings, from_options=options)
        proxy_options = get_options(option_type=ProxySettings, from_options=options)

        self.webdav = WebDAVSettings(webdav_options)
        self.proxy = ProxySettings(proxy_options)
        self.default_options = {}
        self.lastResponse = None
        self.cwd = '/'

    def getresponse(self):
        """ Get the last response

            Returns
            -------
            requests.Response
                The most recent Response object
        """
        return self.lastResponse

    def getreason(self):
        """ Get the reason item from the most recent Response

            Returns
            -------
            str
                The reason text from the most recent Response
        """
        if self.lastResponse is None:
            return ""
        return self.lastResponse.reason

    def getstatus(self):
        """ Get the status code from the most recent Response

            Returns
            -------
            int
                The status code from the most recent Response
        """
        if self.lastResponse is None:
            return 0
        return self.lastResponse.status_code

    def valid(self):
        """ Validates of WebDAV and proxy settings.

            Returns
            -------
            bool
                True in case settings are valid and False otherwise.
        """
        return True if self.webdav.valid() and self.proxy.valid() else False

    @wrap_connection_error
    def list(self, remote_path=root):
        """ Returns list of nested files and directories for remote WebDAV directory by path.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PROPFIND

            Parameters
            ----------
            remote_path : str
                Path to remote directory.

            Returns
            -------
            list
                List of nested file or directory names.
        """
        directory_urn = Urn(remote_path, directory=True)
        if directory_urn.path() != Client.root:
            if not self.check(directory_urn.path()):
                raise exceptions.RemoteResourceNotFound(directory_urn.path())

        response = self.execute_request(action='list', path=directory_urn.quote())
        urns = WebDavXmlUtils.parse_get_list_response(response.content)

        path = Urn.normalize_path(self.get_full_path(directory_urn))
        return [urn.filename() for urn in urns if Urn.compare_path(path, urn.path()) is False]

    @wrap_connection_error
    def free(self):
        """ Returns an amount of free space on remote WebDAV server.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PROPFIND

            Returns
            -------
            int
                An amount of free space in bytes.
        """
        data = WebDavXmlUtils.create_free_space_request_content()
        response = self.execute_request(action='free', path='', data=data)
        return WebDavXmlUtils.parse_free_space_response(response.content, self.webdav.hostname)

    @wrap_connection_error
    def check(self, remote_path=root):
        """ Checks an existence of remote resource on WebDAV server by remote path.
            More information you can find by link http://webdav.org/specs/rfc4918.html#rfc.section.9.4

            Parameters
            ----------
            remote_path : str
                Path to resource on WebDAV server. Defaults is root directory of WebDAV.

            Returns
            -------
            bool
                True if resource is exist or False otherwise
        """
        urn = Urn(remote_path)
        try:
            response = self.execute_request(action='check', path=urn.quote())
        except exceptions.ResponseErrorCode:
            return False

        if int(response.status_code) == 200:
            return True
        return False

    @wrap_connection_error
    def mkdir(self, remote_path, safe=False):
        """ Makes new directory on WebDAV server.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_MKCOL

            Parameters
            ----------
            remote_path : str
                Path to directory

            safe : bool, optional
                Whether it is ok to receive status codes of 301 and 405.
                Default is ``False``.

            Returns
            -------
            bool
                True if request executed with code 200 or 201 and False otherwise.
        """
        expected_codes = (200, 201) if not safe else (201, 301, 405)
        directory_urn = Urn(remote_path, directory=True)
        if not self.check(directory_urn.parent()):
            raise exceptions.RemoteParentNotFound(directory_urn.path())

        response = self.execute_request(action='mkdir', path=directory_urn.quote())
        return response.status_code in expected_codes

    @wrap_connection_error
    def cd(self, path):
        """ Change to a new directory internally

            Parameters
            ----------
            path : str
                The new directory to change to
        """
        path = path.strip()
        if not path:
            return
        stripped_path = '/'.join(part for part in path.split('/') if part) + '/'
        if stripped_path == '/':
            self.cwd = stripped_path
        elif path.startswith('/'):
            self.cwd = '/' + stripped_path
        else:
            self.cwd += stripped_path

    @wrap_connection_error
    def mkdirs(self, path):
        """ Recursively make directories

            Parameters
            ----------
            path : str
                The path to make
        """
        dirs = [d for d in path.split('/') if d]
        if not dirs:
            return
        if path.startswith('/'):
            dirs[0] = '/' + dirs[0]
        old_cwd = self.cwd
        try:
            for d in dirs:
                try:
                    if not self.mkdir(d, safe=True):
                        if self.getstatus() == 409:
                            raise requests.RequestException("Return code 409 received, there is an unspecified conflict between this machine and the remote server.")
                finally:
                    self.cd(dir)
        finally:
            self.cd(old_cwd)

    @wrap_connection_error
    def download_from(self, buff, remote_path):
        """ Downloads file from WebDAV and writes it in buffer.

            Parameters
            ----------
            buff : buffer
                Buffer object for writing of downloaded file content.

            remote_path : str
                Path to file on WebDAV server.
        """
        urn = Urn(remote_path)
        if self.is_dir(urn.path()):
            raise exceptions.OptionNotValid(name="remote_path", value=remote_path)

        if not self.check(urn.path()):
            raise exceptions.RemoteResourceNotFound(urn.path())

        response = self.execute_request(action='download', path=urn.quote())
        shutil.copyfileobj(response.raw, buff)

    def download(self, remote_path, local_path):
        """ Downloads remote resource from WebDAV and save it in local path.
            More information you can find by link http://webdav.org/specs/rfc4918.html#rfc.section.9.4

            Parameters
            ----------
            remote_path : str
                The path to remote resource for downloading can be file and directory.
            local_path : str
                The path to save resource locally.
        """
        urn = Urn(remote_path)
        if self.is_dir(urn.path()):
            self.download_directory(local_path=local_path, remote_path=remote_path)
        else:
            self.download_file(local_path=local_path, remote_path=remote_path)

    def download_directory(self, remote_path, local_path):
        """ Downloads directory and downloads all nested files and directories from remote WebDAV to local.
            If there is something on local path it deletes directories and files then creates new.

            Parameters
            ----------
            remote_path : str
                The path to directory for downloading form WebDAV server.

            local_path : str
                The path to local directory for saving downloaded files and directories.
        """
        urn = Urn(remote_path, directory=True)
        if not self.is_dir(urn.path()):
            raise exceptions.OptionNotValid(name="remote_path", value=remote_path)

        if os.path.exists(local_path):
            shutil.rmtree(local_path)

        os.makedirs(local_path)

        for resource_name in self.list(urn.path()):
            _remote_path = "{parent}{name}".format(parent=urn.path(), name=resource_name)
            _local_path = os.path.join(local_path, resource_name)
            self.download(local_path=_local_path, remote_path=_remote_path)

    @wrap_connection_error
    def download_file(self, remote_path, local_path):
        """ Downloads file from WebDAV server and save it locally.
            More information you can find by link http://webdav.org/specs/rfc4918.html#rfc.section.9.4

            Parameters
            ----------
            remote_path : str
                The path to remote file for downloading.

            local_path : str
                The path to save file locally.
        """
        urn = Urn(remote_path)
        if self.is_dir(urn.path()):
            raise exceptions.OptionNotValid(name="remote_path", value=remote_path)

        if os.path.isdir(local_path):
            raise exceptions.OptionNotValid(name="local_path", value=local_path)

        if not self.check(urn.path()):
            raise exceptions.RemoteResourceNotFound(urn.path())

        with open(local_path, 'wb') as local_file:
            response = self.execute_request('download', urn.quote())
            for block in response.iter_content(1024):
                local_file.write(block)

    def download_sync(self, remote_path, local_path, callback=None):
        """ Downloads remote resources from WebDAV server synchronously.

            Parameters
            ----------
            remote_path : str
                The path to remote resource on WebDAV server. Can be file and directory.

            local_path : str
                The path to save resource locally.

            callback : function
                The callback which will be invoked when downloading is complete.
        """
        self.download(local_path=local_path, remote_path=remote_path)
        if callback:
            callback()

    def download_async(self, remote_path, local_path, callback=None):
        """ Downloads remote resources from WebDAV server asynchronously

            Parameters
            ----------
            remote_path : str
                The path to remote resource on WebDAV server. Can be file and directory.

            local_path : str
                The path to save resource locally.

            callback : function
                The callback which will be invoked when downloading is complete.
        """
        target = (lambda: self.download_sync(local_path=local_path, remote_path=remote_path, callback=callback))
        threading.Thread(target=target).start()

    @wrap_connection_error
    def upload_to(self, buff, remote_path):
        """ Uploads file from buffer to remote path on WebDAV server.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PUT

            Parameters
            ----------
            buff : buffer
                The buffer with content for file.

            remote_path : str
                The path to save file remotely on WebDAV server.
        """
        urn = Urn(remote_path)
        if urn.is_dir():
            raise exceptions.OptionNotValid(name="remote_path", value=remote_path)

        if not self.check(urn.parent()):
            raise exceptions.RemoteParentNotFound(urn.path())

        self.execute_request(action='upload', path=urn.quote(), data=buff)

    def upload(self, remote_path, local_path):
        """ Uploads resource to remote path on WebDAV server.
            In case resource is directory it will upload all nested files and directories.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PUT

            remote_path : str
                The path for uploading resources on WebDAV server. Can be file and directory.

            local_path : str
                The path to local resource for uploading.
        """
        if os.path.isdir(local_path):
            self.upload_directory(local_path=local_path, remote_path=remote_path)
        else:
            self.upload_file(local_path=local_path, remote_path=remote_path)

    def upload_directory(self, remote_path, local_path):
        """ Uploads directory to remote path on WebDAV server.
            In case directory is exist on remote server it will delete it and then upload directory with
            nested files and directories.

            Parameters
            ----------
            remote_path : str
                The path to directory for uploading on WebDAV server.

            local_path : str
                The path to local directory for uploading.
        """
        urn = Urn(remote_path, directory=True)
        if not urn.is_dir():
            raise exceptions.OptionNotValid(name="remote_path", value=remote_path)

        if not os.path.isdir(local_path):
            raise exceptions.OptionNotValid(name="local_path", value=local_path)

        if not os.path.exists(local_path):
            raise exceptions.LocalResourceNotFound(local_path)

        if self.check(urn.path()):
            self.clean(urn.path())

        self.mkdir(remote_path)

        for resource_name in listdir(local_path):
            _remote_path = "{parent}{name}".format(parent=urn.path(), name=resource_name)
            _local_path = os.path.join(local_path, resource_name)
            self.upload(local_path=_local_path, remote_path=_remote_path)

    @wrap_connection_error
    def upload_file(self, remote_path, local_path):
        """ Uploads file to remote path on WebDAV server. File should be 2Gb or less.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PUT

            Parameters
            ----------
            remote_path : str
                The path to uploading file on WebDAV server.

            local_path : str
                The path to local file for uploading.
        """
        if not os.path.exists(local_path):
            raise exceptions.LocalResourceNotFound(local_path)

        urn = Urn(remote_path)
        if urn.is_dir():
            raise exceptions.OptionNotValid(name="remote_path", value=remote_path)

        if os.path.isdir(local_path):
            raise exceptions.OptionNotValid(name="local_path", value=local_path)

        if not self.check(urn.parent()):
            raise exceptions.RemoteParentNotFound(urn.path())

        with open(local_path, "rb") as local_file:
            self.execute_request(action='upload', path=urn.quote(), data=local_file)

    def upload_sync(self, remote_path, local_path, callback=None):
        """ Uploads resource to remote path on WebDAV server synchronously.
            In case resource is directory it will upload all nested files and directories.

            Parameters
            ----------
            remote_path : str
                The path for uploading resources on WebDAV server. Can be file and directory.

            local_path : str
                The path to local resource for uploading.

            callback : function
                The callback which will be invoked when downloading is complete.
        """
        self.upload(local_path=local_path, remote_path=remote_path)

        if callback:
            callback()

    def upload_async(self, remote_path, local_path, callback=None):
        """ Uploads resource to remote path on WebDAV server asynchronously.
            In case resource is directory it will upload all nested files and directories.

            Parameters
            ----------
            remote_path : str
                The path for uploading resources on WebDAV server. Can be file and directory.

            local_path : str
                The path to local resource for uploading.

            callback : function
                The callback which will be invoked when downloading is complete.
        """
        target = (lambda: self.upload_sync(local_path=local_path, remote_path=remote_path, callback=callback))
        threading.Thread(target=target).start()

    @wrap_connection_error
    def copy(self, remote_path_from, remote_path_to, depth=1):
        """ Copies resource from one place to another on WebDAV server.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_COPY

            Parameters
            ----------
            remote_path_from : str
                The path to resource which will be copied,

            remote_path_to : str
                The path where resource will be copied.

            depth : int, optional
                Folder depth to copy, default is 1.
        """
        urn_from = Urn(remote_path_from)
        if not self.check(urn_from.path()):
            raise exceptions.RemoteResourceNotFound(urn_from.path())

        urn_to = Urn(remote_path_to)
        if not self.check(urn_to.parent()):
            raise exceptions.RemoteParentNotFound(urn_to.path())

        header_destination = "Destination: {path}".format(path=self.get_full_path(urn_to))
        header_depth = "Depth: {depth}".format(depth=depth)
        self.execute_request(action='copy', path=urn_from.quote(), headers_ext=[header_destination, header_depth])

    @wrap_connection_error
    def move(self, remote_path_from, remote_path_to, overwrite=False):
        """ Moves resource from one place to another on WebDAV server.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_MOVE

            Parameters
            ----------
            remote_path_from : str
                The path to resource which will be moved,

            remote_path_to : str
                The path where resource will be moved.

            overwrite: bool, optional
                Overwrite file if it exists. Defaults is False
        """
        urn_from = Urn(remote_path_from)
        if not self.check(urn_from.path()):
            raise exceptions.RemoteResourceNotFound(urn_from.path())

        urn_to = Urn(remote_path_to)
        if not self.check(urn_to.parent()):
            raise exceptions.RemoteParentNotFound(urn_to.path())

        header_destination = "Destination: {path}".format(path=self.get_full_path(urn_to))
        header_overwrite = "Overwrite: {flag}".format(flag="T" if overwrite else "F")
        self.execute_request(action='move', path=urn_from.quote(), headers_ext=[header_destination, header_overwrite])

    @wrap_connection_error
    def clean(self, remote_path):
        """ Cleans (Deletes) a remote resource on WebDAV server. The name of method is not changed for back compatibility
            with original library.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_DELETE

            Parameters
            ----------
            remote_path : str
                The remote resource whisch will be deleted.
        """
        urn = Urn(remote_path)
        self.execute_request(action='clean', path=urn.quote())

    @wrap_connection_error
    def info(self, remote_path):
        """ Gets information about resource on WebDAV.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PROPFIND

            Parameters
            ----------
            remote_path : str
                The path to remote resource.

            Returns
            -------
            dict
                A dictionary of information attributes and them values with following keys:
                * `created`: date of resource creation,
                * `name`: name of resource,
                * `size`: size of resource,
                * `modified`: date of resource modification.
        """
        urn = Urn(remote_path)
        if not self.check(urn.path()) and not self.check(Urn(remote_path, directory=True).path()):
            raise exceptions.RemoteResourceNotFound(remote_path)

        response = self.execute_request(action='info', path=urn.quote())
        path = self.get_full_path(urn)
        return WebDavXmlUtils.parse_info_response(content=response.content, path=path, hostname=self.webdav.hostname)

    @wrap_connection_error
    def is_dir(self, remote_path):
        """ Checks is the remote resource directory.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PROPFIND

            Returns
            -------
            remote_path : str
                The path to remote resource.

            Returns
            -------
            bool
                True in case the remote resource is directory and False otherwise.
        """
        urn = Urn(remote_path)
        parent_urn = Urn(urn.parent())
        if not self.check(urn.path()) and not self.check(Urn(remote_path, directory=True).path()):
            raise exceptions.RemoteResourceNotFound(remote_path)

        response = self.execute_request(action='info', path=parent_urn.quote())
        path = self.get_full_path(urn)
        return WebDavXmlUtils.parse_is_dir_response(content=response.content, path=path, hostname=self.webdav.hostname)

    @wrap_connection_error
    def get_property(self, remote_path, option):
        """ Gets metadata property of remote resource on WebDAV server.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PROPFIND

            Parameters
            ----------
            remote_path : str
                The path to remote resource.

            option : dict
                The property attribute as dictionary with following keys:
                * `namespace`: (optional) the namespace for XML property which will be set,
                * `name`: the name of property which will be set.

            Returns
            -------
            various
                The value of property or None if property is not found.
        """
        urn = Urn(remote_path)
        if not self.check(urn.path()):
            raise exceptions.RemoteResourceNotFound(urn.path())

        data = WebDavXmlUtils.create_get_property_request_content(option)
        response = self.execute_request(action='get_property', path=urn.quote(), data=data)
        return WebDavXmlUtils.parse_get_property_response(response.content, option['name'])

    @wrap_connection_error
    def set_property(self, remote_path, option):
        """ Sets metadata property of remote resource on WebDAV server.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PROPPATCH

            Parameters
            ----------
            remote_path : str
                The path to remote resource.

            option : dict
                The property attribute as dictionary with following keys:
                * `namespace`: (optional) the namespace for XML property which will be set,
                * `name`: the name of property which will be set,
                * `value`: (optional) the value of property which will be set. Defaults is empty string.
        """
        self.set_property_batch(remote_path=remote_path, option=[option])

    @wrap_connection_error
    def set_property_batch(self, remote_path, option):
        """ Sets batch metadata properties of remote resource on WebDAV server in batch.
            More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PROPPATCH

            Parameters
            ----------
            remote_path : str
                The path to remote resource.

            option : dict
                The property attributes as list of dictionaries with following keys:
                * `namespace`: (optional) the namespace for XML property which will be set,
                * `name`: the name of property which will be set,
                * `value`: (optional) the value of property which will be set. Defaults is empty string.
        """
        urn = Urn(remote_path)
        if not self.check(urn.path()):
            raise exceptions.RemoteResourceNotFound(urn.path())

        data = WebDavXmlUtils.create_set_property_batch_request_content(option)
        self.execute_request(action='set_property', path=urn.quote(), data=data)

    def resource(self, remote_path):
        """ Turn a remote path into a Resource object

            Parameters
            ----------
            remote_path : str
                The path to remote resource.

            Returns
            -------
            Resource object
        """
        urn = Urn(remote_path)
        return Resource(self, urn)

    def push(self, remote_directory, local_directory):
        """ Upload a directory to a remote server

            Parameters
            ----------
            remote_directory : str
                The remote directory where `local_directory` will land

            local_directory : str
                The local directory to upload


            Raises
            ------
            OptionNotValid, LocalResourceNotFound
                If there are errors in the paths
        """
        def prune(src, exp):
            """ Remove characters from a string

                Parameters
                ----------
                src : list
                    List of strings to be examined

                exp : str
                    The characters to replace.

                Returns
                -------
                list
                    List of strings with `exp` removed.
            """
            return [sub(exp, "", item) for item in src]

        urn = Urn(remote_directory, directory=True)

        if not self.is_dir(urn.path()):
            raise exceptions.OptionNotValid(name="remote_path", value=remote_directory)

        if not os.path.isdir(local_directory):
            raise exceptions.OptionNotValid(name="local_path", value=local_directory)

        if not os.path.exists(local_directory):
            raise exceptions.LocalResourceNotFound(local_directory)

        paths = self.list(urn.path())
        expression = "{begin}{end}".format(begin="^", end=urn.path())
        remote_resource_names = prune(paths, expression)

        for local_resource_name in listdir(local_directory):

            local_path = os.path.join(local_directory, local_resource_name)
            remote_path = "{remote_directory}{resource_name}".format(remote_directory=urn.path(),
                                                                     resource_name=local_resource_name)

            if os.path.isdir(local_path):
                if not self.check(remote_path=remote_path):
                    self.mkdir(remote_path=remote_path)
                self.push(remote_directory=remote_path, local_directory=local_path)
            else:
                if local_resource_name in remote_resource_names:
                    continue
                self.upload_file(remote_path=remote_path, local_path=local_path)

    def pull(self, remote_directory, local_directory):
        """ Download a directory to a remote server

            Parameters
            ----------
            remote_directory : str
                The remote directory which is to be transferred

            local_directory : str
                The local directory where `remote_directory` will land

            Raises
            ------
            OptionNotValid, LocalResourceNotFound
                If there are errors in the paths
        """

        def prune(src, exp):
            """ Remove characters from a string

                Parameters
                ----------
                src : list
                    List of strings to be examined

                exp : str
                    The characters to replace.

                Returns
                -------
                list
                    List of strings with `exp` removed.
            """
            return [sub(exp, "", item) for item in src]

        urn = Urn(remote_directory, directory=True)

        if not self.is_dir(urn.path()):
            raise exceptions.OptionNotValid(name="remote_path", value=remote_directory)

        if not os.path.exists(local_directory):
            raise exceptions.LocalResourceNotFound(local_directory)

        local_resource_names = listdir(local_directory)

        paths = self.list(urn.path())
        expression = "{begin}{end}".format(begin="^", end=remote_directory)
        remote_resource_names = prune(paths, expression)

        for remote_resource_name in remote_resource_names:

            local_path = os.path.join(local_directory, remote_resource_name)
            remote_path = "{remote_directory}{resource_name}".format(remote_directory=urn.path(),
                                                                     resource_name=remote_resource_name)

            remote_urn = Urn(remote_path)

            if self.is_dir(remote_urn.path()):
                if not os.path.exists(local_path):
                    os.mkdir(local_path)
                self.pull(remote_directory=remote_path, local_directory=local_path)
            else:
                if remote_resource_name in local_resource_names:
                    continue
                self.download_file(remote_path=remote_path, local_path=local_path)

    def sync(self, remote_directory, local_directory):
        """ Syncronize the local and remote directories.

            Parameters
            ----------
            remote_directory : str
                The remote directory which is to be synced.

            local_directory : str
                The local directory which is to be synced.
        """
        self.pull(remote_directory=remote_directory, local_directory=local_directory)
        self.push(remote_directory=remote_directory, local_directory=local_directory)


class Resource(object):
    """ Class for resource management

        Parameters
        ----------
        client : Client
            The Client instance to use

        urn : str
            The URN to operate with
    """
    def __init__(self, client, urn):
        self.client = client
        self.urn = urn

    def __str__(self):
        return "resource {path}".format(path=self.urn.path())

    def is_dir(self):
        """ Determine if the URN is a directory

            Returns
            -------
            bool
                Whether or not the URN is a directory
        """
        return self.client.is_dir(self.urn.path())

    def rename(self, new_name):
        """ Rename the URN

            Parameters
            ----------
            new_name : str
                The new name for the URN
        """
        old_path = self.urn.path()
        parent_path = self.urn.parent()
        new_name = Urn(new_name).filename()
        new_path = "{directory}{filename}".format(directory=parent_path, filename=new_name)

        self.client.move(remote_path_from=old_path, remote_path_to=new_path)
        self.urn = Urn(new_path)

    def move(self, remote_path):
        """ Move the URN

            Parameters
            ----------
            remote_path : str
                The path to move the URN to
        """
        new_urn = Urn(remote_path)
        self.client.move(remote_path_from=self.urn.path(), remote_path_to=new_urn.path())
        self.urn = new_urn

    def copy(self, remote_path):
        """ Copy the URN and return a Resource object for it.

            Parameters
            ----------
            remote_path : str
                The path for the copy of the URN

            Returns
            -------
            Resource object
        """
        urn = Urn(remote_path)
        self.client.copy(remote_path_from=self.urn.path(), remote_path_to=remote_path)
        return Resource(self.client, urn)

    def info(self, params=None):
        """ Get info from the URN

            Parameters
            ----------
            params : dict
                Additional parameters for the info

            Returns
            -------
            dict
                Dictionary containing the info
        """
        info = self.client.info(self.urn.path())
        if not params:
            return info

        return {key: value for (key, value) in info.items() if key in params}

    def clean(self):
        """ Clean (delete) the URN
        """
        return self.client.clean(self.urn.path())

    def check(self):
        """ Check the URN
        """
        return self.client.check(self.urn.path())

    def read_from(self, buff):
        """ Upload buff to the URN

            Parameters
            ----------
            buff : buffer
                The buffer to upload
        """
        self.client.upload_to(buff=buff, remote_path=self.urn.path())

    def read(self, local_path):
        """ Upload the specified path

            Parameters
            ----------
            local_path : str
                The path to upload
        """
        return self.client.upload_sync(local_path=local_path, remote_path=self.urn.path())

    def read_async(self, local_path, callback=None):
        """ Upload the specified path asyncronized

            Parameters
            ----------
            local_path : str
                The path to upload

            callback : function
                The callback for the result.
        """
        return self.client.upload_async(local_path=local_path, remote_path=self.urn.path(), callback=callback)

    def write_to(self, buff):
        """ Download the URN to a buffer

            Parameters
            ----------
            buff : buffer
                The buffer to write to
        """
        return self.client.download_from(buff=buff, remote_path=self.urn.path())

    def write(self, local_path):
        """ Download the URN to the local file system

            Parameters
            ----------
            local_path : str
                The path where the data should land
        """
        return self.client.download_sync(local_path=local_path, remote_path=self.urn.path())

    def write_async(self, local_path, callback=None):
        """ Download the URN to the local file system asyncronously

            Parameters
            ----------
            local_path : str
                The path where the data should land
        """
        return self.client.download_async(local_path=local_path, remote_path=self.urn.path(), callback=callback)

    def publish(self):
        """ Publish the URN
        """
        return self.client.publish(self.urn.path())

    def unpublish(self):
        """ Unpublish the URN
        """
        return self.client.unpublish(self.urn.path())

    @property
    def property(self, option):
        """ Property getting function
        """
        return self.client.get_property(remote_path=self.urn.path(), option=option)

    @property.setter
    def property(self, option, value):
        """ Property setting function
        """
        option['value'] = value.__str__()
        self.client.set_property(remote_path=self.urn.path(), option=option)


class WebDavXmlUtils(object):
    """ Class for interacting with a webdav server,
    """
    def __init__(self):
        pass

    @staticmethod
    def parse_get_list_response(content):
        """ Parses of response content XML from WebDAV server and extract file and directory names.

            Parameters
            ----------
            content : str
                The XML content of HTTP response from WebDAV server for getting list of files by remote path.

            Returns
            -------
            list
                List of extracted file or directory names.
        """
        try:
            tree = etree.fromstring(content)
            hrees = [Urn.separate + unquote(urlsplit(hree.text).path) for hree in tree.findall(".//{DAV:}href")]
            return [Urn(hree) for hree in hrees]
        except etree.XMLSyntaxError:
            return list()

    @staticmethod
    def create_free_space_request_content():
        """ Creates an XML for requesting of free space on remote WebDAV server.

            Returns
            -------
            str
                The XML string of request content.
        """
        root = etree.Element("propfind", xmlns="DAV:")
        prop = etree.SubElement(root, "prop")
        etree.SubElement(prop, "quota-available-bytes")
        etree.SubElement(prop, "quota-used-bytes")
        tree = etree.ElementTree(root)
        return WebDavXmlUtils.etree_to_string(tree)

    @staticmethod
    def parse_free_space_response(content, hostname):
        """ Parses of response content XML from WebDAV server and extract an amount of free space.

            Parameters
            ----------
            content : str
                The XML content of HTTP response from WebDAV server for getting free space.

            hostname : str
                The server hostname.

            Returns
            -------
            int
                An amount of free space in bytes.
        """
        try:
            tree = etree.fromstring(content)
            node = tree.find('.//{DAV:}quota-available-bytes')
            if node is not None:
                return int(node.text)
            else:
                raise exceptions.MethodNotSupported(name='free', server=hostname)
        except TypeError:
            raise exceptions.MethodNotSupported(name='free', server=hostname)
        except etree.XMLSyntaxError:
            return str()

    @staticmethod
    def parse_info_response(content, path, hostname):
        """ Parses of response content XML from WebDAV server and extract an information about resource.

            Parameters
            ----------
            content : str
                The XML content of HTTP response from WebDAV server.

            path : str
                The path to resource.

            hostname : str
                The server hostname.

            Returns
            -------
            dict
                A dictionary of information attributes and them values with following keys:

                * `created`: date of resource creation,
                * `name`: name of resource,
                * `size`: size of resource,
                * `modified`: date of resource modification.
        """
        response = WebDavXmlUtils.extract_response_for_path(content=content, path=path, hostname=hostname)
        find_attributes = {
            'created': ".//{DAV:}creationdate",
            'name': ".//{DAV:}displayname",
            'size': ".//{DAV:}getcontentlength",
            'modified': ".//{DAV:}getlastmodified"
        }
        info = dict()
        for (name, value) in find_attributes.items():
            info[name] = response.findtext(value)
        return info

    @staticmethod
    def parse_is_dir_response(content, path, hostname):
        """ Parses of response content XML from WebDAV server and extract an information about resource.

            Parameters
            ----------
            content : str
                The XML content of HTTP response from WebDAV server.

            path : str
                The path to resource.

            hostname : str
                The server hostname.

            Returns
            -------
            bool
                ``True`` in case the remote resource is directory and ``False`` otherwise.
        """
        response = WebDavXmlUtils.extract_response_for_path(content=content, path=path, hostname=hostname)
        resource_type = response.find(".//{DAV:}resourcetype")
        if resource_type is None:
            raise exceptions.MethodNotSupported(name="is_dir", server=hostname)
        dir_type = resource_type.find("{DAV:}collection")

        return True if dir_type is not None else False

    @staticmethod
    def create_get_property_request_content(option):
        """ Creates an XML for requesting of getting a property value of remote WebDAV resource.

            Parameters
            ----------
            option : dict
                The property attributes as dictionary with following keys:

                * `namespace`: (optional) the namespace for XML property which will be get,
                * `name`: the name of property which will be get.

            Returns
            -------
            str
                The XML string of request content.
        """
        root = etree.Element("propfind", xmlns="DAV:")
        prop = etree.SubElement(root, "prop")
        etree.SubElement(prop, option.get('name', ""), xmlns=option.get('namespace', ""))
        tree = etree.ElementTree(root)
        return WebDavXmlUtils.etree_to_string(tree)

    @staticmethod
    def parse_get_property_response(content, name):
        """ Parses of response content XML from WebDAV server for getting metadata property
            value for some resource.

            Parameters
            ----------
            content : str
                The XML content of response as string.

            name : str
                The name of property for finding a value in response.

            Returns
            -------
            various
                The value of property if it has been found or ``None`` otherwise.
        """
        tree = etree.fromstring(content)
        return tree.xpath('//*[local-name() = $name]', name=name)[0].text

    @staticmethod
    def create_set_property_batch_request_content(options):
        """ Creates an XML for requesting of setting a property values for remote WebDAV resource in batch.

            Parameters
            ----------
            options : dict
                The property attributes as list of dictionaries with following keys:

                * `namespace`: (optional) the namespace for XML property which will be set,
                * `name`: the name of property which will be set,
                * `value`: (optional) the value of property which will be set. Defaults is empty string.

            Returns
            -------
            str
                The XML string of request content.
        """
        root_node = etree.Element('propertyupdate', xmlns='DAV:')
        set_node = etree.SubElement(root_node, 'set')
        prop_node = etree.SubElement(set_node, 'prop')
        for option in options:
            opt_node = etree.SubElement(prop_node, option['name'], xmlns=option.get('namespace', ''))
            opt_node.text = option.get('value', '')
        tree = etree.ElementTree(root_node)
        return WebDavXmlUtils.etree_to_string(tree)

    @staticmethod
    def etree_to_string(tree):
        """ Creates string from lxml.etree.ElementTree with XML declaration and UTF-8 encoding.

            Parameters
            ----------
            tree : lxml.etree.ElementTree
                The instance of ElementTree

            Returns
            -------
            str
                The string of XML.
        """
        buff = BytesIO()
        tree.write(buff, xml_declaration=True, encoding='UTF-8')
        return buff.getvalue()

    @staticmethod
    def extract_response_for_path(content, path, hostname):
        """ Extracts single response for specified remote resource.

            Parameters
            ----------
            content : str
                Raw content of response as string.

            path : str
                The path to needed remote resource.

            hostname : str
                The server hostname.

            Returns
            -------
            etree
                XML object of response for the remote resource defined by path.
        """
        try:
            tree = etree.fromstring(content)
            responses = tree.findall("{DAV:}response")

            n_path = Urn.normalize_path(path)

            for resp in responses:
                href = resp.findtext("{DAV:}href")

                if Urn.compare_path(n_path, href) is True:
                    return resp
            raise exceptions.RemoteResourceNotFound(path)
        except etree.XMLSyntaxError:
            raise exceptions.MethodNotSupported(name="is_dir", server=hostname)
