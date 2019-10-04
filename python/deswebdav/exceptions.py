"""
    Webdav exceptions for DES based on webdav-client-python3 (https://github.com/ezhov-evgeny/webdav-client-python-3), version 0.12
"""
class WebDavException(Exception):
    """ Base WebDav Exception """
    pass


class NotValid(WebDavException):
    """ Base invalid exception """
    pass


class OptionNotValid(NotValid):
    """ Exception for options which are not valid

        Parameters
        ----------
        name : str
            The name of the invalid option.

        value : various
            The invalid value of the option.

        ns : str, optional
            String to prepend to the name, default is an empty string.
    """
    def __init__(self, name, value, ns=""):
        NotValid.__init__(self)
        self.name = name
        self.value = value
        self.ns = ns

    def __str__(self):
        return "Option ({ns}{name}={value}) have invalid name or value".format(ns=self.ns, name=self.name,
                                                                               value=self.value)


class CertificateNotValid(NotValid):
    """ Base invalid certificate exception """
    pass


class NotFound(WebDavException):
    """ Base Not Found exception """
    pass


class LocalResourceNotFound(NotFound):
    """ Exception for a local resource which was not found

        Parameters
        ----------
        path : str
            The path to the local file which was not found

    """
    def __init__(self, path):
        NotFound.__init__(self)
        self.path = path

    def __str__(self):
        return "Local file: {path} not found".format(path=self.path)


class RemoteResourceNotFound(NotFound):
    """ Exception for a remote resource which was not found

        Parameters
        ----------
        path : str
            The path of the file which was not found.
    """
    def __init__(self, path):
        NotFound.__init__(self)
        self.path = path

    def __str__(self):
        return "Remote resource: {path} not found".format(path=self.path)


class RemoteParentNotFound(NotFound):
    """ Exception for a remote parent which was not found.

        Parameters
        ----------
        path : str
            The path to the file whose parent was not found.
    """
    def __init__(self, path):
        NotFound.__init__(self)
        self.path = path

    def __str__(self):
        return "Remote parent for: {path} not found".format(path=self.path)


class MethodNotSupported(WebDavException):
    """ Exception for when a requested method is not currently supported.

        Parameter
        ---------
        name : str
            The name of the unsupported method.

        server : str
            The server on which the method was requested.
    """
    def __init__(self, name, server):
        WebDavException.__init__(self)
        self.name = name
        self.server = server

    def __str__(self):
        return "Method {name} not supported for {server}".format(name=self.name, server=self.server)


class ConnectionException(WebDavException):
    """ Exception for a connection issue

        Parameters
        ----------
        exception : Exception
            The exception which this one wraps.
    """
    def __init__(self, exception):
        WebDavException.__init__(self)
        self.exception = exception

    def __str__(self):
        return self.exception.__str__()


class NoConnection(WebDavException):
    """ Exception for when a connection could bot be established.

        Parameters
        ----------
        hostname : str
            The host which could not be contacted.
    """
    def __init__(self, hostname):
        WebDavException.__init__(self)
        self.hostname = hostname

    def __str__(self):
        return "Not connection with {hostname}".format(hostname=self.hostname)


# This exception left only for supporting original library interface.
class NotConnection(WebDavException):
    """ Exception for when a connection could bot be established.

        Parameters
        ----------
        hostname : str
            The host which could not be contacted.

    """
    def __init__(self, hostname):
        WebDavException.__init__(self)
        self.hostname = hostname

    def __str__(self):
        return "No connection with {hostname}".format(hostname=self.hostname)


class ResponseErrorCode(WebDavException):
    """ Exception for an http error code.

        Parameters
        ----------
        url : str
            The URL which generated the error code.

        code : int
            The error code.

        message : str
            The error message.
    """
    def __init__(self, url, code, message):
        WebDavException.__init__(self)
        self.url = url
        self.code = code
        self.message = message

    def __str__(self):
        return "Request to {url} failed with code {code} and message: {message}".format(url=self.url, code=self.code,
                                                                                        message=self.message)

class NotEnoughSpace(WebDavException):
    """ Exception for when there is not enough space on the destination.
    """
    def __init__(self):
        WebDavException.__init__(self)

    def __str__(self):
        return "Not enough space on the server"
