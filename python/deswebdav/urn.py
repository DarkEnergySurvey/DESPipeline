"""
    Webdav Urn for DES based on webdav-client-python3 (https://github.com/ezhov-evgeny/webdav-client-python-3), version 0.12
"""
try:
    from urllib.parse import unquote, quote, urlsplit
except ImportError:
    from urllib import unquote, quote
    from urlparse import urlsplit

from re import sub


class Urn(object):
    """ A URN object for webdav

        Parameters
        ----------
        path : str
            The path of the object

        directory : bool, optional
            Specifies whther `path` is a directory and not a file.
    """
    separate = "/"

    def __init__(self, path, directory=False):

        self._path = quote(path)
        expressions = r"/\.+/", "/+"
        for expression in expressions:
            self._path = sub(expression, Urn.separate, self._path)

        if not self._path.startswith(Urn.separate):
            self._path = "{begin}{end}".format(begin=Urn.separate, end=self._path)

        if directory and not self._path.endswith(Urn.separate):
            self._path = "{begin}{end}".format(begin=self._path, end=Urn.separate)

    def __str__(self):
        return self.path()

    def path(self):
        """ Get the path

            Returns
            -------
            str
                The path
        """
        return unquote(self._path)

    def quote(self):
        """ Get the path with quotes

            Returns
            -------
            str
                The path with quotes
        """
        return self._path

    def filename(self):
        """ Get only the file name

            Returns
            -------
            str
                The file name.
        """
        path_split = self._path.split(Urn.separate)
        name = path_split[-2] + Urn.separate if path_split[-1] == '' else path_split[-1]
        return unquote(name)

    def parent(self):
        """ Get the parent directory

            Parameters
            ----------
            str
                The parent directory
        """
        path_split = self._path.split(Urn.separate)
        nesting_level = self.nesting_level()
        parent_path_split = path_split[:nesting_level]
        parent = self.separate.join(parent_path_split) if nesting_level != 1 else Urn.separate
        if not parent.endswith(Urn.separate):
            return unquote(parent + Urn.separate)
        else:
            return unquote(parent)

    def nesting_level(self):
        """ Determine how deep in the directory structure the target is.

            Returns
            -------
            int
                How deep the target is.
        """
        return self._path.count(Urn.separate, 0, -1)

    def is_dir(self):
        """ Determines if the target is a directory

            Returns
            -------
            bool
                Whether the target is a directory or not
        """
        return self._path[-1] == Urn.separate

    @staticmethod
    def normalize_path(path):
        """ Replace multiple '/' with a single instance

            Parameters
            ----------
            path : str
                The string to do the replacement on

            Returns
            -------
            str
                `path` with the replacement done
        """
        result = sub('/{2,}', '/', path)
        return result if len(result) < 1 or result[-1] != Urn.separate else result[:-1]

    @staticmethod
    def compare_path(path_a, href):
        """ Determine if a path and a full url are the same target.

            Parameters
            ----------
            path_a : str
                The path to compare.

            href : str
                The URL to compare

            Returns
            -------
            bool
                Whether they represent the same target.
        """
        unqouted_path = Urn.separate + unquote(urlsplit(href).path)
        return Urn.normalize_path(path_a) == Urn.normalize_path(unqouted_path)
