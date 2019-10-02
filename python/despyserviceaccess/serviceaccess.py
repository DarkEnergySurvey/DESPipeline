"""
    .. _despyserviceaccess-serviceaccess:

    **serviceaccess**
    -----------------

    Support for service access files for holding credentials for file transfer
    and database access. See :ref:`serviceaccessDescription` for details
    on the use of these files.
"""

import os
import time
import subprocess
import ConfigParser

class ServiceaccessException(Exception):
    """ Class for specific ServiceAccess errors

        Parameters
        ----------
        txt : str
            The error message
    """
    def __init__(self, txt):
        Exception.__init__(self)
        self.txt = txt

    def __str__(self):
        return self.txt


expectedkeys = ("meta_section", "meta_file")

def parse(file_name, section, tag=None, retry=False):
    """ Parse a serviceaccess file, return a dictionary of key values pairs
        containing the entries. See :ref:`serviceaccessDescription` for
        details on the use of a service access file.

        Parameters
        ----------
        file_name : str
            The name of the service access file to open. If this is ``None`` then
            the code will look in the ``DES_SERVICES`` environment variable for
            the file name. Failing this it will assume the file is ``$HOME/.desservices.ini``

        section : str
            The section within the services file to read. If this is ``None`` then
            the code will look in the ``DES_<tag>__SECTION`` environment variable for
            the section name (``<tag>`` is whatever is given by the `tag` argument)

        tag : str, optional
            The string used in looking up the environment variable for the section name.
            If `section` is ``None`` then this cannot be ``None``.
            Default value is ``None``.

        retry : bool
            Whether to retry reading the services file on an IOError. If ser to ``True`` then
            it will retry up to 5 times (sleeping 30 seconds between each) before exiting with an error.
            Default is ``False``.

        Returns
        -------
        dict
            A dictionary containing the key-value pairs from the file

        Raises
        ------
        ServiceaccessException
            If there is an issue with the arguments.

        IOError
            If there is an issue reading the service access file.
    """
    if not file_name:
        file_name = os.getenv("DES_SERVICES")
    if not file_name:
        file_name = os.path.join(os.getenv("HOME"), ".desservices.ini")
    if not section and tag:
        section = os.getenv("DES_%s_SECTION" % tag.upper())
    if not section:
        raise ServiceaccessException('faulty section: %s' % section)

    # config parser throws "no section error" if file does not exist....
    # ... That's Confusing. so do an open to get a more understandable error.
    # to allow for automounting filesystems, retry on failures
    maxtries = 1
    if retry:
        maxtries = 5
    trycnt = 0
    delay = 30
    success = False
    exc = None
    while not success and trycnt <= maxtries:
        trycnt += 1
        try:
            open(file_name)
            success = True
        except IOError as exc:
            if trycnt < maxtries:
                print "IOError: %s" % exc
                print "Sleeping for %s seconds and retrying" % delay
                try:
                    # try triggering automount
                    process = subprocess.Popen(['ls', '-l', file_name], shell=False,
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.STDOUT)
                    process.wait()
                    #print process.communicate()
                except Exception:
                    pass
                time.sleep(delay)
            else:
                raise

    c = ConfigParser.RawConfigParser()
    c.read(file_name)
    d = {}
    [d.__setitem__(key, value) for (key, value) in c.items(section)]
    d["meta_file"] = file_name
    d["meta_section"] = section

    return d

def check(d):
    """ Perform a basic check on the file permission to make sure that the file being read
        is secure enough. See :ref:`serviceaccessDescription` for specifics on this
        requirement.

        Parameters
        ----------
        d : dict
            Dictionary of the file name and other data.

        Raises
        ------
        ServiceaccessException
            If there is an issue with the file permissions.
    """
    import stat
    permission_faults = []
    permission_checks = (("other_read", stat.S_IROTH), ("other_write", stat.S_IWOTH),
                         ("group_write", stat.S_IWGRP))
    permissions = os.stat(d["meta_file"])[0]
    for (text, bit) in permission_checks:
        if permissions & bit:
            permission_faults.append(text)
    if permission_faults:
        raise ServiceaccessException("faulty permissions : %s " % (permission_faults))
