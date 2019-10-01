#!/usr/bin/env python
"""
    .. _filemgmt-compworker:

    **compworker**
    --------------

    Base class for doing file compression
"""

__version__ = "$Rev: 11430 $"

import argparse
import subprocess
import traceback
from abc import ABCMeta, abstractmethod
import despymisc.miscutils as miscutils


class CompWorker(object):
    """ Base class for file compression

        Parameters
        ----------
        cleanup : bool
            Whether to do cleanup (True) or not (False)
    """
    __metaclass__ = ABCMeta

    _passthroughargs = None
    _cleanup = None
    _dateFormat = '%Y-%m-%d %H:%M:%S'
    _retcode = None
    _errmsg = None

    def __init__(self, cleanup=False):
        self._passthroughargs = args.split()
        self._cleanup = cleanup

    @abstractmethod
    def get_cleanup(self):
        """ Base class definition, must be overloaded
        """
        return None

    @abstractmethod
    def get_exebase(self):
        """ Base class definition, must be overloaded
        """
        return None

    @abstractmethod
    def get_extention(self):
        """ Base class definition, must be overloaded
        """
        return None

    @abstractmethod
    def get_exe_version_args(self):
        """ Base class definition, must be overloaded
        """
        return None

    def get_errmsg(self):
        """ Get the error message

            Returns
            -------
            str containing the message
        """
        return self._errmsg

    def get_exe_version(self):
        """ Get the version of the actual executable

            Returns
            -------
            str of the version
        """
        cmdlist = filter(None, [self.get_exebase()] + self.get_exe_version_args())
        return (subprocess.check_output(cmdlist)).strip()

    def get_commandargs(self):
        """ Return the command line arguments

            Returns
            -------
            str of the command line arguments
        """
        return ' '.join(self.get_commandargs_list())

    def get_commandargs_list(self):
        """ Return the command line arguments as a list

            Returns
            -------
            list of the command line arguments
        """
        if self._cleanup:
            return filter(None, self.get_cleanup() + self._passthroughargs)
        return filter(None, self._passthroughargs)

    def execute(self, filename):
        """ Call the executable with the stored command line arguments

            Parameters
            ----------
            filename : str
                The name of the file to compress

            Returns
            -------
            int of the executable return status
        """
        cmdlist = [self.get_exebase()] + self.get_commandargs_list() + [filename]
        try:
            self._errmsg = subprocess.check_output(cmdlist, stderr=subprocess.STDOUT, shell=False)
            self._retcode = 0
        except subprocess.CalledProcessError as e:
            self._retcode = e.returncode
            self._errmsg = e.output
        except:
            self._retcode = 1
            self._errmsg = traceback.format_exc()
        return self._retcode


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Calls a subprogram to compress a file')
    parser.add_argument('--file', action='store')
    parser.add_argument('--exeargs', action='store', default="")
    parser.add_argument('--cleanup', action='store_true', default=False)
    parser.add_argument('--class', action='store', default="filemgmt.fpackcompworker.FpackCompWorker")

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    if "file" not in args or "class" not in args:
        exit(1)

    compressor = miscutils.dynamically_load_class(args["class"])(args["cleanup"], args["exeargs"])
    print "full_commandline=" + compressor.get_exebase() + ' ' + compressor.get_commandargs()
    # compressor.execute(args["file"])
    print "version=" + compressor.get_exe_version()
