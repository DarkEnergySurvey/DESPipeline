#!/usr/bin/env python

# $Id: fpackcompworker.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.
"""
    Class for running FPack on a file
"""
__version__ = "$Rev: 11430 $"

import traceback
import filemgmt.disk_utils_local as diskutils
from filemgmt.compworker import CompWorker


class FpackCompWorker(CompWorker):
    """ Class for running FPack on a file

        Parameters
        ----------
        cleanup : bool
            Whether to do cleanup (True) or not (False)
    """
    def __init__(self, cleanup=False):
        super(FpackCompWorker, self).__init__(cleanup)

    def get_exebase(self):
        """ Get the base of the exec

            Returns
            -------
            str of the exec base
        """
        return "fpack"

    def get_cleanup(self):
        """ Get the cleanup value

            Returns
            -------
            list of the values
        """
        return ["-D", "-Y"]

    def get_extention(self):
        """ Get the default extension for the compression

            Returns
            -------
            str containing the extension
        """
        return ".fz"

    def get_exe_version_args(self):
        """ Get the command line argument needed to get the executable version

            Returns
            -------
            list of the command line arguments
        """
        return ["-V"]

    def execute(self, filename):
        """ Run the executable

            Paramters
            ---------
            filename : str
                The name of the file to compress

            Returns
            -------
            int of the executable return status
        """
        newfilename = filename + self.get_extention()
        try:
            diskutils.remove_file_if_exists(newfilename)
        except:
            self._errmsg = traceback.format_exc()
            return 1
        return super(FpackCompWorker, self).execute(file)
