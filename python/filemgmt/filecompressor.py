#!/usr/bin/env python

"""
    .. _filemgmt-filecompressor:

    **filecompressor**
    ------------------

    Class for compressing files
"""

__version__ = "$Rev: 11430 $"

import os
import sys
import time
import argparse
import despymisc.miscutils as miscutils
import filemgmt.disk_utils_local as diskutils

class FileCompressor(object):
    """ Class for compressing files

        Parameters
        ----------
        infile : str
            The name of the file to compress

        compressor : object
            Class for doing the actual compression

        cleanup : bool
            Currently not used
    """
    _infile_full = None
    _outfile_full = None
    _infile_artifact_id = None
    _outfile_artifact_id = None
    _archive_name = None
    _archive_root = None
    _path = None
    _filename = None
    _ext = None
    _compressor = None
    _infile_size = None
    _outfile_size = None
    _outfile_md5sum = None
    _retcode = None
    _cleanup = None
    _debugDateFormat = '%Y-%m-%d %H:%M:%S'
    _errmsg = None

    def __init__(self, infile, compressor, cleanup=False):
        self._compressor = compressor
        self._ext = self._compressor.get_extention()
        self._cleanup = cleanup
        self._infile_full = infile
        self._infile_size = os.path.getsize(infile)
        self._outfile_full = self._infile_full + self._ext

    def printprefix(self):
        """ Print the prefix of writing a line to output

            Returns
            -------
            Str of the prefix
        """
        return time.strftime(self._debugDateFormat) + " - "

    def execute(self):
        """ Do the actual compression

            Returns
            -------
            int of the compression return code
        """
        sys.stdout.write(self.printprefix() + "compressing %s...." % self._infile_full)
        sys.stdout.flush()
        self._retcode = self._compressor.execute(self._infile_full)
        if self._retcode == 0:
            print "done.",
            finfo = diskutils.get_single_file_disk_info(self._outfile_full, save_md5sum=True, archive_root=None)
            self._outfile_size = finfo['filesize']
            self._outfile_md5sum = finfo['md5sum']
            print "CR=%.2f:1" % (float(self._infile_size) / self._outfile_size)
        else:
            print "ERROR"
            self._errmsg = self._compressor.get_errmsg()
        return self._retcode

    def getOutfileFullpath(self):
        """ Get the full path of the output file

            Returns
            -------
            Str containing the full path and file name
        """
        return self._outfile_full

    def getInfileFullpath(self):
        """ Get the full path of the input file

            Returns
            -------
            str containing the full path and file name
        """
        return self._infile_full

    def getInfileSize(self):
        """ Get the size of the input file

            Returns
            -------
            int of the file size
        """
        return self._infile_size

    def getOutfileSize(self):
        """ Get the size of the output file

            Returns
            -------
            int of the file size
        """
        return self._outfile_size

    def getErrMsg(self):
        """ Get the error message

            Returns
            -------
            str containing the error message
        """
        return self._errmsg

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Compress files and update the database')
    parser.add_argument('--file', action='store')
    parser.add_argument('-e', '--exeargs', action='store', default="",
                        help='arguments to pass through to the compression application')
    parser.add_argument('--cleanup', action='store_true', default=False,
                        help='after compression, delete the uncompressed version')
    parser.add_argument('--class', action='store', default='fpackcompworker.FpackCompWorker',
                        help='the package.class of the compression application wrapper. Default is "fpackcompworker.FpackCompWorker"')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    #artifacts = getFilesToCompress(args,dbh)
    artifacts = []
    compressor = miscutils.dynamically_load_class(args["class"])(args["cleanup"], args["exeargs"])

    prov = {}
    for rowdict in artifacts:
        filecompressor = FileCompressor(infile=args["file"],
                                        compressor=compressor,
                                        cleanup=args["cleanup"])
        retcode = filecompressor.execute()

    print str(prov)
