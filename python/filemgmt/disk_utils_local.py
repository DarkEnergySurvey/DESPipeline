"""
    .. _filemgmt-disk-utils-local:

    **disk_utils_local**
    --------------------

    Generic routines for performing tasks on files that can be seen locally
"""

import os
import sys
import shutil
import hashlib
import errno

import despymisc.miscutils as miscutils


######################################################################
def get_md5sum_file(fullname, blksize=2**15):
    """ Returns md5 checksum for given file

        Parameters
        ----------
        fullname : str
            The name of the file to do the md5sum of

        blksize : int, long
            The block size to read from the file, default is 2**15

        Returns
        -------
        str
            The md5sum
    """
    md5 = hashlib.md5()
    with open(fullname, 'rb') as fhandle:
        for chunk in iter(lambda: fhandle.read(blksize), ''):
            md5.update(chunk)
    return md5.hexdigest()

######################################################################
def get_file_disk_info(arg):
    """ Returns information about files on disk from given list or path

        Parameters
        ----------
        arg : various
            A list containing the path to probe, or a string of file to probe


        Returns
        -------
        dict
            The requested info.
    """

    if isinstance(arg, list):
        return get_file_disk_info_list(arg)
    elif isinstance(arg, str):
        return get_file_disk_info_path(arg)
    else:
        miscutils.fwdie("Error:  argument to get_file_disk_info isn't a list or a path (%s)" % type(arg), 1)

######################################################################
def get_single_file_disk_info(fname, save_md5sum=False, archive_root=None):
    """ Method to get disk info for a single file

        Parameters
        ----------
        fname : str
            The name of the file

        save_md5sum : bool
            Whether to calculate the md5sum (True) or no (False), default is False

        archive_root : str
            The archive root path to prepend to the output data, default is None
    """
    if miscutils.fwdebug_check(3, "DISK_UTILS_LOCAL_DEBUG"):
        miscutils.fwdebug_print("fname=%s, save_md5sum=%s, archive_root=%s" % \
                                (fname, save_md5sum, archive_root))

    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION

    (path, filename, compress) = miscutils.parse_fullname(fname, parsemask)
    if miscutils.fwdebug_check(3, "DISK_UTILS_LOCAL_DEBUG"):
        miscutils.fwdebug_print("path=%s, filename=%s, compress=%s" % (path, filename, compress))

    fdict = {'filename' : filename,
             'compression': compress,
             'path': path,
             'filesize': os.path.getsize(fname)
            }

    if save_md5sum:
        fdict['md5sum'] = get_md5sum_file(fname)

    if archive_root and path.startswith('/'):
        fdict['relpath'] = path[len(archive_root)+1:]

        if compress is None:
            compext = ""
        else:
            compext = compress

        fdict['rel_filename'] = "%s/%s%s" % (fdict['relpath'], filename, compext)

    return fdict


######################################################################
def get_file_disk_info_list(filelist, save_md5sum=False):
    """ Returns information about files on disk from given list

        Parameters
        ----------
        filelist : list
            List of files to probe

        save_md5sum : bool
            Whether to calculate the md5sum (True) or no (False), default is False

        Returns
        -------
        dict
            The resulting data
    """

    fileinfo = {}
    for fname in filelist:
        if os.path.exists(fname):
            fileinfo[fname] = get_single_file_disk_info(fname, save_md5sum)
        else:
            fileinfo[fname] = {'err': "Could not find file"}

    return fileinfo

######################################################################
def get_file_disk_info_path(path, save_md5sum=False):
    """ Returns information about files on disk from given path

        Parameters
        ----------
        path : str
            String of the path to probe

        save_md5sum : bool
            Whether to calculate the md5sum (True) or no (False), default is False

        Returns
        -------
        dict
            The resulting data

    """
    # if relative path, is treated relative to current directory
    if not os.path.exists(path):
        miscutils.fwdie("Error:  path does not exist (%s)" % (path), 1)

    fileinfo = {}
    for (dirpath, _, filenames) in os.walk(path):
        for name in filenames:
            fname = os.path.join(dirpath, name)
            fileinfo[fname] = get_single_file_disk_info(fname, save_md5sum)

    return fileinfo

######################################################################
def copyfiles(filelist, tstats, verify=False):
    """ Copies files in given src,dst in filelist

        Parameters
        ----------
        filelist : list
            List of files to transfer

        tstats : object
            Class for tracking the statistics of the transfer

        verify : bool
            Whether to verify the file transfer (if possible) (True) or
            not (False), default is False

        Returns
        -------
        tuple
            The exit status of the transfer and a dictionary of
            the transfer results
    """

    status = 0
    for filename, fdict in filelist.items():
        fsize = 0
        try:
            src = fdict['src']
            dst = fdict['dst']

            if 'filesize' in fdict:
                fsize = fdict['filesize']
            elif os.path.exists(src):
                fsize = os.path.getsize(src)

            if not os.path.exists(dst):
                if tstats is not None:
                    tstats.stat_beg_file(filename)
                path = os.path.dirname(dst)
                if path and not os.path.exists(path):
                    miscutils.coremakedirs(path)
                shutil.copy(src, dst)
                if tstats is not None:
                    tstats.stat_end_file(0, fsize)
                if verify:
                    newfsize = os.path.getsize(dst)
                    if newfsize != fsize:
                        raise Exception("Incorrect files size for file %s (%i vs %i)" % (filename, newfsize, fsize))
        except Exception:
            status = 1
            if tstats is not None:
                tstats.stat_end_file(1, fsize)
            (_, value, _) = sys.exc_info()
            filelist[filename]['err'] = str(value)
    return (status, filelist)

######################################################################
def remove_file_if_exists(filename):
    """ Method to remove a single file if it exisits

        Parameters
        ----------
        filename : str
            The name of the file to delete
    """
    try:
        os.remove(filename)
    except OSError as exc:
        if exc.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise
# end remove_file_if_exists
