"""
    .. _filemgmt-ftmgmt-raw:

    **ftmgmt_raw**
    --------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

__version__ = "$Rev: 46337 $"

import os
from datetime import datetime
import pyfits

from ftmgmt_genfits import FtMgmtGenFits
import decam_keywords
import despymisc.miscutils as miscutils
import despymisc.create_special_metadata as spmeta

class FtMgmtRaw(FtMgmtGenFits):
    """  Class for managing a raw filetype (get metadata, update metadata, etc)

        Parameters
        ----------
        filetype : str
            The filetype being worked with

        config : dict
            Dictionary of config values

        filepat : str
            File pattern naming string, default is None

    """
    ######################################################################
    def __init__(self, filetype, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata, file_header_info, keywords_file (OPT)
        FtMgmtGenFits.__init__(self, filetype, config, filepat)

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values

            Parameters
            ----------
            fullname : str
                The name of the file to gather data from

            do_update : bool
                Whether to update the metadata of the file from update_info

            update_info : dict
                The data to update the header with

            Returns
            -------
            dict containing the metadata
        """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # open file
        #hdulist = pyfits.open(fullname, 'update')
        primary_hdr = pyfits.getheader(fullname, 0)
        prihdu = pyfits.PrimaryHDU(header=primary_hdr)
        hdulist = pyfits.HDUList([prihdu])


        # read metadata and call any special calc functions
        metadata, _ = self._gather_metadata_file(fullname, hdulist=hdulist)
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))

        # call function to update headers
        if do_update:
            miscutils.fwdebug_print("WARN: cannot update a raw file's metadata")

        # close file
        hdulist.close()

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata

    ######################################################################
    def check_valid(self, listfullnames):
        """ Check whether the given files are valid raw files """

        assert isinstance(listfullnames, list)

        results = {}
        for fname in listfullnames:
            results[fname] = False

        keywords = {'pri':{}, 'ext':{}}
        for keyname, value in decam_keywords.keywords.items():
            keywords['pri'][keyname] = value[0].upper()
            keywords['ext'][keyname] = value[1].upper()

        for fname in listfullnames:
            results[fname] = check_single_valid(keywords, fname, 0)

        return results


######################################################################
def check_single_valid(keywords, fullname, verbose): # should raise exception if not valid
    """ Check whether the given file is a valid raw file

        Parameters
        ----------
        keywords : dict
            Keywords to look for

        fullname : str
            The name of the file

        verbose : bool
            Whether or not to print out extra info to stdout

        Returns
        -------
        bool
    """

    # check fits file
    hdulist = pyfits.open(fullname)
    prihdr = hdulist[0].header

    # check exposure has correct filename (sometimes get NOAO-science-archive renamed exposures)
    correct_filename = prihdr['FILENAME']
    actual_filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)
    if actual_filename != correct_filename:
        raise ValueError('Error: invalid filename (%s)' % actual_filename)


    instrume = prihdr['INSTRUME'].lower()

    req_num_hdus = -1
    if instrume == 'decam':
        req_num_hdus = 71
    else:
        raise ValueError('Error:  Unknown instrume (%s)' % instrume)

    # check # hdus
    num_hdus = len(hdulist)
    if num_hdus != req_num_hdus:
        raise ValueError('Error:  Invalid number of hdus (%s)' % num_hdus)

    # check keywords
    for hdunum in range(0, num_hdus):
        hdr = hdulist[hdunum].header
        (req, want, extra) = check_header_keywords(keywords, hdunum, hdr)

        if verbose > 1:
            if want is not None and want:
                print "HDU #%02d Missing requested keywords: %s" % (hdunum, want)
            if extra is not None and extra:
                print "HDU #%02d Extra keywords: %s" % (hdunum, extra)

        if req is not None and req:
            raise ValueError('Error: HDU #%02d Missing required keywords (%s)' % (hdunum, req))

    return True

######################################################################
def check_header_keywords(keywords, hdunum, hdr):
    """ Check for keywords in header

        Parameters
        ----------
        keywords : dict
            Keywords to look for

        hdunum : int
            the HDU number

        hdr : pyfits.Header
            The header object

        Returns
        -------
        tuple containing required key words that were not found, optional key words
        that were not found, and extra key words that were found

    """
    # missing has the keywords which are missing in the file and are required for processing
    # extra are the keywords which are not required and are present in the system
    # not required are the ones which are not required and are not present

    req_missing = []
    want_missing = []
    extra = []

    hdutype = 'ext'
    if hdunum == 0:
        hdutype = 'pri'

    for keyw, status in keywords[hdutype].items():
        if keyw not in hdr:
            if status == 'R':
                req_missing.append(keyw)
            elif status == 'Y':
                want_missing.append(keyw)

    # check for extra keywords
    for keyw in hdr:
        if keyw not in keywords[hdutype] or \
            keywords[hdutype][keyw] == 'N':
            extra.append(keyw)

    return (req_missing, want_missing, extra)
