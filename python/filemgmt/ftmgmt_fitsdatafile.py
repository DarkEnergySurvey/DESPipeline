#!/usr/bin/env python

# $Id: ftmgmt_fitsdatafile.py 41700 2016-04-19 19:23:55Z mgower $
# $Rev:: 41700                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-04-19 14:23:55 #$:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev: 41700 $"

from filemgmt.ftmgmt_genfits import FtMgmtGenFits

import despymisc.miscutils as miscutils

class FtMgmtFitsDatafile(FtMgmtGenFits):
    """  Class for managing a filetype whose contents can be read by datafile_ingest """

    ######################################################################
    def __init__(self, filetype, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtGenFits.__init__(self, filetype, config, filepat)
