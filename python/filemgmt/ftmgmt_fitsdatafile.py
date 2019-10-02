"""
    .. _filemgmt-ftmgmt-fitsdatafile:

    **ftmgmt_fitsdatafile**
    -----------------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

__version__ = "$Rev: 41700 $"

from filemgmt.ftmgmt_genfits import FtMgmtGenFits

class FtMgmtFitsDatafile(FtMgmtGenFits):
    """  Class for managing a filetype whose contents can be read by datafile_ingest """

    ######################################################################
    def __init__(self, filetype, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtGenFits.__init__(self, filetype, config, filepat)
