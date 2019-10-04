"""
    .. _filemgmt-ftmgmt-fitsdatafile:

    **ftmgmt_fitsdatafile**
    -----------------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

from filemgmt.ftmgmt_genfits import FtMgmtGenFits

class FtMgmtFitsDatafile(FtMgmtGenFits):
    """  Class for managing a fits file

        Parameters
        ----------
        filetype : str
            The filetype being worked with

        config : dict
            Dictionary of config values

        filepat : str, optional
            File pattern naming string, default is ``None``
    """

    ######################################################################
    def __init__(self, filetype, config, filepat=None):
        # config must have filetype_metadata and file_header_info
        FtMgmtGenFits.__init__(self, filetype, config, filepat)
