"""
    .. _filemgmt-ftmgmt-datafile:

    **ftmgmt_datafile**
    -------------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

from filemgmt.ftmgmt_generic import FtMgmtGeneric

class FtMgmtDatafile(FtMgmtGeneric):
    """  Class for managing a generic data file type

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
        FtMgmtGeneric.__init__(self, filetype, config, filepat=None)
