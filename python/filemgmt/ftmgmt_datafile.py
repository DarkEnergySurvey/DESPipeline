"""
    .. _filemgmt-ftmgmt-datafile:

    **ftmgmt_datafile**
    -------------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

__version__ = "$Rev: 41700 $"

from filemgmt.ftmgmt_generic import FtMgmtGeneric

class FtMgmtDatafile(FtMgmtGeneric):
    """  Class for managing a filetype whose contents can be read by datafile_ingest

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
        # config must have filetype_metadata and file_header_info
        FtMgmtGeneric.__init__(self, filetype, config, filepat=None)
