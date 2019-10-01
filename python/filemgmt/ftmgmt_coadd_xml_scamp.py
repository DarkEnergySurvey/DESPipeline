"""
    .. _filemgmt-ftmgmt-coadd-xml-scamp:

    **ftmgmt_coadd_xml_scamp**
    --------------------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

__version__ = "$Rev: 41948 $"

from filemgmt.ftmgmt_datafile import FtMgmtDatafile

class FtMgmtCoaddXmlScamp(FtMgmtDatafile):
    """  Class for managing filetype coadd_xml_scamp

        Parameters
        ----------
        filetype : str
            The file type

        config : dict
            Dictionary of the config parameters

        filepat : str
            The file pattern string. Not used in this instance, but
            variable is required for compatability.
    """

    ######################################################################
    def __init__(self, filetype, config, filepat=None):
        # config must have filetype_metadata and file_header_info
        FtMgmtDatafile.__init__(self, filetype, config, filepat=None)

        #self.filetype should be 'coadd_xml_scamp'
        self.filetype2 = 'coadd_xml_scamp_2'
