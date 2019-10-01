"""
    .. _filemgmt-archive-transfer-local:

    **archive_transfer_local**
    --------------------------

    Class for transferring files locally
"""

__version__ = "$Rev: 41008 $"


import copy
import despymisc.miscutils as miscutils
import filemgmt.disk_utils_local as disk_utils_local

class ArchiveTransferLocal(object):
    """ Class for transferring files locally

        Parameters
        ----------
        src_archive_info : dict
            Dictionary containing the source archive info, for downloading files

        dst_archive_info : dict
            Dictionary containing the destination archive info, for uploading files

        archive_transfer_info : dcit
            Unused in this class, it is here to comply with the calling sequence of the
            other transfer classes

        config : dict
            Dictionary of the configuration data

    """
    @staticmethod
    def requested_config_vals():
        """ get the available config values """
        return {}    # no extra values needed

    # assumes home and target are on same machine

    def __init__(self, src_archive_info, dst_archive_info, archive_transfer_info, config=None):
        #pylint: disable=unused-argument
        self.src_archive_info = src_archive_info
        self.dst_archive_info = dst_archive_info
        self.config = config

    def blocking_transfer(self, filelist):
        """ Do a blocking transfer

            Parameters
            ----------
            filelist : dict
                Dictionary of the files to transfer

            Returns
            -------
            TRansfer results
        """
        miscutils.fwdebug_print("\tNumber files to transfer: %d" % len(filelist))
        if miscutils.fwdebug_check(1, "ARCHIVETRANSFER_DEBUG"):
            miscutils.fwdebug_print("\tfilelist: %s" % filelist)

        srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']

        files2copy = copy.deepcopy(filelist)
        for _, finfo in files2copy.items():
            finfo['src'] = '%s/%s' % (srcroot, finfo['src'])
            finfo['dst'] = '%s/%s' % (dstroot, finfo['dst'])

        transresults = disk_utils_local.copyfiles(files2copy, None)

        return transresults
