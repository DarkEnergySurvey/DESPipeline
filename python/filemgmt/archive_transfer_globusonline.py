"""
    .. _filemgmt-archive-transfer-globusonline:

    **archive_transfer_globusonline**
    ---------------------------------

    Class for transferring files via globus.
"""

__version__ = "$Rev: 41008 $"

import os
import copy

import despymisc.miscutils as miscutils
import filemgmt.gonline as globonline
import filemgmt.filemgmt_defs as fmdefs

GO_USER = 'go_user'
X509_USER_PROXY = 'x509_user_proxy'
PROXY_VALID_HRS = 'proxy_valid_hrs'

class ArchiveTransferGlobusOnline(object):
    """ Class for transferring files via globus online. It assumes that globus credentials
        are stored in X509_USER_PROXY in either the config file or environment variable.
        NOTE: This currently uses an old globus packge (globusonline)

        Parameters
        ----------
        src_archive_info : dict
            Dictionary containing the source archive info, for downloading files

        dst_archive_info : dict
            Dictionary containing the destination archive info, for uploading files

        archive_transfer_info : dict
            Unused in this class, it is here to comply with the calling sequence of the
            other transfer classes

        config : dict
            Dictionary of the configuration data, default is None
    """
    # assumes src and dst archives are not on same machine and should use GlobusOnline for transfers

    @staticmethod
    def requested_config_vals():
        """ return the possible config vals and whether they are required """
        return {GO_USER: fmdefs.REQUIRED,
                X509_USER_PROXY: fmdefs.OPTIONAL,
                PROXY_VALID_HRS: fmdefs.OPTIONAL
               }

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
                Dictionary of the files to be transferred

            Returns
            -------
            Transfer results

        """
        #print "blocking_transfer"
        #print "\tfilelist: ", filelist

        srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']

        files2copy = copy.deepcopy(filelist)
        for fname, _ in filelist.items():
            files2copy[fname]['src'] = "%s/%s" % (srcroot, files2copy[fname]['src'])
            files2copy[fname]['dst'] = "%s/%s" % (dstroot, files2copy[fname]['dst'])


        credfile = None
        if X509_USER_PROXY in self.config:
            credfile = self.config[X509_USER_PROXY]
        elif 'X509_USER_PROXY' in os.environ:
            credfile = os.environ['X509_USER_PROXY']

        if credfile is None:
            miscutils.fwdie('Error:  Cannot determine location of X509 proxy.  Either set in config or environment.', 1)

        proxy_valid_hrs = 12
        if PROXY_VALID_HRS in self.config:
            proxy_valid_hrs = self.config[PROXY_VALID_HRS]

        if GO_USER not in self.config:
            miscutils.fwdie('Error:  Missing %s in config' % GO_USER, 1)

        goclient = globonline.DESGlobusOnline(self.src_archive_info, self.dst_archive_info, credfile,
                                              self.config[GO_USER], proxy_valid_hrs)
        return goclient.blocking_transfer(files2copy)

    ######################################################################
    def transfer_directory(self, relpath):
        """ Transfer a directory between two archives

            Parameters
            ----------
            relpath : str
                The directory to transfer

            Returns
            -------
            Dict of the transfer results
        """

        if miscutils.fwdebug_check(0, "ARCHIVE_TRANSFER_GLOBUSONLINE"):
            miscutils.fwdebug_print("\trelpath: %s" % relpath)

        srcpath = "%s/%s" % (self.src_archive_info['root'], relpath)
        dstpath = "%s/%s" % (self.dst_archive_info['root'], relpath)

        credfile = None
        if X509_USER_PROXY in self.config:
            credfile = self.config[X509_USER_PROXY]
        elif 'X509_USER_PROXY' in os.environ:
            credfile = os.environ['X509_USER_PROXY']

        if credfile is None:
            miscutils.fwdie('Error:  Cannot determine location of X509 proxy.  Either set in config or environment.', 1)

        proxy_valid_hrs = 12
        if PROXY_VALID_HRS in self.config:
            proxy_valid_hrs = self.config[PROXY_VALID_HRS]

        if GO_USER not in self.config:
            miscutils.fwdie('Error:  Missing %s in config' % GO_USER, 1)

        goclient = globonline.DESGlobusOnline(self.src_archive_info, self.dst_archive_info, credfile,
                                              self.config[GO_USER], proxy_valid_hrs)
        _ = goclient.transfer_directory(srcpath, dstpath)


        # get listing of remote directory
        dstlisting = goclient.get_directory_listing(dstpath, self.dst_archive_info['endpoint'], True)

        retresults = {}
        for fullname, finfo in dstlisting.items():
            filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)
            if finfo is not None:   # include labels required by framework
                if finfo['type'] == 'file':
                    retresults[filename] = finfo
                    retresults[filename]['filesize'] = retresults[filename]['size']
                    retresults[filename]['fullname'] = fullname

        # check for missing files
        srclisting = goclient.get_directory_listing(srcpath, self.src_archive_info['endpoint'], True)
        for fullname, finfo in srclisting.items():
            filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)
            if finfo is not None and finfo['type'] == 'file' and filename not in retresults:
                retresults[filename] = finfo
                retresults[filename]['filesize'] = retresults[filename]['size']
                retresults[filename]['fullname'] = fullname
                retresults[filename]['err'] = 'Unknown error'

        return retresults
