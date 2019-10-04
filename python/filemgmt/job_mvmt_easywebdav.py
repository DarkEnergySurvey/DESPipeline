"""
    .. _filemgmt-job-mvmt-easywebdav:

    **job_mvmt_easywebdav**
    -----------------------

    Class for transferring files via webdav
"""

import copy
import re

import despymisc.miscutils as miscutils
import filemgmt.easywebdav_utils as ewd_utils
import filemgmt.filemgmt_defs as fmdefs

DES_SERVICES = 'des_services'
DES_HTTP_SECTION = 'des_http_section'

class JobArchiveEwd(object):
    """ Class for transferring files via webdav

        Parameters
        ----------
        homeinfo : dict
            Dictionary of data on the sending machine

        targetinfo : dict
            Dictionary of data on the destination machine

        mvmtinfo : unused

        tstats : dict
            Dictionary for tracking transfer statistics

        config : dict, optional
            Dictionary of config values, default is ``None``
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        """ Get the configuration values for this class
        """
        return {DES_SERVICES: fmdefs.REQUIRED,
                DES_HTTP_SECTION: fmdefs.REQUIRED}

    def __init__(self, homeinfo, targetinfo, mvmtinfo, tstats, config=None):
        #pylint: disable=unused-argument
        self.home = homeinfo
        self.target = targetinfo
        self.config = config
        self.tstats = tstats

        m = re.match(r"(http://[^/]+)(/.*)", homeinfo['root_http'])
        dest = m.group(1)
        for x in (DES_SERVICES, DES_HTTP_SECTION):
            if x not in self.config:
                miscutils.fwdie('Error:  Missing %s in config' % x, 1)
        self.HU = ewd_utils.EwdUtils(self.config[DES_SERVICES],
                                     self.config[DES_HTTP_SECTION],
                                     dest.replace('http://', ''))


    def home2job(self, filelist):
        """ Stage and transfer files from the archive to the job

            Parameters
            ----------
            filelist : dict
                Dictionary containing the file names and path information

            Returns
            -------
            dict
                The results
        """
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")

        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.home['root_http'] + '/' + finfo['src']

        if self.tstats is not None:
            self.tstats.stat_beg_batch('home2job', self.home['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def target2job(self, filelist):
        """ Transfer files from the target archive

            Parameters
            ----------
            filelist : dict
                Dictionary containing the file names and path information

            Returns
            -------
            dict
                The results
        """
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.target['root_http'] + '/' + finfo['src']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('target2job', self.target['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def job2target(self, filelist):
        """ Transfer files from the job to the target archive

            Parameters
            ----------
            filelist : dict
                Dictionary containing the file names and path information

            Returns
            -------
            dict
                The results
        """
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.target['root_http'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2target', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def job2home(self, filelist, verify=False):
        """ Transfer files from the job to the home archive

            Parameters
            ----------
            filelist : dict
                Dictionary containing the file names and path information

            Returns
            -------
            dict
                The results
        """
        # if staging outside job, this function shouldn't be called
        if verify:
            ver = 'F'
        else:
            ver = None
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.home['root_http'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2home', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats, verify=ver)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results
