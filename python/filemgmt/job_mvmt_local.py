""" Move files locally
"""

__version__ = "$Rev: 46423 $"

import copy

import despymisc.miscutils as miscutils
import filemgmt.disk_utils_local as disk_utils_local

class JobArchiveLocal(object):
    """ Class for transferring files via cp

        Parameters
        ----------
        homeinfo : dict
            Dictionary of data on the sending machine

        targetinfo : dict
            Dictionary of data on the destination machine

        mvmtinfo : unused

        tstats : dict
            Dictionary for tracking transfer statistics

        config : dict
            Dictionary of config values, default is None
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        """ Get the configuration values for this class
        """
        return {}

    def __init__(self, homeinfo, targetinfo, mvmtinfo, tstats, config=None):
        self.home = homeinfo
        self.target = targetinfo
        self.mvmt = mvmtinfo
        self.config = config
        self.tstats = tstats

    def home2job(self, filelist):
        """ Stage and transfer files from the archive to the job

            Parameters
            ----------
            filelist : dict
                Dictionary containing the file names and path information

            Returns
            -------
            dict of the results
        """
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)

        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")

        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.home['root'] + '/' + finfo['src']

        if self.tstats is not None:
            self.tstats.stat_beg_batch('home2job', self.home['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
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
            dict of the results
        """
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.target['root'] + '/' + finfo['src']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('target2job', self.target['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
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
            dict of the results
        """
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.target['root'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2target', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
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
            dict of the results
        """
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.home['root'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2home', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats, verify)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results
