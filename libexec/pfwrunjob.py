#!/usr/bin/env python
# $Id: pfwrunjob.py 48552 2019-05-20 19:38:27Z friedel $
# $Rev:: 48552                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate: 2019-04-05 12:01:17 #$:  # Date of last commit.

# pylint: disable=global-statement

""" Executes a series of wrappers within a single job """

import re
import subprocess
import argparse
import sys
import os
import time
import shutil
import copy
import traceback
import socket
from collections import OrderedDict
import multiprocessing as mp
import multiprocessing.pool as pl
import signal
import threading
import Queue
import psutil

import despymisc.miscutils as miscutils
import despymisc.provdefs as provdefs
import filemgmt.filemgmt_defs as fmdefs
import filemgmt.disk_utils_local as diskutils
from intgutils.wcl import WCL
import intgutils.intgdefs as intgdefs
import intgutils.intgmisc as intgmisc
import intgutils.replace_funcs as replfuncs
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import processingfw.pfwcompression as pfwcompress

__version__ = '$Rev: 48552 $'

pool = None
stop_all = False
jobfiles_global = {}
jobwcl = None
job_track = {}
keeprunning = True
terminating = False
main_lock = threading.Lock()
result_lock = threading.Lock()
lock_monitor = threading.Condition(threading.Lock())
donejobs = 0
results = None

os.environ['PYTHONUNBUFFERED'] = '1'

class WrapOutput(object):
    """ Class to capture printed output and stdout and reformat it to append
        the wrapper number to the lines

        Parameters
        ----------
        wrapnum : int
            The wrapper number to prepend to the lines

    """
    def __init__(self, wrapnum, connection):
        try:
            self.isqueue = not isinstance(connection, file)
            self.connection = connection
            self.wrapnum = int(wrapnum)
        except:
            (extype, exvalue, trback) = sys.exc_info()
            traceback.print_exception(extype, exvalue, trback, file=sys.stdout)

    def write(self, text):
        """ Method to capture, reformat, and write out the requested text

            Parameters
            ----------
            text : str
                The text to reformat

        """
        try:
            text = text.rstrip()
            if not text == 0:
                return
            text = text.replace("\n", "\n%04d: " % (self.wrapnum))
            text = "\n%04d: " % (self.wrapnum) + text
            if self.isqueue:
                self.connection.put(text, timeout=120)
            else:
                self.connection.write(text)
                self.connection.flush()
        except:
            (extype, exvalue, trback) = sys.exc_info()
            traceback.print_exception(extype, exvalue, trback, file=sys.stdout)

    def close(self):
        """ Method to return stdout to its original handle
        """
        if not self.isqueue:
            return self.connection
        return None

    def flush(self):
        """ Method to force the buffer to flush

        """
        if not self.isqueue:
            self.connection.flush()


######################################################################
def save_trans_end_of_job(wcl, jobfiles, putinfo):
    """ If transfering at end of job, save file info for later """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("len(putinfo) = %d" % len(putinfo))

    job2target = 'never'
    if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl:
        job2target = wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower()
    job2home = 'never'
    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl:
        job2home = wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower()

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("job2target = %s" % job2target)
        miscutils.fwdebug_print("job2home = %s" % job2home)

    if putinfo:
        # if not end of job and transferring at end of job, save file info for later
        if job2target == 'job' or job2home == 'job':
            if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                miscutils.fwdebug_print("Adding %s files to save later" % len(putinfo))
            jobfiles['output_putinfo'].update(putinfo)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def transfer_job_to_archives(wcl, jobfiles, putinfo, level, task_label, exitcode):
    """ Call the appropriate transfers based upon which archives job is using """
    #  level: current calling point: wrapper or job

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG %s %s" % (level, task_label))
        miscutils.fwdebug_print("len(putinfo) = %d" % len(putinfo))
        miscutils.fwdebug_print("putinfo = %s" % putinfo)

    level = level.lower()
    job2target = 'never'
    if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl:
        job2target = wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower()
    job2home = 'never'
    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl:
        job2home = wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower()

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("job2target = %s" % job2target)
        miscutils.fwdebug_print("job2home = %s" % job2home)

    if putinfo:
        saveinfo = None
        if level == job2target or level == job2home:
            saveinfo = output_transfer_prep(wcl, jobfiles, putinfo,
                                            task_label, exitcode)

        if level == job2target:
            transfer_job_to_single_archive(wcl, saveinfo, 'target',
                                           task_label)

        if level == job2home:
            transfer_job_to_single_archive(wcl, saveinfo, 'home',
                                           task_label)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def dynam_load_filemgmt(wcl, archive_info):
    """ Dynamically load filemgmt class """

    if archive_info is None:
        if ((pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl and
             wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() != 'never') or
                (pfwdefs.USE_HOME_ARCHIVE_INPUT in wcl and
                 wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() != 'never')):
            archive_info = wcl['home_archive_info']
        elif ((pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl and
               wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower() != 'never') or
              (pfwdefs.USE_TARGET_ARCHIVE_INPUT in wcl and
               wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() != 'never')):
            archive_info = wcl['target_archive_info']
        else:
            raise Exception('Error: Could not determine archive for output files. Check USE_*_ARCHIVE_* WCL vars.')
    filemgmt = pfwutils.pfw_dynam_load_class(wcl, 'filemgmt', archive_info['filemgmt'], None)
    return filemgmt


######################################################################
def dynam_load_jobfilemvmt(wcl, tstats):
    """ Dynamically load job file mvmt class """

    jobfilemvmt = None
    try:
        jobfilemvmt_class = miscutils.dynamically_load_class(wcl['job_file_mvmt']['mvmtclass'])
        valdict = miscutils.get_config_vals(wcl['job_file_mvmt'], wcl,
                                            jobfilemvmt_class.requested_config_vals())
        jobfilemvmt = jobfilemvmt_class(wcl['home_archive_info'], wcl['target_archive_info'],
                                        wcl['job_file_mvmt'], tstats, valdict)
    except Exception as err:
        msg = "Error: creating job_file_mvmt object\n%s" % err
        print "ERROR\n%s" % msg
        raise

    return jobfilemvmt


######################################################################
def pfw_save_file_info(filemgmt, ftype, fullnames,
                       do_update, update_info, filepat):
    """ Call and time filemgmt.register_file_data routine for pfw created files """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG (%s)" % (ftype))
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("fullnames=%s" % (fullnames))
        miscutils.fwdebug_print("do_update=%s, update_info=%s" % (do_update, update_info))

    starttime = time.time()
    res = {}
    listing = []

    try:
        res = filemgmt.register_file_data(ftype, fullnames, do_update, update_info, filepat)
        filemgmt.commit()

        # if some files failed to register data then the task failed
        for k, v in res.iteritems():
            if v is None:
                listing.append(k)

        print "DESDMTIME: pfw_save_file_info %0.3f" % (time.time()-starttime)
    except:
        (extype, exvalue, trback) = sys.exc_info()
        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)

        print "DESDMTIME: pfw_save_file_info %0.3f" % (time.time()-starttime)
        raise

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

    return listing

######################################################################
def transfer_single_archive_to_job(wcl, files2get, jobfiles, dest):
    """ Handle the transfer of files from a single archive to the job directory """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    archive_info = wcl['%s_archive_info' % dest.lower()]

    res = None
    transinfo = get_file_archive_info(wcl, files2get, jobfiles,
                                      archive_info,)

    if len(transinfo) != len(files2get):
        badfiles = []
        for file_name in files2get:
            if file_name not in transinfo.keys():
                badfiles.append(file_name)
        raise Exception("Error: the following files did not have entries in the database:\n%s" % (", ".join(badfiles)))
    if transinfo:
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("\tCalling target2job on %s files" % len(transinfo))
        starttime = time.time()
        tasktype = '%s2job' % dest
        tstats = None
        if 'transfer_stats' in wcl:
            tstats = pfwutils.pfw_dynam_load_class(wcl, 'stats_' + tasktype, wcl['transfer_stats'], None)

        jobfilemvmt = dynam_load_jobfilemvmt(wcl, tstats)

        if dest.lower() == 'target':
            res = jobfilemvmt.target2job(transinfo)
        else:
            res = jobfilemvmt.home2job(transinfo)

    print "DESDMTIME: %s2job %0.3f" % (dest.lower(), time.time()-starttime)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

    return res



######################################################################
def transfer_archives_to_job(wcl, neededfiles):
    """ Call the appropriate transfers based upon which archives job is using """
    # transfer files from target/home archives to job scratch dir

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
    if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("neededfiles = %s" % neededfiles)

    files2get = neededfiles.keys()

    arc = ""
    if 'home_archive' in wcl and 'archive' in wcl:
        ha = wcl['home_archive']
        if ha in wcl['archive'] and 'root_http' in wcl['archive'][ha]:
            arc = ' (' + wcl['archive'][wcl['home_archive']]['root_http'] + ')'

    if files2get and wcl[pfwdefs.USE_TARGET_ARCHIVE_INPUT].lower() != 'never':
        res = transfer_single_archive_to_job(wcl, files2get, neededfiles,
                                             'target')

        if res is not None and res:
            problemfiles = {}
            for fkey, finfo in res.items():
                if 'err' in finfo:
                    problemfiles[fkey] = finfo
                    msg = "Warning: Error trying to get file %s from target archive%s: %s" % \
                          (fkey, arc, finfo['err'])
                    print msg

            files2get = list(set(files2get) - set(res.keys()))
            if problemfiles:
                print "Warning: had problems getting input files from target archive%s" % arc
                print "\t", problemfiles.keys()
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from target archive%s." % arc
            print "\ttransfer function returned no results"


    # home archive
    if files2get and pfwdefs.USE_HOME_ARCHIVE_INPUT in wcl and \
        wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == 'wrapper':
        res = transfer_single_archive_to_job(wcl, files2get, neededfiles,
                                             'home')

        if res is not None and res:
            problemfiles = {}
            for fkey, finfo in res.items():
                if 'err' in finfo:
                    problemfiles[fkey] = finfo
                    msg = "Warning: Error trying to get file %s from home archive%s: %s" % \
                          (fkey, arc, finfo['err'])
                    print msg

            files2get = list(set(files2get) - set(res.keys()))
            if problemfiles:
                print "Warning: had problems getting input files from home archive%s" % arc
                print "\t", problemfiles.keys()
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from home archive%s." % arc
            print "\ttransfer function returned no results"

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return files2get




######################################################################
def get_file_archive_info(wcl, files2get, jobfiles, archive_info):
    """ Get information about files in the archive after creating appropriate filemgmt object """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("archive_info = %s" % archive_info)


    # dynamically load class for archive file mgmt to find location of files in archive
    filemgmt = dynam_load_filemgmt(wcl, archive_info)

    fileinfo_archive = filemgmt.get_file_archive_info(files2get, archive_info['name'],
                                                      fmdefs.FM_PREFER_UNCOMPRESSED)

    if files2get and not fileinfo_archive:
        print "\tInfo: 0 files found on %s" % archive_info['name']
        print "\t\tfilemgmt = %s" % archive_info['filemgmt']

    transinfo = {}
    for name, info in fileinfo_archive.items():
        transinfo[name] = copy.deepcopy(info)
        transinfo[name]['src'] = info['rel_filename']
        transinfo[name]['dst'] = jobfiles[name]

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return transinfo


######################################################################
def get_wrapper_inputs(wcl, infiles):
    """ Transfer any inputs needed for this wrapper """

    missinginputs = {}
    existinginputs = {}

    # check which input files are already in job scratch directory
    #    (i.e., outputs from a previous execution)
    if not infiles:
        print "\tInfo: 0 inputs needed for wrapper"
        return

    for isect in infiles:
        exists, missing = intgmisc.check_files(infiles[isect])

        for efile in exists:
            existinginputs[miscutils.parse_fullname(efile, miscutils.CU_PARSE_FILENAME)] = efile

        for mfile in missing:
            missinginputs[miscutils.parse_fullname(mfile, miscutils.CU_PARSE_FILENAME)] = mfile

    if missinginputs:
        if miscutils.fwdebug_check(9, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("missing inputs: %s" % missinginputs)

        files2get = transfer_archives_to_job(wcl, missinginputs)

        # check if still missing input files
        if files2get:
            print '!' * 60
            for fname in files2get:
                msg = "Error: input file needed that was not retrieved from target or home archives\n(%s)" % fname
                print msg
            raise Exception("Error:  Cannot find all input files in an archive")

        # double-check: check that files are now on filesystem
        errcnt = 0
        for sect in infiles:
            _, missing = intgmisc.check_files(infiles[sect])

            if missing:
                for mfile in missing:
                    msg = "Error: input file doesn't exist despite transfer success (%s)" % mfile
                    print msg
                    errcnt += 1
        if errcnt > 0:
            raise Exception("Error:  Cannot find all input files after transfer.")
    else:
        print "\tInfo: all %s input file(s) already in job directory." % \
              len(existinginputs)



######################################################################
def get_exec_names(wcl):
    """ Return string containing comma separated list of executable names """

    execnamesarr = []
    exec_sectnames = intgmisc.get_exec_sections(wcl, pfwdefs.IW_EXECPREFIX)
    for sect in sorted(exec_sectnames):
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("section %s" % sect)
        if 'execname' not in wcl[sect]:
            print "Error: Missing execname in input wcl.  sect =", sect
            print "wcl[sect] = ", miscutils.pretty_print_dict(wcl[sect])
            miscutils.fwdie("Error: Missing execname in input wcl", pfwdefs.PF_EXIT_FAILURE)

        execnamesarr.append(wcl[sect]['execname'])

    return ','.join(execnamesarr)


######################################################################
def create_exec_tasks(wcl):
    """ Create exec tasks saving task_ids in wcl """

    wcl['task_id']['exec'] = OrderedDict()

    exec_sectnames = intgmisc.get_exec_sections(wcl, pfwdefs.IW_EXECPREFIX)
    for sect in sorted(exec_sectnames):
        # make sure execnum in the exec section in wcl for the insert_exec function
        if 'execnum' not in wcl[sect]:
            result = re.match(r'%s(\d+)' % pfwdefs.IW_EXECPREFIX, sect)
            if not result:
                miscutils.fwdie("Error:  Cannot determine execnum for input wcl sect %s" % \
                                sect, pfwdefs.PF_EXIT_FAILURE)
            wcl[sect]['execnum'] = result.group(1)

######################################################################
def get_wrapper_outputs(wcl, jobfiles):
    """ get output filenames for this wrapper """
    # pylint: disable=unused-argument

    # placeholder - needed for multiple exec sections
    return {}


######################################################################
def setup_working_dir(workdir, files, jobroot):
    """ create working directory for fw threads and symlinks to inputs """

    miscutils.coremakedirs(workdir)
    os.chdir(workdir)

    # create symbolic links for input files
    for isect in files:
        for ifile in files[isect]:
            # make subdir inside fw thread working dir so match structure of job scratch
            subdir = os.path.dirname(ifile)
            if subdir != "":
                miscutils.coremakedirs(subdir)

            os.symlink(os.path.join(jobroot, ifile), ifile)

    os.symlink("../inputwcl", "inputwcl")
    os.symlink("../log", "log")
    os.symlink("../outputwcl", "outputwcl")
    if os.path.exists("../list"):
        os.symlink("../list", "list")

######################################################################
def setup_wrapper(wcl, logfilename, workdir, ins):
    """ Create output directories, get files from archive, and other setup work """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    if workdir is not None:
        wcl['pre_disk_usage'] = 0
    else:
        wcl['pre_disk_usage'] = pfwutils.diskusage(wcl['jobroot'])


    # make directory for log file
    logdir = os.path.dirname(logfilename)
    miscutils.coremakedirs(logdir)

    # get execnames to put on command line for QC Framework
    wcl['execnames'] = wcl['wrapper']['wrappername'] + ',' + get_exec_names(wcl)


    # get input files from targetnode
    get_wrapper_inputs(wcl, ins)

    # if running in a fw thread, run in separate safe directory
    if workdir is not None:
        setup_working_dir(workdir, ins, os.getcwd())

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

######################################################################
def compose_path(dirpat, wcl, infdict):
    """ Create path by replacing variables in given directory pattern """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    dirpat2 = replfuncs.replace_vars(dirpat, wcl, {'searchobj': infdict,
                                                   'required': True,
                                                   intgdefs.REPLACE_VARS: True})
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return dirpat2

######################################################################
def output_transfer_prep(wcl, jobfiles, putinfo, task_label, exitcode):
    """ Compress files if necessary and make archive rel paths """

    mastersave = wcl.get(pfwdefs.MASTER_SAVE_FILE).lower()
    mastercompress = wcl.get(pfwdefs.MASTER_COMPRESSION)
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("%s: mastersave = %s" % (task_label, mastersave))
        miscutils.fwdebug_print("%s: mastercompress = %s" % (task_label, mastercompress))

    # make archive rel paths for transfer
    saveinfo = {}
    for key, fdict in putinfo.items():
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("putinfo[%s] = %s" % (key, fdict))
        should_save = pfwutils.should_save_file(mastersave, fdict['filesave'], exitcode)
        if should_save:
            if 'path' not in fdict:
                miscutils.fwdebug_print("Error: Missing path (archivepath) in file definition")
                print key, fdict
                sys.exit(1)
            should_compress = pfwutils.should_compress_file(mastercompress,
                                                            fdict['filecompress'],
                                                            exitcode)
            fdict['filecompress'] = should_compress
            fdict['dst'] = "%s/%s" % (fdict['path'], os.path.basename(fdict['src']))
            saveinfo[key] = fdict

    call_compress_files(wcl, jobfiles, saveinfo)
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("After compress saveinfo = %s" % (saveinfo))

    return saveinfo


######################################################################
def transfer_job_to_single_archive(wcl, saveinfo, dest, task_label):
    """ Handle the transfer of files from the job directory to a single archive """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("TRANSFER JOB TO ARCHIVE SECTION")
    archive_info = wcl['%s_archive_info' % dest.lower()]
    tstats = None
    if 'transfer_stats' in wcl:
        tstats = pfwutils.pfw_dynam_load_class(wcl, 'stats_' + task_label, wcl['transfer_stats'], None)

    # dynamically load class for job_file_mvmt
    if 'job_file_mvmt' not in wcl:
        msg = "Error:  Missing job_file_mvmt in job wcl"
        raise KeyError(msg)

    jobfilemvmt = None
    jobfilemvmt = dynam_load_jobfilemvmt(wcl, tstats)

    # tranfer files to archive
    if dest.lower() == 'target':
        res = jobfilemvmt.job2target(saveinfo)
    else:
        res = jobfilemvmt.job2home(saveinfo, wcl['verify_files'])

    arc = ""
    if 'home_archive' in wcl and 'archive' in wcl:
        ha = wcl['home_archive']
        if ha in wcl['archive'] and 'root_http' in wcl['archive'][ha]:
            arc = ' (' + wcl['archive'][wcl['home_archive']]['root_http'] + ')'

    # register files that we just copied into archive
    problemfiles = {}
    for fkey, finfo in res.items():
        if 'err' in finfo:
            problemfiles[fkey] = finfo
            msg = "Warning: Error trying to copy file %s to %s archive%s: %s" % \
                   (fkey, dest, arc, finfo['err'])
            print msg

    if problemfiles:
        print "ERROR\n\n\nError: putting %d files into archive %s" % \
              (len(problemfiles), archive_info['name'])
        print "\t", problemfiles.keys()
        raise Exception("Error: problems putting %d files into archive %s" %
                        (len(problemfiles), archive_info['name']))


######################################################################
def save_log_file(filemgmt, wcl, jobfiles, logfile):
    """ Register log file and prepare for copy to archive """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    putinfo = {}
    if logfile is not None and os.path.isfile(logfile):
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("log exists (%s)" % logfile)

        filepat = wcl['filename_pattern']['log']

        # Register log file
        try:
            pfw_save_file_info(filemgmt, 'log', [logfile],
                               False, None, filepat)
        except:
            (extype, exvalue, trback) = sys.exc_info()
            traceback.print_exception(extype, exvalue, trback, file=sys.stdout)

        # since able to register log file, save as not junk file
        jobfiles['outfullnames'].append(logfile)

        # prep for copy log to archive(s)
        filename = miscutils.parse_fullname(logfile, miscutils.CU_PARSE_FILENAME)
        putinfo[filename] = {'src': logfile,
                             'filename': filename,
                             'fullname': logfile,
                             'compression': None,
                             'path': wcl['log_archive_path'],
                             'filetype': 'log',
                             'filesave': True,
                             'filecompress': False}
    else:
        miscutils.fwdebug_print("Warning: log doesn't exist (%s)" % logfile)

    return putinfo


######################################################################
def copy_output_to_archive(wcl, jobfiles, fileinfo, level, task_label, exitcode):
    """ If requested, copy output file(s) to archive """
    # fileinfo[filename] = {filename, fullname, sectname}

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
    putinfo = {}


    # check each output file definition to see if should save file
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("Checking for save_file_archive")

    for (filename, fdict) in fileinfo.items():
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("filename %s, fdict=%s" % (filename, fdict))
        (filename, compression) = miscutils.parse_fullname(fdict['fullname'],
                                                           miscutils.CU_PARSE_FILENAME|miscutils.CU_PARSE_COMPRESSION)

        putinfo[filename] = {'src': fdict['fullname'],
                             'compression': compression,
                             'filename': filename,
                             'filetype': fdict['filetype'],
                             'filesave': fdict['filesave'],
                             'filecompress': fdict['filecompress'],
                             'path': fdict['path']}

    transfer_job_to_archives(wcl, jobfiles, putinfo, level, task_label, exitcode)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def get_pfw_hdrupd(wcl):
    """ Create the dictionary with PFW values to be written to fits file header """
    hdrupd = {}
    hdrupd['pipeline'] = "%s/DESDM pipeline name/str" %  wcl.get('wrapper.pipeline')
    hdrupd['reqnum'] = "%s/DESDM processing request number/int" % wcl.get('reqnum')
    hdrupd['unitname'] = "%s/DESDM processing unit name/str" % wcl.get('unitname')
    hdrupd['attnum'] = "%s/DESDM processing attempt number/int" % wcl.get('attnum')
    hdrupd['eupsprod'] = "%s/eups pipeline meta-package name/str" % wcl.get('wrapper.pipeprod')
    hdrupd['eupsver'] = "%s/eups pipeline meta-package version/str" % wcl.get('wrapper.pipever')
    return hdrupd

######################################################################
def cleanup_dir(dirname, removeRoot=False):
    """ Function to remove empty folders """

    if not os.path.isdir(dirname):
        return

    # remove empty subfolders
    files = os.listdir(dirname)
    if files > 0:
        for f in files:
            fullpath = os.path.join(dirname, f)
            if os.path.isdir(fullpath):
                cleanup_dir(fullpath, True)

    # if folder empty, delete it
    files = os.listdir(dirname)
    if files == 0 and removeRoot:
        try:
            os.rmdir(dirname)
        except:
            pass


######################################################################
def post_wrapper(wcl, ins, jobfiles, logfile, exitcode, workdir):
    """ Execute tasks after a wrapper is done """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
    #logfile = None
    # Save disk usage for wrapper execution
    disku = 0
    if workdir is not None:
        disku = pfwutils.diskusage(os.getcwd())

        # outputwcl and log are softlinks skipped by diskusage command
        # so add them individually
        if os.path.exists(wcl[pfwdefs.IW_WRAPSECT]['outputwcl']):
            disku += os.path.getsize(wcl[pfwdefs.IW_WRAPSECT]['outputwcl'])
        if os.path.exists(logfile):
            disku += os.path.getsize(logfile)
    else:
        disku = pfwutils.diskusage(wcl['jobroot'])
    wcl['wrap_usage'] = disku - wcl['pre_disk_usage']

    # don't save logfile name if none was actually written
    if not os.path.isfile(logfile):
        logfile = None

    outputwclfile = wcl[pfwdefs.IW_WRAPSECT]['outputwcl']
    if not os.path.exists(outputwclfile):
        outputwclfile = None

    filemgmt = dynam_load_filemgmt(wcl, None)

    finfo = {}

    excepts = []

    # always try to save log file
    logfinfo = save_log_file(filemgmt, wcl, jobfiles, logfile)
    if logfinfo is not None and logfinfo:
        finfo.update(logfinfo)

    outputwcl = WCL()
    if outputwclfile and os.path.exists(outputwclfile):
        with open(outputwclfile, 'r') as outwclfh:
            outputwcl.read(outwclfh, filename=outputwclfile)

        # add wcl file to list of non-junk output files
        jobfiles['outfullnames'].append(outputwclfile)

        # if running in a fw thread
        if workdir is not None:

            # undo symbolic links to input files
            for sect in ins:
                for fname in ins[sect]:
                    os.unlink(fname)

            #jobroot = os.getcwd()[:os.getcwd().find(workdir)]
            jobroot = wcl['jobroot']

            # move any output files from fw thread working dir to job scratch dir
            if outputwcl is not None and outputwcl and \
               pfwdefs.OW_OUTPUTS_BY_SECT in outputwcl and \
               outputwcl[pfwdefs.OW_OUTPUTS_BY_SECT]:
                for byexec in outputwcl[pfwdefs.OW_OUTPUTS_BY_SECT].values():
                    for elist in byexec.values():
                        files = miscutils.fwsplit(elist, ',')
                        for _file in files:
                            subdir = os.path.dirname(_file)
                            if subdir != "":
                                newdir = os.path.join(jobroot, subdir)
                                miscutils.coremakedirs(newdir)

                            # move file from fw thread working dir to job scratch dir
                            shutil.move(_file, os.path.join(jobroot, _file))

            # undo symbolic links to log and outputwcl dirs
            os.unlink('log')
            os.unlink('outputwcl')
            os.unlink('inputwcl')
            if os.path.exists('list'):
                os.unlink('list')

            os.chdir(jobroot)    # change back to job scratch directory from fw thread working dir
            cleanup_dir(workdir, True)

        # handle output files - file metadata, prov, copying to archive
        if outputwcl is not None and outputwcl:
            pfw_hdrupd = get_pfw_hdrupd(wcl)
            execs = intgmisc.get_exec_sections(outputwcl, pfwdefs.OW_EXECPREFIX)
            for sect in execs:
                print "DESDMTIME: app_exec %s %0.3f" % (sect, float(outputwcl[sect]['walltime']))

            if pfwdefs.OW_OUTPUTS_BY_SECT in outputwcl and \
               outputwcl[pfwdefs.OW_OUTPUTS_BY_SECT]:
                badfiles = []
                wrap_output_files = []
                for sectname, byexec in outputwcl[pfwdefs.OW_OUTPUTS_BY_SECT].items():
                    sectkeys = sectname.split('.')
                    sectdict = wcl.get('%s.%s' % (pfwdefs.IW_FILESECT, sectkeys[-1]))
                    filesave = miscutils.checkTrue(pfwdefs.SAVE_FILE_ARCHIVE, sectdict, True)
                    filecompress = miscutils.checkTrue(pfwdefs.COMPRESS_FILES, sectdict, False)

                    updatedef = {}
                    # get any hdrupd secton from inputwcl
                    for key, val in sectdict.items():
                        if key.startswith('hdrupd'):
                            updatedef[key] = val

                    # add pfw hdrupd values
                    updatedef['hdrupd_pfw'] = pfw_hdrupd
                    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                        miscutils.fwdebug_print("sectname %s, updatedef=%s" % \
                                                (sectname, updatedef))

                    for _, elist in byexec.items():
                        fullnames = miscutils.fwsplit(elist, ',')
                        wrap_output_files.extend(fullnames)
                        filepat = None
                        if 'filepat' in sectdict:
                            if sectdict['filepat'] in wcl['filename_pattern']:
                                filepat = wcl['filename_pattern'][sectdict['filepat']]
                            else:
                                raise KeyError('Missing file pattern (%s, %s, %s)' % (sectname,
                                                                                      sectdict['filetype'],
                                                                                      sectdict['filepat']))
                        try:
                            badfiles.extend(pfw_save_file_info(filemgmt, sectdict['filetype'],
                                                               fullnames, True, updatedef, filepat))
                        except Exception, e:
                            miscutils.fwdebug_print('An error occurred')
                            (extype, exvalue, trback) = sys.exc_info()
                            traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
                            excepts.append(e)
                        for fname in fullnames:
                            if fname in badfiles:
                                continue
                            finfo[fname] = {'sectname': sectname,
                                            'filetype': sectdict['filetype'],
                                            'filesave': filesave,
                                            'filecompress': filecompress,
                                            'fullname': fname}
                            if 'archivepath' in sectdict:
                                finfo[fname]['path'] = sectdict['archivepath']

                wrap_output_files = list(set(wrap_output_files))
                if badfiles:
                    miscutils.fwdebug_print("An error occured during metadata ingestion the following file(s) had issues: %s" % \
', '.join(badfiles))
                    (extype, exvalue, trback) = sys.exc_info()
                    traceback.print_exception(extype, exvalue, trback, file=sys.stdout)

                    excepts.append(Exception("An error occured during metadata ingestion the following file(s) had issues: %s" % ', '.join(badfiles)))
                    for f in badfiles:
                        if f in wrap_output_files:
                            wrap_output_files.remove(f)

                jobfiles['outfullnames'].extend(wrap_output_files)
                # update input files
                for isect in ins:
                    for ifile in ins[isect]:
                        jobfiles['infullnames'].append(ifile)

    if finfo:
        save_trans_end_of_job(wcl, jobfiles, finfo)
        copy_output_to_archive(wcl, jobfiles, finfo, 'wrapper', 'wrapper_output', exitcode)

    # clean up any input files no longer needed - TODO

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    if excepts:
        raise Exception('An exception was raised. See tracebacks further up the output for information.')
# end postwrapper


######################################################################
def parse_wrapper_line(line, linecnt):
    #pylint: disable=unbalanced-tuple-unpacking
    """ Parse a line from the job's wrapper list """
    wrapinfo = {}
    lineparts = miscutils.fwsplit(line.strip())
    if len(lineparts) == 5:
        (wrapinfo['wrapnum'], wrapinfo['wrapname'], wrapinfo['wclfile'], wrapinfo['wrapdebug'], wrapinfo['logfile']) = lineparts
    elif len(lineparts) == 4:
        (wrapinfo['wrapnum'], wrapinfo['wrapname'], wrapinfo['wclfile'], wrapinfo['logfile']) = lineparts
        wrapinfo['wrapdebug'] = 0  # default wrapdebug
    else:
        print "Error: incorrect number of items in line #%s" % linecnt
        print "       Check that modnamepat matches wrapperloop"
        print "\tline: %s" % line
        raise SyntaxError("Error: incorrect number of items in line #%s" % linecnt)
    #wrapinfo['logfile'] = None
    return wrapinfo


######################################################################
def gather_initial_fullnames():
    """ save fullnames for files initially in job scratch directory
        so won't appear in junk tarball """

    infullnames = []
    for (dirpath, _, filenames) in os.walk('.'):
        dpath = dirpath[2:]
        if dpath:
            dpath += '/'
        for fname in filenames:
            infullnames.append('%s%s' % (dpath, fname))

    if miscutils.fwdebug_check(6, 'PFWRUNJOB_DEBUG'):
        miscutils.fwdebug_print("initial infullnames=%s" % infullnames)
    return infullnames

######################################################################
def exechost_status():
    """ Print various information about exec host """

    exechost = socket.gethostname()

    # free
    try:
        subp = subprocess.Popen(["free", "-m"], stdout=subprocess.PIPE)
        output = subp.communicate()[0]
        print "EXECSTAT %s FREE\n%s" % (exechost, output)
    except:
        print "Problem running free command"
        (extype, exvalue, trback) = sys.exc_info()
        traceback.print_exception(extype, exvalue, trback, limit=1, file=sys.stdout)
        print "Ignoring error and continuing...\n"

    # df
    try:
        cwd = os.getcwd()
        subp = subprocess.Popen(["df", "-h", cwd], stdout=subprocess.PIPE)
        output = subp.communicate()[0]
        print "EXECSTAT %s DF\n%s" % (exechost, output)
    except:
        print "Problem running df command"
        (extype, exvalue, trback) = sys.exc_info()
        traceback.print_exception(extype, exvalue, trback, limit=1, file=sys.stdout)
        print "Ignoring error and continuing...\n"

######################################################################
def job_thread(argv):
    #pylint: disable=lost-exception
    """ run a task in a thread """

    try:
        exitcode = pfwdefs.PF_EXIT_FAILURE
        pid = os.getpid()
        stdp = None
        stde = None
        stdporig = None
        stdeorig = None
        wcl = WCL()
        wcl['wrap_usage'] = 0.0
        jobfiles = {}
        task = {'wrapnum':'-1'}
        try:
            # break up the input data
            (task, jobfiles, jwcl, ins, outq, errq, multi) = argv
            stdp = WrapOutput(task['wrapnum'], outq)
            stdporig = sys.stdout
            sys.stdout = stdp
            stde = WrapOutput(task['wrapnum'], errq)
            stdeorig = sys.stderr
            sys.stderr = stde

            # print machine status information
            exechost_status()

            wrappercmd = "%s %s" % (task['wrapname'], task['wclfile'])

            if not os.path.exists(task['wclfile']):
                print "Error: input wcl file does not exist (%s)" % task['wclfile']
                return (1, jobfiles, jwcl, 0, task['wrapnum'], pid)

            with open(task['wclfile'], 'r') as wclfh:
                wcl.read(wclfh, filename=task['wclfile'])
            wcl.update(jwcl)

            sys.stdout.flush()

            # set up the working directory if needed
            if multi:
                workdir = "fwtemp%04i" % (int(task['wrapnum']))
            else:
                workdir = None
            setup_wrapper(wcl, task['logfile'], workdir, ins)

            print "Running wrapper: %s" % (wrappercmd)
            sys.stdout.flush()
            starttime = time.time()
            try:
                exitcode = pfwutils.run_cmd_qcf(wrappercmd, task['logfile'],
                                                wcl['execnames'])
            except:
                (extype, exvalue, trback) = sys.exc_info()
                print '!' * 60
                print "%s: %s" % (extype, str(exvalue))

                traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
                exitcode = pfwdefs.PF_EXIT_FAILURE
            sys.stdout.flush()
            if exitcode != pfwdefs.PF_EXIT_SUCCESS:
                print "Error: wrapper %s exited with non-zero exit code %s.   Check log:" % \
                    (wcl[pfwdefs.PF_WRAPNUM], exitcode),
                logfilename = miscutils.parse_fullname(wcl['log'], miscutils.CU_PARSE_FILENAME)
                print " %s/%s" % (wcl['log_archive_path'], logfilename)
            print "DESDMTIME: run_wrapper %0.3f" % (time.time()-starttime)

            print "Post-steps (exit: %s)" % (exitcode)
            post_wrapper(wcl, ins, jobfiles, task['logfile'], exitcode, workdir)

            if exitcode:
                miscutils.fwdebug_print("Aborting due to non-zero exit code")
        except:
            print traceback.format_exc()
            exitcode = pfwdefs.PF_EXIT_FAILURE
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=4, file=sys.stdout)

        finally:
            if stdp is not None:
                sys.stdout = stdporig
            if stde is not None:
                sys.stderr = stdeorig
            sys.stdout.flush()
            sys.stderr.flush()

            return (exitcode, jobfiles, wcl, wcl['wrap_usage'], task['wrapnum'], pid)
    except:
        print "Error: Unhandled exception in job_thread."
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=4, file=sys.stdout)
        return (1, None, None, 0.0, '-1', pid)

######################################################################
def terminate(save=[], force=False):
    """ docstring """
    #pylint: disable=protected-access
    global main_lock
    # use a lock to make sure there is never more than 1 running at a time
    with main_lock:
        global pool
        global keeprunning
        global terminating
        terminating = True
        try:
            pool._taskqueue = Queue.Queue()
            pool._state = pl.TERMINATE

            pool._worker_handler._state = pl.TERMINATE
            pool._terminate.cancel()
            parent = psutil.Process(os.getpid())

            children = parent.children(recursive=False)

            grandchildren = []
            for child in children:
                grandchildren += child.children(recursive=True)
            for proc in grandchildren:
                try:
                    proc.send_signal(signal.SIGTERM)
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_exception(exc_type, exc_value, exc_traceback,
                                              limit=4, file=sys.stdout)
            # if we need to make sure all child processes are stopped
            if force:
                for proc in children:
                    if proc.pid in save:
                        continue
                    try:
                        proc.send_signal(signal.SIGTERM)
                    except:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                                  limit=4, file=sys.stdout)

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=4, file=sys.stdout)
        keeprunning = False

######################################################################
def results_checker(result):
    """ method to collec the results  """
    global pool
    global stop_all
    global results
    global jobfiles_global
    global jobwcl
    global job_track
    global result_lock
    global lock_monitor
    global donejobs
    global keeprunning
    global terminating
    try:
        (res, jobf, wcl, usage, wrapnum, pid) = result
        jobfiles_global['outfullnames'].extend(jobf['outfullnames'])
        jobfiles_global['output_putinfo'].update(jobf['output_putinfo'])
        if not terminating:
            del job_track[wrapnum]
        if usage > jobwcl['job_max_usage']:
            jobwcl['job_max_usage'] = usage
        results.append(res)
        # if the current thread exited with non-zero status, then kill remaining threads
        #  but keep the log files

        if (res != 0 and stop_all) and not terminating:
            if result_lock.acquire(False):
                keeprunning = False
                try:
                    # manually end the child processes as pool.terminate can deadlock
                    # if multiple threads return with errors
                    terminate(save=[pid], force=True)
                    for _, (logfile, jobfiles) in job_track.iteritems():
                        filemgmt = dynam_load_filemgmt(wcl, None)

                        if logfile is not None and os.path.isfile(logfile):
                            # only update the log if it has not been ingested already
                            if not filemgmt.has_metadata_ingested('log', logfile):
                                lfile = open(logfile, 'a')
                                lfile.write("\n****************\nWrapper terminated early due to error in parallel thread.\n****************")
                                lfile.close()
                            logfileinfo = save_log_file(filemgmt, wcl, jobfiles, logfile)
                            jobfiles_global['outfullnames'].append(logfile)
                            jobfiles_global['output_putinfo'].update(logfileinfo)
                    time.sleep(10)
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_exception(exc_type, exc_value, exc_traceback,
                                              limit=4, file=sys.stdout)
                finally:
                    keeprunning = False
            else:
                result_lock.acquire()

    except:
        keeprunning = False
        print "Error: thread monitoring encountered an unhandled exception."
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=4, file=sys.stdout)
        results.append(1)
    finally:
        if not result_lock.acquire(False):
            result_lock.release()
            lock_monitor.acquire()
            lock_monitor.notify_all()
            lock_monitor.release()
        else:
            result_lock.release()

        donejobs += 1

######################################################################
def job_workflow(workflow, jobfiles, jwcl=WCL()):
    #pylint: disable=protected-access,expression-not-assigned,lost-exception
    """ Run each wrapper execution sequentially """
    global pool
    global results
    global stop_all
    global jobfiles_global
    global job_track
    global keeprunning
    global donejobs
    global result_lock
    global lock_monitor

    infullnames = {}
    with open(workflow, 'r') as workflowfh:
        # for each wrapper execution
        lines = workflowfh.readlines()
        sys.stdout.flush()
        inputs = {}
        # read in all of the lines in dictionaries
        for linecnt, line in enumerate(lines):
            wrapnum = miscutils.fwsplit(line.strip())[0]
            task = parse_wrapper_line(line, linecnt)
            #task['logfile'] = None
            wcl = WCL()
            with open(task['wclfile'], 'r') as wclfh:
                wcl.read(wclfh, filename=task['wclfile'])
                wcl.update(jwcl)

            # get fullnames for inputs and outputs
            ins, _ = intgmisc.get_fullnames(wcl, wcl, None)
            del wcl
            # save input filenames to eliminate from junk tarball later
            infullnames[wrapnum] = []
            for isect in ins:
                for ifile in ins[isect]:
                    infullnames[wrapnum].append(ifile)
                    jobfiles['infullnames'].extend(ifile)
            inputs[wrapnum] = (task, copy.deepcopy(jobfiles), jwcl, ins)
            job_track[task['wrapnum']] = (task['logfile'], jobfiles)
        # get all of the task groupings, they will be run in numerical order
        tasks = jwcl["fw_groups"].keys()
        tasks.sort()
        # loop over each grouping
        manager = mp.Manager()
        for task in tasks:
            results = []   # the results of running each task in the group
            # get the maximum number of parallel processes to run at a time
            nproc = int(jwcl["fw_groups"][task]["fw_nthread"])
            procs = miscutils.fwsplit(jwcl["fw_groups"][task]["wrapnums"])
            tempproc = []
            # pare down the list to include only those in this run
            for p in procs:
                if p in inputs.keys():
                    tempproc.append(p)
            procs = tempproc
            if nproc > 1:
                numjobs = len(procs)
                # set up the thread pool
                pool = mp.Pool(processes=nproc, maxtasksperchild=2)
                outq = manager.Queue()
                errq = manager.Queue()
                with lock_monitor:
                    try:
                        donejobs = 0
                        # update the input files now, so that it only contains those from the current taks(s)
                        for inp in procs:
                            jobfiles_global['infullnames'].extend(infullnames[inp])
                        # attach all the grouped tasks to the pool
                        [pool.apply_async(job_thread, args=(inputs[inp] + (outq, errq, True,),), callback=results_checker) for inp in procs]
                        pool.close()
                        time.sleep(10)
                        while donejobs < numjobs and keeprunning:
                            count = 0
                            while count < 2:
                                count = 0
                                try:
                                    msg = outq.get_nowait()
                                    print msg
                                except:
                                    count += 1
                                try:
                                    errm = errq.get_nowait()
                                    sys.stderr.write(errm)
                                except:
                                    count += 1
                            time.sleep(.1)
                    except:
                        results.append(1)
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                                  limit=4, file=sys.stdout)

                        raise

                    finally:
                        if stop_all and max(results) > 0:
                            # wait to give everything time to do the first round of cleanup
                            time.sleep(20)
                            # get any waiting messages
                            for _ in range(1000):
                                try:
                                    msg = outq.get_nowait()
                                    print msg
                                except:
                                    break
                            for _ in range(1000):
                                try:
                                    errm = errq.get_nowait()
                                    sys.stderr.write(errm)
                                except:
                                    break
                            if not result_lock.acquire(False):
                                lock_monitor.wait(60)
                            else:
                                result_lock.release()
                            # empty the worker queue so nothing else starts
                            terminate(force=True)
                            # wait so everything can clean up, otherwise risk a deadlock
                            time.sleep(50)
                        del pool
                        while True:
                            try:
                                msg = outq.get(timeout=.1)
                                print msg
                            except:
                                break

                        while True:
                            try:
                                errm = errq.get(timeout=.1)
                                sys.stderr.write(errm)
                            except:
                                break
                        # in case the sci code crashed badly
                        if not results:
                            results.append(1)
                        jobfiles = jobfiles_global
                        jobfiles['infullnames'] = list(set(jobfiles['infullnames']))
                        if stop_all and max(results) > 0:
                            return max(results), jobfiles
            # if running in single threaded mode
            else:
                temp_stopall = stop_all
                stop_all = False

                donejobs = 0
                for inp in procs:
                    try:
                        jobfiles_global['infullnames'].extend(infullnames[inp])
                        results_checker(job_thread(inputs[inp] + (sys.stdout, sys.stderr, False,)))
                    except:
                        (extype, exvalue, trback) = sys.exc_info()
                        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
                        results = [1]
                    jobfiles = jobfiles_global
                    if results[-1] != 0:
                        return results[-1], jobfiles
                stop_all = temp_stopall


    return 0, jobfiles

def run_job(args):
    """Run tasks inside single job"""

    global stop_all
    global jobfiles_global
    global jobwcl

    jobwcl = WCL()
    jobfiles = {'infullnames': [args.config, args.workflow],
                'outfullnames': [],
                'output_putinfo': {}}
    jobfiles_global = {'infullnames': [args.config, args.workflow],
                       'outfullnames': [],
                       'output_putinfo': {}}

    jobstart = time.time()
    with open(args.config, 'r') as wclfh:
        jobwcl.read(wclfh, filename=args.config)
    jobwcl['verify_files'] = miscutils.checkTrue('verify_files', jobwcl, False)
    jobwcl['jobroot'] = os.getcwd()
    jobwcl['job_max_usage'] = 0
    #jobwcl['pre_job_disk_usage'] = pfwutils.diskusage(jobwcl['jobroot'])
    jobwcl['pre_job_disk_usage'] = 0

    # Save pointers to archive information for quick lookup
    if jobwcl[pfwdefs.USE_HOME_ARCHIVE_INPUT] != 'never' or \
       jobwcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT] != 'never':
        jobwcl['home_archive_info'] = jobwcl[pfwdefs.SW_ARCHIVESECT][jobwcl[pfwdefs.HOME_ARCHIVE]]
    else:
        jobwcl['home_archive_info'] = None

    if jobwcl[pfwdefs.USE_TARGET_ARCHIVE_INPUT] != 'never' or \
            jobwcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT] != 'never':
        jobwcl['target_archive_info'] = jobwcl[pfwdefs.SW_ARCHIVESECT][jobwcl[pfwdefs.TARGET_ARCHIVE]]
    else:
        jobwcl['target_archive_info'] = None

    # run the tasks (i.e., each wrapper execution)
    stop_all = miscutils.checkTrue('stop_on_fail', jobwcl, True)

    try:
        jobfiles['infullnames'] = gather_initial_fullnames()
        jobfiles_global['infullnames'].extend(jobfiles['infullnames'])
        miscutils.coremakedirs('log')
        miscutils.coremakedirs('outputwcl')
        exitcode, jobfiles = job_workflow(args.workflow, jobfiles, jobwcl)
    except Exception:
        (extype, exvalue, trback) = sys.exc_info()
        print '!' * 60
        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
        exitcode = pfwdefs.PF_EXIT_FAILURE
        print "Aborting rest of wrapper executions.  Continuing to end-of-job tasks\n\n"

    try:
        create_junk_tarball(jobwcl, jobfiles, exitcode)
    except:
        print "Error creating junk tarball"
    # if should transfer at end of job
    if jobfiles['output_putinfo']:
        print "\n\nCalling file transfer for end of job (%s files)" % \
              (len(jobfiles['output_putinfo']))

        copy_output_to_archive(jobwcl, jobfiles, jobfiles['output_putinfo'], 'job',
                               'job_output', exitcode)
    else:
        print "\n\n0 files to transfer for end of job"
        if miscutils.fwdebug_check(1, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("len(jobfiles['outfullnames'])=%s" % \
                                    (len(jobfiles['outfullnames'])))
    print "\nDESDMTIME: pfwrun_job %0.3f" % (time.time()-jobstart)
    return exitcode

###############################################################################
def create_compression_wdf(wgb_fnames):
    """ Create the was derived from provenance for the compression """
    # assumes filename is the same except the compression extension
    wdf = {}
    cnt = 1
    for child in wgb_fnames:
        parent = os.path.splitext(child)[0]
        wdf['derived_%s' % cnt] = {provdefs.PROV_PARENTS: parent, provdefs.PROV_CHILDREN: child}
        cnt += 1

    return wdf


###############################################################################
def call_compress_files(jwcl, jobfiles, putinfo):
    """ Compress output files as specified """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    # determine which files need to be compressed
    to_compress = []
    for fname, fdict in putinfo.items():
        if fdict['filecompress']:
            to_compress.append(fdict['src'])

    if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("to_compress = %s" % to_compress)

    if to_compress:
        miscutils.fwdebug_print("0 files to compress")
    else:
        errcnt = 0
        (res, _, _) = pfwcompress.compress_files(to_compress,
                                                 jwcl[pfwdefs.COMPRESSION_SUFFIX],
                                                 jwcl[pfwdefs.COMPRESSION_EXEC],
                                                 jwcl[pfwdefs.COMPRESSION_ARGS],
                                                 3, jwcl[pfwdefs.COMPRESSION_CLEANUP])

        filelist = []
        wgb_fnames = []
        for fname, fdict in res.items():
            if miscutils.fwdebug_check(3, 'PFWRUNJOB_DEBUG'):
                miscutils.fwdebug_print("%s = %s" % (fname, fdict))

            if fdict['err'] is None:
                # add new filename to jobfiles['outfullnames'] so not junk
                jobfiles['outfullnames'].append(fdict['outname'])

                # update jobfiles['output_putinfo'] for transfer
                (filename, compression) = miscutils.parse_fullname(fdict['outname'],
                                                                   miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
                if filename in putinfo:
                    # info for desfile entry
                    dinfo = diskutils.get_single_file_disk_info(fdict['outname'],
                                                                save_md5sum=True,
                                                                archive_root=None)
                    # compressed file should be one saved to archive
                    putinfo[filename]['src'] = fdict['outname']
                    putinfo[filename]['compression'] = compression
                    putinfo[filename]['dst'] += compression

                    del dinfo['path']
                    wgb_fnames.append(filename + compression)
                    dinfo['filetype'] = putinfo[filename]['filetype']
                    filelist.append(dinfo)

                else:
                    miscutils.fwdie("Error: compression mismatch %s" % filename,
                                    pfwdefs.PF_EXIT_FAILURE)
            else:  # errstr
                miscutils.fwdebug_print("WARN: problem compressing file - %s" % fdict['err'])
                errcnt += 1

        # register compressed file with file manager, save used provenance info
        filemgmt = dynam_load_filemgmt(jwcl, None)
        for finfo in filelist:
            filemgmt.save_desfile(finfo)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END")

################################################################################
def create_junk_tarball(wcl, jobfiles, exitcode):
    """ Create the junk tarball """

    if not pfwdefs.CREATE_JUNK_TARBALL in wcl or \
       not miscutils.convertBool(wcl[pfwdefs.CREATE_JUNK_TARBALL]):
        return

    # input files are what files where staged by framework (i.e., input wcl)
    # output files are only those listed as outputs in outout wcl

    miscutils.fwdebug_print("BEG")
    if miscutils.fwdebug_check(1, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("# infullnames = %s" % len(jobfiles['infullnames']))
        miscutils.fwdebug_print("# outfullnames = %s" % len(jobfiles['outfullnames']))
    if miscutils.fwdebug_check(11, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("infullnames = %s" % jobfiles['infullnames'])
        miscutils.fwdebug_print("outfullnames = %s" % jobfiles['outfullnames'])

    junklist = []

    # remove paths
    notjunk = {}
    for fname in jobfiles['infullnames']:
        notjunk[os.path.basename(fname)] = True
    for fname in jobfiles['outfullnames']:
        notjunk[os.path.basename(fname)] = True

    if miscutils.fwdebug_check(11, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("notjunk = %s" % notjunk.keys())
    # walk job directory to get all files
    miscutils.fwdebug_print("Looking for files at add to junk tar")
    cwd = '.'
    for (dirpath, _, filenames) in os.walk(cwd):
        for walkname in filenames:
            if miscutils.fwdebug_check(13, "PFWRUNJOB_DEBUG"):
                miscutils.fwdebug_print("walkname = %s" % walkname)
            if walkname not in notjunk:
                if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
                    miscutils.fwdebug_print("Appending walkname to list = %s" % walkname)

                if dirpath.startswith('./'):
                    dirpath = dirpath[2:]
                elif dirpath == '.':
                    dirpath = ''
                if dirpath:
                    fname = "%s/%s" % (dirpath, walkname)
                else:
                    fname = walkname

                if not os.path.islink(fname):
                    junklist.append(fname)

    if miscutils.fwdebug_check(1, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("# in junklist = %s" % len(junklist))
    if miscutils.fwdebug_check(11, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("junklist = %s" % junklist)

    putinfo = {}
    if junklist:
        pfwutils.tar_list(wcl['junktar'], junklist)

        # register junktar with file manager
        filemgmt = dynam_load_filemgmt(wcl, None)
        try:
            pfw_save_file_info(filemgmt, 'junk_tar', [wcl['junktar']],
                               False, None, wcl['filename_pattern']['junktar'])
        except:
            (extype, exvalue, trback) = sys.exc_info()
            traceback.print_exception(extype, exvalue, trback, file=sys.stdout)

        parsemask = miscutils.CU_PARSE_FILENAME|miscutils.CU_PARSE_COMPRESSION
        (filename, compression) = miscutils.parse_fullname(wcl['junktar'], parsemask)

        # gather "disk" metadata about tarball
        putinfo = {wcl['junktar']: {'src': wcl['junktar'],
                                    'filename': filename,
                                    'fullname': wcl['junktar'],
                                    'compression': compression,
                                    'path': wcl['junktar_archive_path'],
                                    'filetype': 'junk_tar',
                                    'filesave': True,
                                    'filecompress': False}}

        # if save setting is wrapper, save junktar here, otherwise save at end of job
        save_trans_end_of_job(wcl, jobfiles, putinfo)
        transfer_job_to_archives(wcl, jobfiles, putinfo, 'wrapper',
                                 'junktar', exitcode)



    if putinfo:
        jobfiles['output_putinfo'].update(putinfo)
        miscutils.fwdebug_print("Junk tar created")
    else:
        miscutils.fwdebug_print("No files found for junk tar. Junk tar not created.")
    miscutils.fwdebug_print("END\n\n")

######################################################################
def parse_args(argv):
    """ Parse the command line arguments """
    parser = argparse.ArgumentParser(description='pfwrun_job.py')
    parser.add_argument('--version', action='store_true', default=False)
    parser.add_argument('--config', action='store', required=True)
    parser.add_argument('workflow', action='store')

    args = parser.parse_args(argv)

    if args.version:
        print __version__
        sys.exit(0)

    return args

if __name__ == '__main__':
    os.environ['PYTHONUNBUFFERED'] = 'true'
    print "Cmdline given: %s" % ' '.join(sys.argv)
    sys.exit(run_job(parse_args(sys.argv[1:])))
