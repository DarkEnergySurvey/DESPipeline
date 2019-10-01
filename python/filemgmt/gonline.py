"""
    .. _filemgmt-gonline:

    **gonline**
    -----------

    Module for transferring files via globus online, uses a deprecated python
    module
"""

from  datetime import datetime, timedelta
import sys
import copy
import time

import globusonline.transfer.api_client
from globusonline.transfer.api_client import Transfer, x509_proxy

import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs



class DESGlobusOnline(object):
    """ Class for transferring files via globus

        Parameters
        ----------
        srcinfo : dict
            Dictionary containing the info of the source endpoint

        dstinfo : dict
            Dictionary containing the info of the destination endpoint

        credfile : str
            The file containing the user credentials

        gouser : str
            The name of the user

        proxy_valid_hrs : int
            How long in hours to make the proxy valid for
    """
    def __init__(self, srcinfo, dstinfo, credfile, gouser, proxy_valid_hrs=6):
        self.lsdesc = ['name', 'type', 'permissions', 'size', 'user', 'group', 'last_modified']

        self.srcinfo = srcinfo
        self.dstinfo = dstinfo
        clientargs = ['-c', credfile, gouser]
        self.submission_id = None
        try:
            self.goclient, _ = globusonline.transfer.api_client.create_client_from_args(clientargs)
        except:
            (_type, value, _) = sys.exc_info()
            print "Error when trying to create globusonline client"
            print "\t%s: %s" % (_type, value)
            print "\tTypically means problem with operator proxy.  Check that there is a valid proxy using grid-proxy-info"
            sys.exit(fmdefs.FM_EXIT_FAILURE)

        self.proxy_valid_hrs = proxy_valid_hrs
        self.credfile = credfile


    def endpoint_activate(self, endpoint):
        """ Activate the endpoint

            Parameters
            ----------
            endpoint : str
                The endpoint to activate
        """
        _, _, reqs = self.goclient.endpoint_activation_requirements(endpoint, type="delegate_proxy")
        public_key = reqs.get_requirement_value("delegate_proxy", "public_key")
        proxy = x509_proxy.create_proxy_from_file(self.credfile, public_key, self.proxy_valid_hrs)
        reqs.set_requirement_value("delegate_proxy", "proxy_chain", proxy)
        result = self.goclient.endpoint_activate(endpoint, reqs)
        return result

    def makedirs(self, filelist, endpoint):
        """ get list of dirs to make

            Parameters
            ----------
            filelist : list
                The list of files

            endpoint : str
                The endpoint to use
        """
        print "makedirs: filelist=", filelist
        dirlist = miscutils.get_list_directories(filelist)
        print "makedirs: dirlist=", dirlist
        for path in sorted(dirlist): # should already be sorted, but just in case
            miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', 'endpoint=%s, path=%s' % (endpoint, path))
            try:
                _ = self.goclient.endpoint_mkdir(endpoint, path)
            except Exception as e:
                if 'already exists' not in str(e):
                    raise
                else:
                    miscutils.fwdebug(2, 'GLOBUS_ONLINE_DEBUG', 'already exists endpoint=%s, path=%s' % (endpoint, path))


    def start_transfer(self, filelist):
        """ activate src endpoint

            Parameters
            ----------
            filelist : list
                List of files to transfer

            Returns
            -------
            str of the globus task id
        """
        src_endpoint = self.srcinfo['endpoint']
        result = self.endpoint_activate(src_endpoint)

        # activate dst endpoint
        dst_endpoint = self.dstinfo['endpoint']
        result = self.endpoint_activate(dst_endpoint)

        # create dst directories
        self.makedirs([finfo['dst'] for finfo in filelist.values()], dst_endpoint)

        ##    Get a submission id:
        _, _, result = self.goclient.transfer_submission_id()
        self.submission_id = result["value"]
        miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubmission id = %s" % self.submission_id)

        ##    Create a transfer object:
        #t = Transfer(submission_id, src_endpoint, dst_endpoint, notify_on_succeeded = False,
        #  notify_on_failed = False, notify_on_inactive= False, deadline='2m')
        deadline = datetime.utcnow() + timedelta(minutes=30)
        t = Transfer(self.submission_id, src_endpoint, dst_endpoint, notify_on_succeeded=False,
                     notify_on_failed=False, notify_on_inactive=False, deadline=deadline)
        #print t.as_data()

        # add files to transfer
        for _, finfo in filelist.items():
            sfile = finfo['src']
            dfile = finfo['dst']
            miscutils.fwdebug(2, 'GLOBUS_ONLINE_DEBUG', "\tadding to transfer %s = %s" % (sfile, dfile))
            if sfile.endswith('/'):
                t.add_item(sfile, dfile, recursive=True)  # error if true for file
            else:
                t.add_item(sfile, dfile)

        # start transfer
        _, _, result = self.goclient.transfer(t)
        task_id = result["task_id"]
        miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\ttask id = %s" % task_id)

        return task_id


    # blocking transfer
    def blocking_transfer(self, filelist):
        """ Do a blocking transfer

            Parameters
            ----------
            filelist : list
                The files to transfer

            Returns
            -------
            dict of the transfer results
        """
        task_id = self.start_transfer(filelist)
        miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\ttask_id = %s" % task_id)

        # wait for transfer to complete
        ##    Check the progress of the new transfer:
        MAX_NUM_CHKS = 600
        MAX_NUM_RETRY = 5
        CHK_INTERVAL_SECS = 30

        status = "ACTIVE"
        chk_cnt = 0
        retry_cnt = 0
        errstrs = {}
        while status == "ACTIVE" and chk_cnt < MAX_NUM_CHKS and retry_cnt < MAX_NUM_RETRY:
            miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "Checking transfer task status")
            status, reason, result = self.goclient.task(task_id)
            status = result["status"]
            miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tstatus = %s" % result["status"])
            miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tfiles = %s" % result["files"])
            miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_total = %s" % result["subtasks_total"])
            miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_failed = %s" % result["subtasks_failed"])
            miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_retrying = %s" % result["subtasks_retrying"])
            miscutils.fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tnice_status_details = %s" % result["nice_status_details"])

            if status == "ACTIVE":
                chk_cnt += 1

                # cannot call task_successful_transfers on task that is still active
                if result["nice_status_details"] is not None and result["nice_status_details"].startswith("Error"):
                    # only print error message once
                    if result["nice_status_details"] not in errstrs:
                        print result["nice_status_details"]
                        errstrs[result["nice_status_details"]] = True

                    if result['subtasks_retrying'] != 0:
                        retry_cnt += 1
                    else:
                        miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tstatus = %s" % result["status"])
                        miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tfiles = %s" % result["files"])
                        miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_total = %s" % result["subtasks_total"])
                        miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_failed = %s" % result["subtasks_failed"])
                        miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_retrying = %s" % result["subtasks_retrying"])
                        miscutils.fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tnice_status_details = %s" % result["nice_status_details"])
                        miscutils.fwdie("Error while transfering files", fmdefs.FM_EXIT_FAILURE)

                if chk_cnt < MAX_NUM_CHKS and retry_cnt < MAX_NUM_RETRY:
                    time.sleep(CHK_INTERVAL_SECS)


        self.goclient.task_cancel(task_id)

        status, reason, successes = self.goclient.task_successful_transfers(task_id)
        print status
        print reason
        print "----------\n\n\n"
        print "subtask_list=", result
        print "\n\n\n"

        transresults = copy.deepcopy(filelist)
        if len(successes['DATA']) != len(filelist):
            for fname, _ in transresults.items():
                transresults[fname]['err'] = 'problems transferring file'
        return transresults


    def transfer_directory(self, srcpath, dstpath):
        """ transfer a directory

            Parameters
            ----------
            srcpath : str
                The path to the source directory

            dstpath : str
                The path of the destination directory

            Returns
            -------
            dict of the transfer results
        """
        # activate src endpoint
        src_endpoint = self.srcinfo['endpoint']
        _ = self.endpoint_activate(src_endpoint)

        # activate dst endpoint
        dst_endpoint = self.dstinfo['endpoint']
        _ = self.endpoint_activate(dst_endpoint)

        if srcpath[-1] != '/':
            srcpath += '/'

        # start transfer
        transresults = self.blocking_transfer({srcpath: {'src': srcpath, 'dst':dstpath}})
        return transresults


    def get_directory_listing(self, path, endpoint, recursive=False):
        """ get directory listing

            Parameters
            ----------
            path : str
                The path the look at

            endpoint : str
                The endpoint to use

            recursive : bool
                Whether or not to do a recursive listing, default is False

            Returns
            -------
            dict of the results
        """
        # endpoint_ls currently only does ls for directories not single files

        # get directory listing from endpoint
        diskinfo = {}
        _ = self.endpoint_activate(endpoint)
        _, _, data = self.goclient.endpoint_ls(endpoint, path)
        for f in data["DATA"]:
            lsdict = dict(zip(self.lsdesc, [unicode(f[k]) for k in self.lsdesc]))
            #print self.lsdesc
            lsdict['path'] = path
            print lsdict
            diskinfo["%s/%s" % (path, lsdict['name'])] = lsdict
            if recursive and lsdict['type'] == 'dir':
                diskinfo.update(self.get_directory_listing("%s/%s" % (path, lsdict['name']), endpoint, recursive))

        #print diskinfo
        return diskinfo


    def get_file_disk_info(self, filelist, endpoint):
        """ get info on the files

            Parameters
            ----------
            filelist : list
                List of files to look at

            endpoint : str
                The endpoint to use

            Returns
            -------
            dict of the results
        """
        # endpoint_ls currently only does ls for directories not single files

        # determine directories for which to get listing
        pathlist = {}
        filebypath = {}
        for fname in filelist:
            (path, _) = miscutils.parse_fullname(fname, miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME)
            pathlist[path] = True
            if path not in filebypath:
                filebypath[path] = {}
            filebypath[path][fname] = True

        # get directory listing from endpoint
        diskinfo = {}
        _ = self.endpoint_activate(endpoint)
        for path in pathlist.keys():
            dirlist = self.get_directory_listing(path, endpoint, False)
            for fullname, finfo in dirlist.items():
                if fullname in filebypath[path]:
                    diskinfo[fullname] = finfo

        return diskinfo
