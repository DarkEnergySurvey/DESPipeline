"""
    .. _filemgmt-ftmgmt-generic:

    **ftmgmt_generic**
    ------------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

from collections import OrderedDict
import copy
import re

import despymisc.miscutils as miscutils

class FtMgmtGeneric(object):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc)

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
        self.filetype = filetype
        self.config = config
        self.filepat = filepat

    ######################################################################
    def check_valid(self, listfullnames):
        #pylint: disable=no-self-use
        """ Check if files of of the current filetype

            Parameters
            ----------
            listfullmanes : list
                The files to check

            Returns
            -------
            dict
                The filenames as keys and a bool as to whther they are of
                the current file type (``True``) or not (``False``).
        """
        assert isinstance(listfullnames, list)

        results = {}
        for fname in listfullnames:
            results[fname] = True

        return results

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        #pylint: disable=unused-argument
        """ Read metadata from file, updating file values

            Parameters
            ----------
            fullname : str
                The name of the file to gather data from

            do_update : bool
                Whether to update the metadata of the file from update_info
                (Not used in this class, but is in sub classes)

            update_info : dict
                The data to update the header with (Not used in this class,
                but is in sub classes)

            Returns
            -------
            dict
                The metadata
        """

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: beg")

        # read metadata and call any special calc functions
        metadata = self._gather_metadata_file(fullname)

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        #pylint: disable=unused-argument
        """ Gather metadata for a single file

            Parameters
            ----------
            fullname : str
                The name of the file to gather data from

            kwargs : used by subclasses

            Returns
            -------
            dict
                The metadata
        """

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: beg  file=%s" % (fullname))

        metadata = OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: metadefs=%s" % (metadefs))
        for hdname, hddict in metadefs['hdus'].items():
            for status_sect in hddict:  # don't worry about missing here, ingest catches
                # get value from filename
                if 'f' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['f'].keys()
                    mdata2 = self._gather_metadata_from_filename(fullname, metakeys)
                    metadata.update(mdata2)

                # get value from wcl/config
                if 'w' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['w'].keys()
                    mdata2 = self._gather_metadata_from_config(fullname, metakeys)
                    metadata.update(mdata2)

                # get value directly from header
                if 'h' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot read values from header %s = %s" % \
                                    (self.__class__.__name__, hdname,
                                     hddict[status_sect]['h'].keys()), 1)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot calculate values = %s" % \
                                    (self.__class__.__name__, hddict[status_sect]['c'].keys()), 1)

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot copy values between headers = %s" % \
                                    (self.__class__.__name__, hddict[status_sect]['p'].keys()), 1)

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_from_config(self, fullname, metakeys):
        """ Get values from config

            Parameters
            ----------
            fullname : str
                The name of the file to gather data about

            metakeys : list
                List of keys to look for

            Returns
            -------
            dict
                The metadata
        """
        metadata = OrderedDict()

        for wclkey in metakeys:
            metakey = wclkey.split('.')[-1]
            if metakey == 'fullname':
                metadata['fullname'] = fullname
            elif metakey == 'filename':
                metadata['filename'] = miscutils.parse_fullname(fullname,
                                                                miscutils.CU_PARSE_FILENAME)
            elif metakey == 'filetype':
                metadata['filetype'] = self.filetype
            else:
                #if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                #    miscutils.fwdebug_print("INFO: wclkey=%s" % (wclkey))
                (exists, val) = self.config.search(wclkey)
                if exists:
                    metadata[metakey] = val

        return metadata


    ######################################################################
    def _gather_metadata_from_filename(self, fullname, metakeys):
        """ Parse filename using given filepat

                        Parameters
            ----------
            fullname : str
                The name of the file to gather data about

            metakeys : list
                List of keys to look for

            Returns
            -------
            dict
                The metadata
        """

        if self.filepat is None:
            raise TypeError("None filepat for filetype %s" % self.filetype)

        # change wcl file pattern into a pattern usable by re
        newfilepat = copy.deepcopy(self.filepat)
        varpat = r"\$\{([^$}]+:\d+)\}|\$\{([^$}]+)\}"
        listvar = []
        m = re.search(varpat, newfilepat)
        while m:
            #print m.group(1), m.group(2)
            if m.group(1) is not None:
                m2 = re.search(r'([^:]+):(\d+)', m.group(1))
                #print m2.group(1), m2.group(2)
                listvar.append(m2.group(1))

                # create a pattern that will remove the 0-padding
                newfilepat = re.sub(r"\${%s}" % (m.group(1)), r'(\d{%s})' % m2.group(2), newfilepat)
            else:
                newfilepat = re.sub(r"\${%s}" % (m.group(2)), r'(\S+)', newfilepat)
                listvar.append(m.group(2))

            m = re.search(varpat, newfilepat)


        # now that have re pattern, parse the filename for values
        filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: newfilepat = %s" % newfilepat)
        #    miscutils.fwdebug_print("INFO: filename = %s" % filename)

        m = re.search(newfilepat, filename)
        if m is None:
            miscutils.fwdebug_print("INFO: newfilepat = %s" % newfilepat)
            miscutils.fwdebug_print("INFO: filename = %s" % filename)
            raise ValueError("Pattern (%s) did not match filename (%s)" % (newfilepat, filename))

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: m.group() = %s" %  m.group())
        #    miscutils.fwdebug_print("INFO: listvar = %s" % listvar)

        # only save values parsed from filename that were requested per metakeys
        mddict = {}
        for cnt, key in enumerate(listvar):
            if key in metakeys:
                #if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                #    miscutils.fwdebug_print("INFO: saving as metadata key = %s, cnt = %s" % (key, cnt))
                mddict[key] = m.group(cnt+1)
            #elif miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            #    miscutils.fwdebug_print("INFO: skipping key = %s because not in metakeys" % key)


        #if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: mddict = %s" % mddict)

        return mddict
