"""
    .. _filemgmt-ftmgmt-genfits:

    **ftmgmt_genfits**
    ------------------

    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

__version__ = "$Rev: 47020 $"

from collections import OrderedDict
import time
import pyfits

from filemgmt.ftmgmt_generic import FtMgmtGeneric
import despymisc.miscutils as miscutils
import despyfitsutils.fits_special_metadata as spmeta
import despyfitsutils.fitsutils as fitsutils

class FtMgmtGenFits(FtMgmtGeneric):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc)

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
        FtMgmtGeneric.__init__(self, filetype, config, filepat)

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values

            Parameters
            ----------
            fullname : str
                The name of the file to gather data from

            do_update : bool
                Whether to update the metadata of the file from update_info

            update_info : dict
                The data to update the header with

            Returns
            -------
            dict containing the metadata
        """
        starttime = time.time()
        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # open file
        if do_update:
            hdulist = pyfits.open(fullname, 'update')
        else:
            hdulist = pyfits.open(fullname)
        readtime = time.time()
        # read metadata and call any special calc functions
        metadata, datadefs = self._gather_metadata_file(fullname, hdulist=hdulist)
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))
        gathertime = time.time()
        # call function to update headers
        if do_update:
            self._update_headers_file(hdulist, metadata, datadefs, update_info)
        updatetime = time.time()
        # close file
        hdulist.close()

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        print "       PMT read:   %.3f" % (readtime-starttime)
        print "       PMT gather: %.3f" % (gathertime-readtime)
        print "       PMT update: %.3f" % (updatetime-gathertime)
        print "        PMT Total: %.3f" % (time.time()-starttime)
        return metadata

    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        """ Gather metadata for a single file

           Parameters
            ----------
            fullname : str
                The name of the file to gather data from

            kwargs : dict
                Dictionary containing additional info

            Returns
            -------
            dict containing the metadata
        """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))

        hdulist = kwargs['hdulist']

        metadata = OrderedDict()
        datadef = OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
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
                    if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                        miscutils.fwdebug_print("INFO: headers=%s" % \
                                                (hddict[status_sect]['h'].keys()))
                    metakeys = hddict[status_sect]['h'].keys()
                    mdata2, ddef2 = self._gather_metadata_from_header(fullname, hdulist,
                                                                      hdname, metakeys)
                    metadata.update(mdata2)
                    datadef.update(ddef2)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    for funckey in hddict[status_sect]['c'].keys():
                        try:
                            specmf = getattr(spmeta, 'func_%s' % funckey.lower())
                        except AttributeError:
                            miscutils.fwdebug_print("WARN: Couldn't find func_%s in despyfits.fits_special_metadata" % (funckey))

                        try:
                            val = specmf(fullname, hdulist, hdname)
                            metadata[funckey] = val
                        except KeyError:
                            if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                                miscutils.fwdebug_print("INFO: couldn't create value for key %s in %s header of file %s" % (funckey, hdname, fullname))

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['p'].keys()
                    mdata2, ddef2 = self._gather_metadata_from_header(fullname, hdulist,
                                                                      hdname, metakeys)
                    #print 'ddef2 = ', ddef2
                    metadata.update(mdata2)
                    datadef.update(ddef2)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: metadata = %s" % metadata)
            miscutils.fwdebug_print("INFO: datadef = %s" % datadef)
            miscutils.fwdebug_print("INFO: end")
        return metadata, datadef


    ######################################################################
    def _get_update_values_metadata(self, metadata, datadefs):
        """ Put metadata values for update in data structure easy to use

            Parameters
            ----------
            metadata : dict
                Dictionary of the metadata

            datadefs : dict
                Dictionary of additional definitions

            Returns
            -------
            OrderedDict of the update info
        """

        metadefs = self.config['filetype_metadata'][self.filetype]
        update_info = OrderedDict()
        update_info[0] = OrderedDict()   # update primary header

        for hdname, hddict in metadefs['hdus'].items():
            update_info[hdname] = OrderedDict()
            for stdict in hddict.values():
                # include values created by metadata functions and those copied from other hdu
                for derived in ['c', 'p', 'w']:
                    if derived in stdict:
                        for key in stdict[derived]:
                            uvalue = ucomment = udatatype = None
                            # we don't write filetype nor pfw_attempt_id to headers
                            if key == 'filename':
                                # write filename to header as DESFNAME
                                fitscomment = 'DES production filename'

                                # shorten comment if file name is so long the comment won't fit
                                if len(metadata['filename']) + \
                                        len('\' / %s' % fitscomment) + \
                                        len('DESFNAME= \'') > 80:
                                    if miscutils.fwdebug_check(3, "FTMGMT_DEBUG"):
                                        miscutils.fwdebug_print("WARN: %s's filename too long for DESFNAME: %s" % \
                                            (metadata['filename'], len(metadata['filename'])))
                                        fitscomment = fitscomment[:min(len(fitscomment), 80 - len(metadata['filename']) - 16)]

                                update_info[0]['DESFNAME'] = (metadata['filename'], fitscomment, 'str')

                            elif key != 'filetype' and key != 'pfw_attempt_id':
                                if key in metadata:
                                    uvalue = metadata[key]
                                    if key in datadefs:
                                        ucomment = datadefs[key][0]
                                        udatatype = datadefs[key][1]
                                    elif miscutils.fwdebug_check(3, "FTMGMT_DEBUG"):
                                        miscutils.fwdebug_print("WARN: could not find comment for key=%s" % (key))
                                    update_info[0][key] = (uvalue, ucomment, udatatype)
                                else:
                                    miscutils.fwdebug_print("WARN: could not find metadata for key=%s" % (key))
        return update_info

    ######################################################################
    def _get_file_header_key_info(self, key):
        """ From definitions of file header keys, return comment and fits data type

            Parameters
            ----------
            key : str
                The key to look for in the header

            Returns
            -------
            tuple of the description and data type
        """

        file_header_info = self.config['file_header']
        ucomment = None
        udatatype = None
        if key in file_header_info:
            if 'description' in file_header_info[key]:
                ucomment = file_header_info[key]['description']
            else:
                miscutils.fwdebug_print("WARN: could not find description for key=%s" % (key))

            if 'fits_data_type' in file_header_info[key]:
                udatatype = file_header_info[key]['fits_data_type']
            else:
                miscutils.fwdebug_print("WARN: could not find fits_data_type for key=%s" % (key))
        return ucomment, udatatype


    ######################################################################
    def _get_update_values_explicit(self, update_info):
        """ include values explicitly set by operator/framework

            Parameters
            ----------
            update_info : dict
                Dictionary of the updated header values

            Returns
            -------
            dict of the updated header values with additional components
        """

        upinfo2 = OrderedDict()

        # for each set of header updates
        for updset in update_info.values():
            headers = ['0']   # default to primary header
            if 'headers' in updset:
                headers = miscutils.fwsplit(update_info[updset], ',')

            hdu_updset = OrderedDict()
            for key, val in updset.items():
                if key != 'headers':
                    uval = ucomment = udatatype = None
                    header_info = miscutils.fwsplit(val, '/')
                    uval = header_info[0]
                    if len(header_info) == 3:
                        ucomment = header_info[1]
                        udatatype = header_info[2]
                    hdu_updset[key] = (uval, ucomment, udatatype)

            for hdname in headers:
                if hdname not in update_info:
                    upinfo2[hdname] = OrderedDict()

                upinfo2[hdname].update(hdu_updset)

        return upinfo2

    ######################################################################
    @classmethod
    def _gather_metadata_from_header(cls, fullname, hdulist, hdname, metakeys):
        """ Get values from config

            Parameters
            ----------
            fullname : str
                The name of the file

            hdulist : list
                List of the HDU's

            hdname : str
                The name of the header to look in

            metakeys : dict
                Dictionary of the keys to look for

            Returns
            -------
            tuple of the metadata and data definitions
        """

        metadata = OrderedDict()
        datadef = OrderedDict()
        for key in metakeys:
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: key=%s" % (key))
            try:
                metadata[key] = fitsutils.get_hdr_value(hdulist, key.upper(), hdname)
                datadef[key] = fitsutils.get_hdr_extra(hdulist, key.upper(), hdname)
            except KeyError:
                if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("INFO: didn't find key %s in %s header of file %s" %\
                                            (key, hdname, fullname))

        return metadata, datadef

    ######################################################################
    def _update_headers_file(self, hdulist, metadata, datadefs, update_info):
        """ Update headers in file

            Parameters
            ----------
            hdulist : list
                List of HDU's

            metadata : dict
                Dictionary of metadata to use to update the headers

            datadefs : dict
                Dictionary of the data definitions for the metadata

            update_info : dict
                The data used in the update
        """
        all_update_info = self._get_update_values_metadata(metadata, datadefs)
        wcl_update_info = self._get_update_values_explicit(update_info)
        all_update_info.update(wcl_update_info)

        # update values in file
        for hdname in all_update_info:
            newhdname = hdname
            try:
                newhdname = int(hdname)
            except ValueError:
                newhdname = hdname

            hdr = hdulist[newhdname].header
            for key, info in all_update_info[hdname].items():
                uval = info[0]
                ucomment = info[1]
                udatatype = info[2]

                if ucomment is None:
                    ucomment, udatatype = self._get_file_header_key_info(key)
                elif udatatype is None:
                    _, udatatype = self._get_file_header_key_info(key)

                if isinstance(udatatype, str) and isinstance(uval, str) and udatatype != 'str':
                    udatatype = udatatype.lower()
                    #global __builtins__
                    #uval = getattr(__builtins__, udatatype)(uval)
                    if udatatype == 'int':
                        uval = int(uval)
                    elif udatatype == 'float':
                        uval = float(uval)
                    elif udatatype == 'bool':
                        uval = bool(uval)

                if ucomment is not None:
                    hdr[key.upper()] = (uval, ucomment)
                else:
                    hdr[key.upper()] = uval
