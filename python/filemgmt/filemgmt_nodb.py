#!/usr/bin/env python

# $Id: filemgmt_nodb.py 47142 2018-06-20 14:27:28Z friedel $
# $Rev:: 47142                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-06-20 09:27:28 #$:  # Date of last commit.

"""
    Define a class to do file management tasks without DB
"""

__version__ = "$Rev: 47142 $"

import os

import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs

class FileMgmtNoDB(object):
    """ Class to manage files without a database
    """

    @staticmethod
    def requested_config_vals():
        """ Get the config values ans whether they are required or not """
        return {'archive':'req', fmdefs.FILE_HEADER_INFO:'req', 'filetype_metadata':'req'}

    def __init__(self, config=None, argv=None):
        self.config = config
        self.argv = argv

    def get_list_filenames(self, args):
        """ Not used for this class """
        pass

    def is_file_in_archive(self, fnames, filelist, args):
        """ Determine if the requested file is in the archive

            Parameters
            ----------
            fnames : list
                List of files to check for

            filelist : dict
                Dict of the file info

            args : dict
                Dict of additional info

            Returns
            -------
            list of booleans, one for each file, stating whether the file
            is in the archive (True) ro not (False)
        """
        archivename = args['archive']
        archivedict = self.config['archive'][archivename]
        archiveroot = os.path.realpath(archivedict['root'])

        in_archive = []
        for f in fnames:
            if os.path.exists(archiveroot + '/' + filelist['path'] + '/' + f):
                in_archive.append(f)
        return in_archive

    def is_valid_filetype(self, ftype):
        """ Determine if the file type is a valid one

            Parameters
            ----------
            ftype : str
                The file type to check

            Returns
            -------
            bool, True if the file type is valid, False otherwise
        """
        return ftype.lower() in self.config[fmdefs.FILETYPE_METADATA]

    def is_valid_archive(self, arname):
        """ Determine if the archive is valid

            Parameters
            ----------
            arname : str
                The archive name to check

            Returns
            -------
            bool, True if the archive is valid, False otherwise
        """
        return arname.lower() in self.config['archive']

    def get_file_location(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Find the location of the given files

            Parameters
            ----------
            filelist : list
                List of the files to find

            arname : str
                Name of the archive to look in

            compress_order : list
                What order to look for the file in, compressed first or uncompressed first.
                Default is filemgmt_defs.FM_PREFER_COMPRESSED

            Returns
            -------
            dict of the files and their locations
        """
        fileinfo = self.get_file_archive_info(filelist, arname, compress_order)
        rel_filenames = {}
        for f, finfo in fileinfo.items():
            rel_filenames[f] = finfo['rel_filename']
        return rel_filenames


    # compression = compressed_only, uncompressed_only, prefer uncompressed, prefer compressed, either (treated as prefer compressed)
    def get_file_archive_info(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Get the archive info for the given files

            Parameters
            ----------
            filelist : list
                List of the files to probe

            arname : str
                Name of the archive to look in

            compress_order : list
                What order to look for the file in, compressed first or uncompressed first.
                Default is filemgmt_defs.FM_PREFER_COMPRESSED

            Returns
            -------
            dict of the files and their locations
        """
        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)', 1)

        # walk archive to get all files
        fullnames = {}
        for p in compress_order:
            fullnames[p] = {}

        root = self.config['archive'][arname]['root']
        root = root.rstrip("/")  # canonicalize - remove trailing / to ensure

        for (dirpath, _, filenames) in os.walk(root, followlinks=True):
            for fname in filenames:
                d = {}
                (d['filename'], d['compression']) = miscutils.parse_fullname(fname, 3)
                d['filesize'] = os.path.getsize("%s/%s" % (dirpath, fname))
                d['path'] = dirpath[len(root)+1:]
                if d['compression'] is None:
                    compext = ""
                else:
                    compext = d['compression']
                d['rel_filename'] = "%s/%s%s" % (d['path'], d['filename'], compext)
                fullnames[d['compression']][d['filename']] = d

        print "uncompressed:", len(fullnames[None])
        print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in filelist:
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        print "archiveinfo = ", archiveinfo
        return archiveinfo

    # compression = compressed_only, uncompressed_only, prefer uncompressed, prefer compressed, either (treated as prefer compressed)
    def get_file_archive_info_path(self, path, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Get the archive info of a directory

            Parameters
            ----------
            path : str
                The path to probe

            arname : str
                Name of the archive to look in

            compress_order : list
                What order to look for the file in, compressed first or uncompressed first.
                Default is filemgmt_defs.FM_PREFER_COMPRESSED

            Returns
            -------
            dict of the files and their info
        """
        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)', 1)

        # walk archive to get all files
        fullnames = {}
        for p in compress_order:
            fullnames[p] = {}

        root = self.config['archive'][arname]['root']
        root = root.rstrip("/")  # canonicalize - remove trailing / to ensure

        list_by_name = {}
        for (dirpath, _, filenames) in os.walk(root + '/' + path):
            for fname in filenames:
                d = {}
                (d['filename'], d['compression']) = miscutils.parse_fullname(fname, 3)
                d['filesize'] = os.path.getsize("%s/%s" % (dirpath, fname))
                d['path'] = dirpath[len(root)+1:]
                if d['compression'] is None:
                    compext = ""
                else:
                    compext = d['compression']
                d['rel_filename'] = "%s/%s%s" % (d['path'], d['filename'], compext)
                fullnames[d['compression']][d['filename']] = d
                list_by_name[d['filename']] = True

        print "uncompressed:", len(fullnames[None])
        print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in list_by_name.keys():
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        print "archiveinfo = ", archiveinfo
        return archiveinfo
