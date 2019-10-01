"""
    .. _processingfw-pfwblock:

    **pfwblock**
    ------------

    functions used by the block tasks
"""

import sys
import stat
import os
import itertools
import copy
import re
import time
import json
from collections import OrderedDict

import despymisc.miscutils as miscutils

import filemgmt.archive_transfer_utils as archive_transfer_utils
import filemgmt.metadefs as metadefs
import filemgmt.fmutils as fmutils

from intgutils.wcl import WCL
import intgutils.intgdefs as intgdefs
import intgutils.intgmisc as intgmisc
import intgutils.replace_funcs as replfuncs
import intgutils.queryutils as queryutils

import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils

#######################################################################
def get_datasect_types(config, modname):
    """ tell which data sections (files, lists) are inputs vs outputs """

    miscutils.fwdebug_print("BEG %s" % modname)

    #infsect = which_are_inputs(config, modname)
    #outfsect = which_are_outputs(config, modname)

    inputs = {pfwdefs.SW_FILESECT: [], pfwdefs.SW_LISTSECT: []}
    outfiles = OrderedDict()
    intermedfiles = OrderedDict()

    # For wrappers with more than 1 exec section, the inputs of one exec can
    #     be the inputs of a 2nd exec the framework should not attempt to stage
    #     these intermediate files
    execs = intgmisc.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for _, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                parts = miscutils.fwsplit(outfile, '.')
                outfiles['.'.join(parts[1:])] = True
                intermedfiles[outfile] = True

        if pfwdefs.SW_INPUTS in einfo:
            inarr = miscutils.fwsplit(einfo[pfwdefs.SW_INPUTS].lower())
            inarr2 = []
            for inname in inarr:
                numdots = inname.count('.')
                if numdots == 1:
                    inarr2.append(inname)
                else:
                    parts = miscutils.fwsplit(inname, '.')
                    inarr2.append('.'.join(parts[0:2]))
                    inarr2.append('file.%s'% (parts[2]))

            for inname in inarr2:
                if inname not in intermedfiles:
                    parts = miscutils.fwsplit(inname, '.')
                    inputs[parts[0]].append('.'.join(parts[1:]))
    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print('inputs=%s' % inputs)
        miscutils.fwdebug_print('outputs=%s' % outfiles.keys())
    miscutils.fwdebug_print("END")
    return (inputs, outfiles.keys())



#######################################################################
def copy_master(masterdata, nickname=None, startline=1):
    """ For master data list that has multiple files per line, copy set of files """

    lines = {}
    linecnt = startline
    for masterline in masterdata['list'][intgdefs.LISTENTRY].values():
        try:
            if nickname is not None:
                if nickname not in masterline['file']:
                    raise KeyError("Line doesn't have file with nickname %s" % nickname)
                else:
                    lines[linecnt] = {'file': {'file0001': masterline['file'][nickname]}}
            elif len(masterline['file']) == 1:
                lines[linecnt] = {'file': {'file0001': masterline['file'].values()[0]}}
            else:
                raise ValueError("Problem copying master line - nickname count mismatch")

            linecnt += 1
        except:
            print "line %s: masterline['file'] = %s" % (linecnt, masterline['file'])
            print "\n\nline %s: nickname = %s" % (linecnt, masterline['file'])
            raise
    #return {'list': {intgdefs.LISTENTRY: lines}}
    return lines

#######################################################################
def add_runtime_path(config, currvals, fname, finfo, filename):
    """ Add runtime path to filename """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("creating path for %s" % fname)
        miscutils.fwdebug_print("finfo = %s" % finfo)
        miscutils.fwdebug_print("currvals = %s" % currvals)




    path = config.get_filepath('runtime', None, {pfwdefs.PF_CURRVALS: currvals,
                                                 'searchobj': finfo,
                                                 intgdefs.REPLACE_VARS: True,
                                                 'expand': True})

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("\tpath = %s" % path)

    cmpext = ''
    if ('compression' in finfo and
            finfo['compression'] is not None and
            finfo['compression'] != 'None'):
        cmpext = finfo['compression']

    fullname = []
    if isinstance(filename, list):
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("%s has multiple names, number of names = %s" % (fname, len(filename)))
        for name in filename:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("path + filename = %s/%s" % (path, name))
            fullname.append("%s/%s%s" % (path, name, cmpext))
    else:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Adding path to filename for %s" % filename)
        fullname = ["%s/%s%s" % (path, filename, cmpext)]

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END fullname = %s" % fullname)
    return fullname


#######################################################################
def create_simple_list(config, lname, ldict, currvals):
    """ Create simple filename list file based upon patterns """
    miscutils.fwdebug_print("BEG - %s" % lname)
    listname = config.getfull('listname',
                              {pfwdefs.PF_CURRVALS: currvals,
                               'searchobj': ldict})

    filename = config.get_filename(None,
                                   {pfwdefs.PF_CURRVALS: currvals,
                                    'searchobj': ldict,
                                    'required': True,
                                    'expand': True,
                                    intgdefs.REPLACE_VARS: False})

    pfwutils.search_wcl_for_variables(config)


    if isinstance(filename, list):
        listcontents = '\n'.join(filename)
    else:
        listcontents = filename

    listdir = os.path.dirname(listname)
    if listdir and not os.path.exists(listdir):
        miscutils.coremakedirs(listdir)

    with open(listname, 'w', 0) as listfh:
        listfh.write(listcontents+"\n")

    miscutils.fwdebug_print("END\n\n")


###########################################################
def create_sublist_file(config, fname, finfo, currvals):
    """ Create sublists of filenames for file definition """
    #filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
    #                                      'searchobj': finfo,
    #                                      intgdefs.REPLACE_VARS: False,
    #                                      'expand': False})

    searchopts = {pfwdefs.PF_CURRVALS: currvals,
                  'searchobj': finfo,
                  intgdefs.REPLACE_VARS: False,
                  'expand': False}

    # first check for filename pattern override
    (found, filenamepat) = config.search('filename', searchopts)
    if not found:
        # get filename pattern from global settings:
        (found, filepat) = config.search(pfwdefs.SW_FILEPAT, searchopts)

    if not found:
        miscutils.fwdie("Error: Could not find file pattern %s" % pfwdefs.SW_FILEPAT,
                        pfwdefs.PF_EXIT_FAILURE)

    if pfwdefs.SW_FILEPATSECT not in config:
        miscutils.fwdie("Error: Could not find filename pattern section (%s)" % \
                        pfwdefs.SW_FILEPATSECT, pfwdefs.PF_EXIT_FAILURE)
    elif filepat in config[pfwdefs.SW_FILEPATSECT]:
        filenamepat = config[pfwdefs.SW_FILEPATSECT][filepat]
    else:
        miscutils.fwdie("Error: Could not find filename pattern for %s" % filepat,
                        pfwdefs.PF_EXIT_FAILURE, 2)

    # get 2 list (filename, filedict) by expanding variables in the filename pattern
    newfileinfo = replfuncs.replace_vars(filenamepat, config,
                                         {pfwdefs.PF_CURRVALS: currvals,
                                          'searchobj': finfo,
                                          intgdefs.REPLACE_VARS: True,
                                          'expand': True,
                                          'keepvars': True})

    # convert to same format as if read from file created by query
    filelist_wcl = None
    if newfileinfo:
        if isinstance(newfileinfo[0], str):
            newfileinfo = ([newfileinfo[0]], [newfileinfo[1]])

        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("newfileinfo = %s" % str(newfileinfo))

        filedict_list = []
        for fcnt in range(0, len(newfileinfo[0])):
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("name = %s" % str(newfileinfo[0][fcnt]))
                miscutils.fwdebug_print("info = %s" % str(newfileinfo[1][fcnt]))
            file1 = newfileinfo[1][fcnt]
            file1['filename'] = newfileinfo[0][fcnt]

            # merge particular file information with file definition
            sinfo = copy.deepcopy(finfo)
            sinfo.update(file1)

            file1['fullname'] = add_runtime_path(config, currvals, fname, sinfo, file1['filename'])[0]
            filedict_list.append(file1)
        filelist_wcl = queryutils.convert_single_files_to_lines(filedict_list)

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.pretty_print_dict(filelist_wcl)
    return filelist_wcl


###########################################################
def create_simple_sublist(config, moddict, ldict, currvals):
    """ create a simple sublist of files for a list without query """

    miscutils.fwdebug_print("BEG")

    # grab file section names from columns value in list def
    filesects = OrderedDict()
    if 'columns' in ldict:
        columns = convert_col_string_to_list(ldict['columns'], with_format=True)
        for col in columns:
            filesects[col.lower().split('.')[0]] = True

    if len(filesects) > 1:
        miscutils.fwdie('The framework currently does not support multiple file-column lists without query', pfwdefs.PF_EXIT_FAILURE)

    fname = filesects.keys()[0]
    finfo = moddict[pfwdefs.SW_FILESECT][fname]
    filelist_wcl = create_sublist_file(config, fname, finfo, currvals)

    miscutils.fwdebug_print("END")

    return filelist_wcl


#######################################################################
def get_match_keys(sdict):
    """ Get keys on which to match files """
    mkeys = []

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("keys in sdict: %s " % sdict.keys())

    if 'loopkey' in sdict:
        mkeys = miscutils.fwsplit(sdict['loopkey'].lower())
        #mkeys.sort()
    elif 'match' in sdict:
        mkeys = miscutils.fwsplit(sdict['match'].lower())
        #mkeys.sort()
    elif 'divide_by' in sdict:
        mkeys = miscutils.fwsplit(sdict['divide_by'].lower())
        #mkeys.sort()

    return mkeys


#######################################################################
def find_sublist(objdef, objinst, sublists):
    """ Find sublist """

    if len(sublists.keys()) > 1:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("sublist keys: %s" % (sublists.keys()))

        matchkeys = get_match_keys(objdef)

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("matchkeys: %s" % (matchkeys))

        index = ""
        for mkey in matchkeys:
            if mkey not in objinst:
                miscutils.fwdie("Error: Cannot find match key %s in inst %s" % (mkey, objinst),
                                pfwdefs.PF_EXIT_FAILURE)
            index += objinst[mkey] + '_'

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("sublist index = "+index)

        if index not in sublists:
            miscutils.fwdie("Error: Cannot find sublist matching "+index, pfwdefs.PF_EXIT_FAILURE)
        sublist = sublists[index]
    else:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Taking first sublist.  sublist keys: %s" % (sublists.keys()))
        sublist = sublists.values()[0]

    return sublist

#######################################################################
def which_are_inputs(config, modname):
    """ Return dict of files/lists that are inputs for given module """
    miscutils.fwdebug_print("BEG %s" % modname)

    inputs = {pfwdefs.SW_FILESECT: [], pfwdefs.SW_LISTSECT: []}
    outfiles = OrderedDict()

    # For wrappers with more than 1 exec section, the inputs of one exec can
    #     be the inputs of a 2nd exec the framework should not attempt to stage
    #     these intermediate files
    execs = intgmisc.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for _, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                outfiles[outfile] = True

        if pfwdefs.SW_INPUTS in einfo:
            inarr = miscutils.fwsplit(einfo[pfwdefs.SW_INPUTS].lower())
            for inname in inarr:
                if inname not in outfiles:
                    parts = miscutils.fwsplit(inname, '.')
                    inputs[parts[0]].append('.'.join(parts[1:]))

    #miscutils.fwdebug_print(inputs)
    miscutils.fwdebug_print("END")
    return inputs


#######################################################################
def which_are_outputs(config, modname):
    """ Return dict of files that are outputs for given module """
    miscutils.fwdebug_print("BEG %s" % modname)

    outfiles = OrderedDict()

    execs = intgmisc.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for _, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                parts = miscutils.fwsplit(outfile, '.')
                outfiles['.'.join(parts[1:])] = True

    #miscutils.fwdebug_print(outfiles.keys())
    miscutils.fwdebug_print("END")
    return outfiles.keys()





#######################################################################
def assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict,
                                currvals, winst, fsectname, finfo,
                                masterdata, sublists, is_iter_obj=False):
    """ Assign files to wrapper instance """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG: Working on file %s" % fsectname)
        miscutils.fwdebug_print("theinputs: %s" % theinputs)
        miscutils.fwdebug_print("outputs: %s" % theoutputs)
        miscutils.fwdebug_print("is_iter_obj: %s" % is_iter_obj)

    if pfwdefs.IW_FILESECT not in winst:
        winst[pfwdefs.IW_FILESECT] = OrderedDict()

    if 'listonly' in finfo and miscutils.convertBool(finfo['listonly']):
        for osectname in theoutputs:
            if osectname.endswith('.'+fsectname):
                winst[pfwdefs.IW_FILESECT][fsectname] = OrderedDict()
                miscutils.fwdebug_print("Added %s a listonly key to the file section" % fsectname)

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Skipping %s due to listonly key" % fsectname)
        return

    modname = moddict['modulename']

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("modname = %s" % modname)
        miscutils.fwdebug_print("winst: %s" % winst)
        miscutils.fwdebug_print("currvals: %s" % currvals)

    fkey = 'file-%s' % fsectname
    winst[pfwdefs.IW_FILESECT][fsectname] = OrderedDict()
    if sublists is not None and fkey in sublists:  # files came from query
        sublist = find_sublist(finfo, winst, sublists[fkey])
        ignore_multiple_error = False
        if 'ignore_multiple_error' in finfo and miscutils.convertBool(finfo['ignore_multiple_error']):
            ignore_multiple_error = True

        if len(sublist['list'][intgdefs.LISTENTRY]) > 1 and not ignore_multiple_error:
            print "Error: more than 1 line to choose from for file %s" % fkey
            print "\twinst = ", winst
            print "\tnum sublists = ", len(sublists[fkey])
            skeys = sublists[fkey].keys()
            for i in range(0, min(10, len(skeys))):
                print skeys[i],
            print "\n"
            print "\t# files = ", len(sublist['list'][intgdefs.LISTENTRY])
            print miscutils.pretty_print_dict(sublist['list'][intgdefs.LISTENTRY])

            print "\tCheck divide_by/match"
            miscutils.fwdie("Error: more than 1 line to choose from for file (%s)" % \
                            fkey, pfwdefs.PF_EXIT_FAILURE)

        fullnames = []
        for line in sublist['list'][intgdefs.LISTENTRY].values():
            if 'file' not in line:
                miscutils.fwdie("Error: 0 file in line" + str(line), pfwdefs.PF_EXIT_FAILURE)

            if len(line['file']) > 1:
                #print miscutils.pretty_print_dict(line['file'])
                raise Exception("more than 1 file to choose from for file" + line['file'])
            finfo = line['file'].values()[0]
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("finfo = %s" % finfo)

            fullnames.append(finfo['fullname'])
        winst[pfwdefs.IW_FILESECT][fsectname]['fullname'] = ','.join(fullnames)

    elif 'fullname' in moddict[pfwdefs.SW_FILESECT][fsectname]:
        winst[pfwdefs.IW_FILESECT][fsectname]['fullname'] = moddict[pfwdefs.SW_FILESECT][fsectname]['fullname']
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Copied fullname for %s = %s" % \
                                    (fsectname, winst[pfwdefs.IW_FILESECT][fsectname]))
    else:
        sobj = copy.deepcopy(winst)
        sobj.update(finfo)   # order matters file values must override winst values

        # note: save keys/vals used when creating filenames in order to use to create future filenames

        if 'filename' in moddict[pfwdefs.SW_FILESECT][fsectname]:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("filename in %s" % fsectname)

            filename = config.get('filename', {pfwdefs.PF_CURRVALS: currvals,
                                               'searchobj': sobj,
                                               'expand': False,
                                               'required': True,
                                               intgdefs.REPLACE_VARS:False})

            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("filename = %s" % filename)

        else:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("creating filename for %s" % fsectname)
                miscutils.fwdebug_print("\tfinfo = %s" % finfo)
                miscutils.fwdebug_print("\tsobj = %s" % sobj)
            filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
                                                  'searchobj': sobj,
                                                  'expand': False,
                                                  intgdefs.REPLACE_VARS:False})

        fileinfo = replfuncs.replace_vars(filename, config, {pfwdefs.PF_CURRVALS: currvals,
                                                             'searchobj': sobj,
                                                             'expand': True,
                                                             intgdefs.REPLACE_VARS:True,
                                                             'keepvars': True})
        if fileinfo is None:
            miscutils.fwdie('empty fileinfo %s %s' % (modname, fkey), pfwdefs.PF_EXIT_FAILURE)

        # save file info as if we read from query
        fnames = fileinfo[0]
        filelist = []
        if isinstance(fnames, list):
            for i, fname in enumerate(fnames):
                finfo = fileinfo[1][i]
                finfo['filename'] = fname
                filelist.append(finfo)
        else:
            finfo = fileinfo[1]
            finfo['filename'] = fnames
            filelist.append(finfo)


        if modname not in masterdata:
            masterdata[modname] = OrderedDict()

        if fkey in masterdata[modname]:
            initcnt = len(masterdata[modname][fkey]['list']['line']) + 1
            newdata = queryutils.convert_single_files_to_lines(filelist, initcnt)
            masterdata[modname][fkey]['list']['line'].update(newdata['list']['line'])
        else:
            masterdata[modname][fkey] = queryutils.convert_single_files_to_lines(filelist)

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("saved file info for %s.%s" % (modname, fkey))

        winst[pfwdefs.IW_FILESECT][fsectname]['filename'] = fnames

        # Add runtime path to filename
        fullname = add_runtime_path(config, currvals, fsectname, sobj, winst[pfwdefs.IW_FILESECT][fsectname]['filename'])

        winst[pfwdefs.IW_FILESECT][fsectname]['fullname'] = ','.join(fullname)
        #print winst[pfwdefs.IW_FILESECT][fsectname]['fullname']
        del winst[pfwdefs.IW_FILESECT][fsectname]['filename']



    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("is_iter_obj = %s %s" % (is_iter_obj, finfo))
    if finfo is not None and is_iter_obj:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("is_iter_obj = true")
        winst['iter_obj_info'] = {}
        for key, val in finfo.items():
            if key not in ['fullname', 'filename', 'filepat', 'dirpat', 'filetype', 'compression']:
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("is_iter_obj: saving %s" % key)
                winst['iter_obj_info'][key] = val

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END: Done working on file %s" % fsectname)
        miscutils.fwdebug_print("END: winst=%s" % winst)



#######################################################################
def assign_list_to_wrapper_inst(config, theinputs, moddict, currvals,
                                winst, lname, ldict, sublists):
    """ Assign list to wrapper instance """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG: Working on list %s from %s" % (lname, moddict['modulename']))
        miscutils.fwdebug_print("sublists.keys() = %s" % (sublists.keys()))
        miscutils.fwdebug_print("currvals = %s" % (currvals))
    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("ldict = %s" % (ldict))

    if pfwdefs.IW_LISTSECT not in winst:
        winst[pfwdefs.IW_LISTSECT] = OrderedDict()


    ### create an object that has values from ldict and winst
    sobj = copy.deepcopy(ldict)
    sobj.update(winst)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("sobj = %s" % (sobj))

    sublist = None
    lkey = 'list-%s' % lname
    if lkey not in sublists:
        sublist = create_simple_sublist(config, lname, ldict, currvals)
    else:
        sublist = find_sublist(ldict, winst, sublists[lkey])

    if sublist is not None:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("lname = %s, sublist has %s lines" % (lname, len(sublist['list'][intgdefs.LISTENTRY])))

        for llabel, lldict in sublist['list'][intgdefs.LISTENTRY].items():
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("llabel = %s, ldict = %s" % (llabel, ldict))
            for flabel, _ in lldict['file'].items():
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("flabel = %s, theinputs = %s" % (flabel, theinputs))

        ### create an object that has values from ldict and winst
        msobj = copy.deepcopy(ldict)
        msobj.update(winst)

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("msobj = %s" % (msobj))

        if pfwdefs.DIV_LIST_BY_COL in msobj:
            divbycol = msobj[pfwdefs.DIV_LIST_BY_COL]
            del msobj[pfwdefs.DIV_LIST_BY_COL]
            for divcolname, divcoldict in divbycol.items():
                sobj = copy.deepcopy(msobj)
                sobj.update(divcoldict)
                winst[pfwdefs.IW_LISTSECT][divcolname] = {'fullname': output_list(config, sublist, sobj, lname, currvals),
                                                          'columns': ','.join(convert_col_string_to_list(divcoldict['columns'], False))}
                lineformat = intgdefs.DEFAULT_LIST_FORMAT
                if intgdefs.LIST_FORMAT in divcoldict:
                    lineformat = divcoldict[intgdefs.LIST_FORMAT]
                winst[pfwdefs.IW_LISTSECT][divcolname][intgdefs.LIST_FORMAT] = lineformat

        else:
            cols = get_list_all_columns(msobj, with_format=False)
            winst[pfwdefs.IW_LISTSECT][lname] = {'fullname': output_list(config, sublist, msobj, lname, currvals),
                                                 'columns': ','.join(cols[0])}

            lineformat = intgdefs.DEFAULT_LIST_FORMAT
            if intgdefs.LIST_FORMAT in ldict:
                lineformat = ldict[intgdefs.LIST_FORMAT]
            winst[pfwdefs.IW_LISTSECT][lname][intgdefs.LIST_FORMAT] = lineformat
    else:
        print "Warning: Couldn't find files to put in list %s in %s" % (lname, moddict['modulename'])

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END")




#######################################################################
def assign_data_wrapper_inst(config, modname, winst, masterdata, sublists,
                             theinputs, theoutputs):
    #pylint: disable=unbalanced-tuple-unpacking
    """ Assign data like files and lists to wrapper instances """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s" % modname)
        miscutils.fwdebug_print("sublists.keys() = %s" % (sublists.keys()))

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    currvals = {'curr_module': modname}
    (found, loopkeys) = config.search('wrapperloop',
                                      {pfwdefs.PF_CURRVALS: currvals,
                                       'required': False,
                                       intgdefs.REPLACE_VARS: True})
    if found:
        loopkeys = miscutils.fwsplit(loopkeys.lower())
    else:
        loopkeys = []

    #winst['wrapinputs'] = OrderedDict()
    #winst['wrapoutputs'] = OrderedDict()

    # create currvals
    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
    for key in loopkeys:
        currvals[key] = winst[key]
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("currvals " + str(currvals))

    # do wrapper loop object first, if exists, to provide keys for filenames
    iter_obj_key = get_wrap_iter_obj_key(moddict)


    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("%s: Assigning files to wrapper inst" % winst[pfwdefs.PF_WRAPNUM])

    #if iter_obj_key is not None or pfwdefs.SW_FILESECT in moddict:
    if iter_obj_key is not None:
        (iter_obj_sect, iter_obj_name) = miscutils.fwsplit(iter_obj_key, '.')
        iter_obj_dict = pfwutils.get_wcl_value(iter_obj_key, moddict)
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("iter_obj %s %s" % (iter_obj_name, iter_obj_sect))
        if iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower():
            assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst,
                                        iter_obj_name, iter_obj_dict, masterdata, sublists, True)
        elif iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower():
            assign_list_to_wrapper_inst(config, theinputs, moddict, currvals, winst,
                                        iter_obj_name, iter_obj_dict, sublists)
        else:
            miscutils.fwdie("Error: unknown iter_obj_sect (%s)" % iter_obj_sect,
                            pfwdefs.PF_EXIT_FAILURE)


    if pfwdefs.SW_FILESECT in moddict:
        for fname, fdict in moddict[pfwdefs.SW_FILESECT].items():
            if iter_obj_key is not None and \
               iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower() and \
               iter_obj_name.lower() == fname.lower():
                continue    # already did iter_obj
            assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst,
                                        fname, fdict, masterdata, sublists, False)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("currvals " + str(currvals))

    if pfwdefs.SW_LISTSECT in moddict:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("%s: Assigning lists to wrapper inst" % winst[pfwdefs.PF_WRAPNUM])
        for lname, ldict in moddict[pfwdefs.SW_LISTSECT].items():
            if iter_obj_key is not None and \
               iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower() and \
               iter_obj_name.lower() == lname.lower():
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("skipping list %s as already did for it as iter_obj" % lname)
                continue    # already did iter_obj
            assign_list_to_wrapper_inst(config, theinputs, moddict, currvals, winst,
                                        lname, ldict, sublists)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")



#######################################################################
def output_list(config, sublist, sobj, lname, currvals):
    """ Output list """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG: %s" % (lname))
        miscutils.fwdebug_print("sobj dict: %s" % sobj)
        miscutils.fwdebug_print("creating listdir and listname")

    # list dir and filename must use current attempt values
    currvals2 = copy.deepcopy(currvals)
    currvals2[pfwdefs.REQNUM] = config.getfull(pfwdefs.REQNUM)
    currvals2[pfwdefs.UNITNAME] = config.getfull(pfwdefs.UNITNAME)
    currvals2[pfwdefs.ATTNUM] = config.getfull(pfwdefs.ATTNUM)

    listdir = config.get_filepath('runtime', 'list', {pfwdefs.PF_CURRVALS: currvals2,
                                                      'required': True, intgdefs.REPLACE_VARS: True,
                                                      'searchobj': sobj})

    listname = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals2,
                                          'searchobj': sobj, 'required': True, intgdefs.REPLACE_VARS: True})
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("listname = %s" % (listname))
    listname = "%s/%s" % (listdir, listname)

    #winst[pfwdefs.IW_LISTSECT][lname]['fullname'] = listname
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("full listname = %s" % (listname))

    listdir = os.path.dirname(listname)
    miscutils.coremakedirs(listdir)

    lineformat = intgdefs.DEFAULT_LIST_FORMAT
    if intgdefs.LIST_FORMAT in sobj:
        lineformat = sobj[intgdefs.LIST_FORMAT]

    lines = sublist['list'][intgdefs.LISTENTRY].values()
    if 'sortkey' in sobj and sobj['sortkey'] is not None:
        # (key, numeric, reverse)
        sort_reverse = False
        sort_numeric = False

        if sobj['sortkey'].strip().startswith('('):
            rmatch = re.match(r'\(([^)]+)', sobj['sortkey'])
            if rmatch:
                sortinfo = miscutils.fwsplit(rmatch.group(1))
                sort_key = sortinfo[0]
                if len(sortinfo) > 1:
                    sort_numeric = miscutils.convertBool(sortinfo[1])
                if len(sortinfo) > 2:
                    sort_reverse = miscutils.convertBool(sortinfo[2])
            else:
                miscutils.fwdie("Error: problems parsing sortkey...\n%s" % \
                                (sobj['sortkey']), pfwdefs.PF_EXIT_FAILURE)
        else:
            sort_key = sobj['sortkey']

        sort_key = sort_key.lower()

        if sort_numeric:
            lines = sorted(lines, reverse=sort_reverse,
                           key=lambda k: float(get_value_from_line(k, sort_key, None, 1)))
        else:
            lines = sorted(lines, reverse=sort_reverse,
                           key=lambda k: get_value_from_line(k, sort_key, None, 1))

    allow_missing = False
    if 'allow_missing' in sobj:
        allow_missing = miscutils.convertBool(sobj['allow_missing'])

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("sobj = %s" % sobj)
    columns = get_list_all_columns(sobj)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("Writing list to file %s" % listname)
    with open(listname, "w") as listfh:
        for linedict in lines:
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("columns = %s" % columns)
            output_line(listfh, linedict, lineformat, allow_missing, columns[0])

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return listname




#####################################################################
def output_line(listfh, line, lineformat, allow_missing, keyarr):
    """ output line into input list for science code"""
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG line=%s  keyarr=%s" % (line, keyarr))

    lineformat = lineformat.lower()

    if lineformat == 'config' or lineformat == 'wcl':
        listfh.write("<file>\n")

    numkeys = len(keyarr)
    for i in range(0, numkeys):
        key = keyarr[i]
        value = None
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("key: %s" % key)

        valuefmt = None
        if key.startswith('$FMT{'):
            rmatch = re.match(r'\$FMT\{\s*([^,]+)\s*,\s*(\S+)\s*\}', key)
            if rmatch:
                valuefmt = rmatch.group(1).strip()
                key = rmatch.group(2).strip()
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("valuefmt = %s, key = %s" % (valuefmt, key))
            else:
                miscutils.fwdie("Error: invalid FMT column: %s" % (key), pfwdefs.PF_EXIT_FAILURE)


        if '.' in  key:
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("Found period in key")
            [nickname, key2] = key.replace(' ', '').split('.')
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("\tnickname = %s, key2 = %s" % (nickname, key2))
            value = get_value_from_line(line, key2, nickname, None)
            if value is None:
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("Didn't find value in line with nickname %s" % (nickname))
                    miscutils.fwdebug_print("Trying to find %s without nickname" % (key2))
                value = get_value_from_line(line, key2, None, 1)
                if value is None:
                    if allow_missing:
                        value = ""
                    else:
                        miscutils.fwdie("Error: could not find value %s for line...\n%s" % \
                                        (key, line), pfwdefs.PF_EXIT_FAILURE)
                else: # assume nickname was really table name
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("\tassuming nickname (%s) was really table name" % (nickname))
                    key = key2
        else:
            value = get_value_from_line(line, key, None, 1)

        # handle last field (separate to avoid trailing comma)
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("printing key=%s value=%s" % (key, value))
        if i == numkeys - 1:
            print_value(listfh, key, value, lineformat, True, valuefmt)
        else:
            print_value(listfh, key, value, lineformat, False, valuefmt)

    if lineformat == "config" or lineformat == 'wcl':
        listfh.write("</file>\n")
    else:
        listfh.write("\n")


#####################################################################
def print_value(outfh, key, value, lineformat, last, valuefmt):
    """ output value to input list in correct format """

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s=%s (%s)" % (key, value, type(value)))

    if valuefmt is not None:
        if re.search(r'%\d*d', valuefmt):
            value = valuefmt % int(value)
        elif re.search(r'%\d*(.\d+)f', valuefmt):
            value = valuefmt % float(value)
        else:
            value = valuefmt % value

    lineformat = lineformat.lower()
    if lineformat == 'config' or lineformat == 'wcl':
        outfh.write("     %s=%s\n" % (key, str(value)))
    else:
        outfh.write(str(value))
        if not last:
            if lineformat == 'textcsv':
                outfh.write(', ')
            elif lineformat == 'texttab':
                outfh.write('\t')
            else:
                outfh.write(' ')



#######################################################################
def finish_wrapper_inst(config, modname, winst, outfsect):
    """ Finish creating wrapper instances with tasks like making input and output filenames """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s" % modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    if 'iter_obj_info' in winst:
        for key, val in winst['iter_obj_info'].items():
            if key not in winst:
                winst[key] = val
        del winst['iter_obj_info']

    # create searching options
    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
    searchopts = {pfwdefs.PF_CURRVALS: currvals,
                  'searchobj': winst,
                  intgdefs.REPLACE_VARS: True,
                  'required': True}


    if pfwdefs.SW_FILESECT in moddict:
        for fname, fdict in moddict[pfwdefs.SW_FILESECT].items():
            #print "fname = %s" % fname
            is_output_file = False
            for ofsect in outfsect:
                #print "ofsect = %s" % ofsect
                if ofsect == fname or ofsect.endswith('.'+fname):
                    is_output_file = True
            #print "is_output_file = %s" % is_output_file

            if 'listonly' in fdict and miscutils.convertBool(fdict['listonly']):
                if not is_output_file:
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("Skipping %s due to listonly key" % fname)
                    continue

            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print('%s: working on file: %s' % (winst[pfwdefs.PF_WRAPNUM], fname))
                if 'fullname' in winst[pfwdefs.IW_FILESECT][fname]:
                    miscutils.fwdebug_print("fullname = %s" % (winst[pfwdefs.IW_FILESECT][fname]['fullname']))

            #for k in ['filetype', metadefs.WCL_META_REQ, metadefs.WCL_META_OPT,
            #          pfwdefs.SAVE_FILE_ARCHIVE, pfwdefs.COMPRESS_FILES,pfwdefs.DIRPAT]:
            #    if k in fdict:
            for k in fdict:
                if k not in ['keyvals']:
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("%s copying %s" % (fname, k))
                    winst[pfwdefs.IW_FILESECT][fname][k] = copy.deepcopy(fdict[k])
                elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("%s: no %s" % (fname, k))

            if pfwdefs.SW_OUTPUT_OPTIONAL in fdict:
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("%s copying %s " % (fname, pfwdefs.SW_OUTPUT_OPTIONAL))

                winst[pfwdefs.IW_FILESECT][fname][pfwdefs.IW_OUTPUT_OPTIONAL] = miscutils.convertBool(fdict[pfwdefs.SW_OUTPUT_OPTIONAL])

            hdrups = pfwutils.get_hdrup_sections(fdict, metadefs.WCL_UPDATE_HEAD_PREFIX)
            for hname, hdict in hdrups.items():
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("%s copying %s" % (fname, hname))
                winst[pfwdefs.IW_FILESECT][fname][hname] = copy.deepcopy(hdict)

            # save OPS path for archive
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("Is fname (%s) in outputfiles? %s" % \
                                        (fname, is_output_file))
            filesave = miscutils.checkTrue(pfwdefs.SAVE_FILE_ARCHIVE, fdict, True)
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("Is save_file_archive true? %s" % (filesave))

            if is_output_file:
                winst[pfwdefs.IW_FILESECT][fname][pfwdefs.SAVE_FILE_ARCHIVE] = filesave  # canonicalize
                if pfwdefs.DIRPAT not in fdict:
                    print "Warning: Could not find %s in %s's section" % (pfwdefs.DIRPAT, fname)
                else:
                    searchobj = copy.deepcopy(fdict)
                    searchobj.update(winst)
                    searchopts['searchobj'] = searchobj
                    winst[pfwdefs.IW_FILESECT][fname]['archivepath'] = config.get_filepath('ops',
                                                                                           fdict[pfwdefs.DIRPAT], searchopts)

            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("fdict = %s" % fdict)

    searchopts[intgdefs.REPLACE_VARS] = True

    # wrappername
    winst['wrappername'] = config.getfull('wrappername', searchopts)

    # input wcl fullname
    inputwcl_name = config.get_filename('inputwcl', searchopts)
    inputwcl_path = config.get_filepath('runtime', 'inputwcl', searchopts)
    #print inputwcl_name, inputwcl_path
    winst['inputwcl'] = inputwcl_path + '/' + inputwcl_name


    # log fullname
    log_name = config.get_filename('log', searchopts)
    log_path = config.get_filepath('runtime', 'log', searchopts)
    winst['log'] = log_path + '/' + log_name
    winst['log_archive_path'] = config.get_filepath('ops', 'log', searchopts)
    #output_filenames.append(winst['log'])


    # output wcl fullname
    outputwcl_name = config.get_filename('outputwcl', searchopts)
    outputwcl_path = config.get_filepath('runtime', 'outputwcl', searchopts)
    winst['outputwcl'] = outputwcl_path + '/' + outputwcl_name

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    #return input_filenames, output_filenames


#######################################################################
def add_file_metadata(config, modname):
    """ add file metadata sections to a single file section from a module"""

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("Working on module " + modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = intgmisc.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)

    if pfwdefs.SW_FILESECT in moddict:
        for k in execs:
            if pfwdefs.SW_OUTPUTS in moddict[k]:
                for outfile in miscutils.fwsplit(moddict[k][pfwdefs.SW_OUTPUTS]):
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("Working on output file " + outfile)
                    match = re.match(r'%s.(\w+)' % pfwdefs.SW_FILESECT, outfile)
                    if match:
                        fname = match.group(1)
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("Working on file " + fname)
                        if fname not in moddict[pfwdefs.SW_FILESECT]:
                            msg = "Error: file %s listed in %s, but not defined in %s section" % \
                                (fname, pfwdefs.SW_OUTPUTS, pfwdefs.SW_FILESECT)
                            miscutils.fwdie(msg, pfwdefs.PF_EXIT_FAILURE)

                        fdict = moddict[pfwdefs.SW_FILESECT][fname]
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("output file dictionary for %s = %s" % (outfile, fdict))
                    elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("output file %s doesn't have definition (%s) " % (k, pfwdefs.SW_FILESECT))

            elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("No was_generated_by for %s" % (k))

    elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("No file section (%s)" % pfwdefs.SW_FILESECT)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")





#######################################################################
def init_use_archive_info(config, jobwcl, which_use_input, which_use_output, which_archive):
    """ Initialize use archive info """
    if which_use_input in config:
        jobwcl[which_use_input] = config.getfull(which_use_input).lower()
    else:
        jobwcl[which_use_input] = 'never'

    if which_use_output in config:
        jobwcl[which_use_output] = config.getfull(which_use_output).lower()
    else:
        jobwcl[which_use_output] = 'never'

    if jobwcl[which_use_input] != 'never' or jobwcl[which_use_output] != 'never':
        jobwcl[which_archive] = config.getfull(which_archive)
        archive = jobwcl[which_archive]
    else:
        jobwcl[which_archive] = None
        archive = 'no_archive'

    return archive


#######################################################################
def write_jobwcl(config, jobkey, jobdict):
    """ write a little config file containing variables needed at the job level """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG jobnum=%s jobkey=%s" % (jobdict['jobnum'], jobkey))

    jobdict['jobwclfile'] = config.get_filename('jobwcl', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM: jobdict['jobnum']}, 'required': True, intgdefs.REPLACE_VARS: True})
    jobdict['outputwcltar'] = config.get_filename('outputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}, 'required': True, intgdefs.REPLACE_VARS: True})

    jobdict['envfile'] = config.get_filename('envfile')

    modulelist = miscutils.fwsplit(config.getfull(pfwdefs.SW_MODULELIST).lower())
    fwgroups = OrderedDict()
    gnum = 1
    for modname in modulelist:
        if modname in jobdict['parlist']:
            fwgroups['g%04i' % (gnum)] = {'wrapnums': ','.join(jobdict['parlist'][modname]['wrapnums']),
                                          'fw_nthread': jobdict['parlist'][modname]['fw_nthread']}
            gnum += 1

    jobwcl = WCL({pfwdefs.PF_JOBNUM: jobdict['jobnum'],
                  'numexpwrap': len(jobdict['tasks']),
                  'save_md5sum': config.getfull('save_md5sum'),
                  'pipeprod': config.getfull('pipeprod'),
                  'pipever': config.getfull('pipever'),
                  'jobkeys': jobkey[1:].replace('_', ','),
                  pfwdefs.SW_ARCHIVESECT: config[pfwdefs.SW_ARCHIVESECT],
                  'output_wcl_tar': jobdict['outputwcltar'],
                  'envfile': jobdict['envfile'],
                  'junktar': config.get_filename('junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
                  'junktar_archive_path': config.get_filepath('ops', 'junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
                  'fw_groups': fwgroups,
                  'verify_files': config.getfull(pfwdefs.PF_VERIFY_FILES),
                  'maxthread_used': config.getfull('maxthread_used'),
                 })

    (_, create_junk_tarball) = config.search(pfwdefs.CREATE_JUNK_TARBALL, {intgdefs.REPLACE_VARS: True})
    jobwcl[pfwdefs.CREATE_JUNK_TARBALL] = miscutils.convertBool(create_junk_tarball)

    if 'transfer_stats' in config:
        jobwcl['transfer_stats'] = config.getfull('transfer_stats')

    # compression
    if pfwdefs.MASTER_COMPRESSION in config:
        jobwcl[pfwdefs.MASTER_COMPRESSION] = config.getfull(pfwdefs.MASTER_COMPRESSION).lower()
    else:
        jobwcl[pfwdefs.MASTER_COMPRESSION] = pfwdefs.MASTER_COMPRESSION_DEFAULT.lower()

    if pfwdefs.COMPRESSION_CLEANUP in config:
        jobwcl[pfwdefs.COMPRESSION_CLEANUP] = config.getfull(pfwdefs.COMPRESSION_CLEANUP)
    else:
        jobwcl[pfwdefs.COMPRESSION_CLEANUP] = pfwdefs.COMPRESSION_CLEANUP_DEFAULT

    if jobwcl[pfwdefs.MASTER_COMPRESSION] != 'never':
        for key in [pfwdefs.COMPRESSION_EXEC,
                    pfwdefs.COMPRESSION_ARGS,
                    pfwdefs.COMPRESSION_SUFFIX,
                    pfwdefs.COMPRESSION_CLEANUP]:
            if key in config:
                jobwcl[key] = config.get(key)


    # copy transfer_semname keys to jobwcl
    for tsemname in ['input_transfer_semname_target',
                     'input_transfer_semname_home',
                     'input_transfer_semname',
                     'output_transfer_semname_target',
                     'output_transfer_semname_home',
                     'output_transfer_semname',
                     'transfer_semname']:
        if tsemname in config:
            jobwcl[tsemname] = config.getfull(tsemname)

    if pfwdefs.MASTER_SAVE_FILE in config:
        jobwcl[pfwdefs.MASTER_SAVE_FILE] = config.getfull(pfwdefs.MASTER_SAVE_FILE)
    else:
        jobwcl[pfwdefs.MASTER_SAVE_FILE] = pfwdefs.MASTER_SAVE_FILE_DEFAULT


    target_archive = init_use_archive_info(config, jobwcl, pfwdefs.USE_TARGET_ARCHIVE_INPUT,
                                           pfwdefs.USE_TARGET_ARCHIVE_OUTPUT, pfwdefs.TARGET_ARCHIVE)
    home_archive = init_use_archive_info(config, jobwcl, pfwdefs.USE_HOME_ARCHIVE_INPUT,
                                         pfwdefs.USE_HOME_ARCHIVE_OUTPUT, pfwdefs.HOME_ARCHIVE)


    # include variables needed by target archive's file mgmt class
    if jobwcl[pfwdefs.TARGET_ARCHIVE] is not None:
        try:
            filemgmt_class = miscutils.dynamically_load_class(config[pfwdefs.SW_ARCHIVESECT][target_archive]['filemgmt'])
            valdict = config.get_param_info(filemgmt_class.requested_config_vals())
            jobwcl.update(valdict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    # include variables needed by home archive's file mgmt class
    if jobwcl[pfwdefs.HOME_ARCHIVE] is not None:
        try:
            filemgmt_class = miscutils.dynamically_load_class(config[pfwdefs.SW_ARCHIVESECT][home_archive]['filemgmt'])
            valdict = config.get_param_info(filemgmt_class.requested_config_vals(),
                                            {pfwdefs.PF_CURRVALS: config[pfwdefs.SW_ARCHIVESECT][home_archive]})
            jobwcl.update(valdict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    try:
        jobwcl['job_file_mvmt'] = config['job_file_mvmt'][config.getfull('curr_site')][home_archive][target_archive]
    except:
        print "\n\n\nError: Problem trying to find: config['job_file_mvmt'][%s][%s][%s]" % (config.getfull('curr_site'), home_archive, target_archive)
        print "USE_HOME_ARCHIVE_INPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_INPUT]
        print "USE_HOME_ARCHIVE_OUTPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT]
        print "site =", config.getfull('curr_site')
        print "home_archive =", home_archive
        print "target_archive =", target_archive
        print 'job_file_mvmt ='
        miscutils.pretty_print_dict(config['job_file_mvmt'])
        print "\n"
        raise

    # include variables needed by job_file_mvmt class
    try:
        jobfilemvmt_class = miscutils.dynamically_load_class(jobwcl['job_file_mvmt']['mvmtclass'])
        valdict = config.get_param_info(jobfilemvmt_class.requested_config_vals(),
                                        {pfwdefs.PF_CURRVALS: jobwcl['job_file_mvmt']})
        jobwcl.update(valdict)
    except Exception as err:
        print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
        raise

    jobwcl['filetype_metadata'] = config['filetype_metadata']
    jobwcl['file_header'] = config['file_header']
    jobwcl['filename_pattern'] = config['filename_pattern']
    jobwcl['directory_pattern'] = config['directory_pattern']
    jobwcl[pfwdefs.IW_EXEC_DEF] = config[pfwdefs.SW_EXEC_DEF]
    #jobwcl['wrapinputs'] = jobdict['wrapinputs']

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("jobwcl.keys() = %s" % jobwcl.keys())

    tjpad = pfwutils.pad_jobnum(jobdict['jobnum'])
    miscutils.coremakedirs(tjpad)
    with open("%s/%s" % (tjpad, jobdict['jobwclfile']), 'w') as wclfh:
        jobwcl.write(wclfh, True, 4)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


#######################################################################
def add_needed_values(config, modname, wrapinst, wrapwcl):
    """ Make sure all variables in the wrapper instance have values in the wcl """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s" % modname)

    # start with those needed by framework
    neededvals = {pfwdefs.PF_JOBNUM: config.getfull(pfwdefs.PF_JOBNUM,
                                                    {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                     'searchobj': wrapinst}),
                  pfwdefs.PF_WRAPNUM: config.getfull(pfwdefs.PF_WRAPNUM,
                                                     {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                      'searchobj': wrapinst}),
                 }

    # start with specified
    if 'req_vals' in config[pfwdefs.SW_MODULESECT][modname]:
        for rval in miscutils.fwsplit(config[pfwdefs.SW_MODULESECT][modname]['req_vals']):
            neededvals[rval] = True

    # go through all values in wcl
    #miscutils.pretty_print_dict(wrapwcl)
    neededvals.update(pfwutils.search_wcl_for_variables(wrapwcl))


    # add neededvals to wcl (values can also contain vars)
    done = False
    count = 0
    maxtries = 1000
    while not done and count < maxtries:
        done = True
        count += 1
        for nval in neededvals.keys():
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("nval = %s" % nval)
            if isinstance(neededvals[nval], bool):
                if ':' in nval:
                    nval = nval.split(':')[0]

                if nval in 'qoutfile':
                    val = nval
                else:
                    try:
                        (found, val) = config.search(nval,
                                                     {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                                      'searchobj': wrapinst,
                                                      'required': False,
                                                      intgdefs.REPLACE_VARS: False})
                    except:
                        print "Why  config.search threw an error"

                    if not found:
                        try:
                            val = pfwutils.get_wcl_value(nval, wrapwcl)
                        except KeyError as err:
                            print "----- Searching for value in wcl:", nval
                            print wrapwcl.write()
                            raise err

                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("val = %s" % val)

                neededvals[nval] = val
                viter = [m.group(1) for m in re.finditer(r'(?i)\$\{([^}]+)\}', str(val))]
                for vstr in viter:
                    if ':' in vstr:
                        vstr = vstr.split(':')[0]
                    if vstr not in neededvals:
                        neededvals[vstr] = True
                        done = False

    if count >= maxtries:
        raise Exception("Error: exceeded maxtries")


    # add needed values to wrapper wcl
    for key, val in neededvals.items():
        pfwutils.set_wcl_value(key, val, wrapwcl)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


#######################################################################
def create_wrapper_inst(config, modname, loopvals):
    """ Create set of empty wrapper instances """

    miscutils.fwdebug_print("BEG %s" % modname)
    wrapperinst = OrderedDict()
    (found, loopkeys) = config.search('wrapperloop',
                                      {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                       'required': False, intgdefs.REPLACE_VARS: True})
    wrapperinst = OrderedDict()
    if found:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("loopkeys = %s" % loopkeys)
        loopkeys = miscutils.fwsplit(loopkeys.lower())
        #loopkeys.sort()  # sort so can make same key easily

        for instvals in sorted(loopvals):
            if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("creating instance for %s" % str(instvals))

            config.inc_wrapnum()
            winst = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM]}

            if len(instvals) != len(loopkeys):
                miscutils.fwdebug_print("Error: invalid number of values for instance")
                miscutils.fwdebug_print("\t%d loopkeys (%s)" % (len(loopkeys), loopkeys))
                miscutils.fwdebug_print("\t%d instvals (%s)" % (len(instvals), instvals))
                raise IndexError("Invalid number of values for instance")

            try:
                instkey = ""
                for k, key in enumerate(loopkeys):
                    winst[key] = instvals[k]
                    instkey += '_' + instvals[k]
            except:
                miscutils.fwdebug_print("Error: problem trying to create wrapper instance")
                miscutils.fwdebug_print("\tWas creating instance for %s" % str(instvals))
                miscutils.fwdebug_print("\tloopkeys = %s" % loopkeys)
                raise

            winst['wrapkeys'] = instkey
            wrapperinst[instkey] = winst
    else:
        config.inc_wrapnum()
        wrapperinst['noloop'] = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM],
                                 'wrapkeys': 'noloop'}

    miscutils.fwdebug_print("Number wrapper inst: %s" % len(wrapperinst))
    if wrapperinst:
        miscutils.fwdebug_print("Error: 0 wrapper inst")
        raise Exception("Error: 0 wrapper instances")

    miscutils.fwdebug_print("END\n\n")
    return wrapperinst



#####################################################################
def create_new_filename(config, fsectname, fsectdict, sobj, currvals):
    """ doc """
    miscutils.fwdebug_print("BEG")

    new_sobj = copy.deepcopy(fsectdict)
    new_sobj.update(sobj)

    # see if wcl specifies filename directly
    if 'filename' in fsectdict:
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("filename in %s" % fsectname)

        filename = config.get('filename', {pfwdefs.PF_CURRVALS: currvals,
                                           'searchobj': sobj,
                                           'expand': False,
                                           'required': True,
                                           intgdefs.REPLACE_VARS:False})

        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("filename = %s" % filename)
    else:
        # create filename from pattern
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("creating filename for %s" % fsectname)
            miscutils.fwdebug_print("\tfsectdict = %s" % fsectdict)
            miscutils.fwdebug_print("\tsobj = %s" % sobj)
            miscutils.fwdebug_print("\tnews_obj = %s" % new_sobj)

        filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
                                              'searchobj': new_sobj,
                                              'expand': False,
                                              intgdefs.REPLACE_VARS:False})

    fileinfo = replfuncs.replace_vars(filename, config,
                                      {pfwdefs.PF_CURRVALS: currvals,
                                       'searchobj': new_sobj,
                                       'expand': True,
                                       intgdefs.REPLACE_VARS:True,
                                       'keepvars': True})
    if fileinfo is None:
        miscutils.fwdie('empty fileinfo %s' % (fsectname), pfwdefs.PF_EXIT_FAILURE)

    # save file info as if we read from query
    fnames = fileinfo[0]
    filelist = []
    if isinstance(fnames, list):
        for i, fname in enumerate(fnames):
            finfo = fileinfo[1][i]
            finfo['filename'] = fname
            filelist.append(finfo)
    else:
        finfo = fileinfo[1]
        finfo['filename'] = fnames
        filelist.append(finfo)

    return filelist


#####################################################################
def create_new_depends_filenames(config, master, modname, flabel):
    """ Create new filenames for output files that depended upon input data """

    miscutils.fwdebug_print("BEG %s %s" % (modname, flabel))

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    currvals = {'curr_module': modname}
    fsectdict = moddict[pfwdefs.SW_FILESECT][flabel]

    for _, ldict in master['list'][intgdefs.LISTENTRY].items():
        for fnickname in ldict['file'].keys():
            newfinfo = copy.deepcopy(ldict['file'][fnickname])
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("fnickname=%s, newfinfo=%s" % (fnickname, newfinfo))

            if 'filename' in newfinfo:
                del newfinfo['filename']
                if 'compression' in newfinfo:
                    del newfinfo['compression']
                if 'fullname' in newfinfo:
                    del newfinfo['fullname']

                sobj = copy.deepcopy(newfinfo)
                sobj.update(fsectdict)

                filelist = create_new_filename(config, flabel, fsectdict, sobj, currvals)
                #print type(filelist), filelist
                if len(filelist) == 1:
                    ###newfinfo = filelist[0]
                    newfinfo.update(filelist[0])
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("fnickname=%s, newfinfo=%s" % (fnickname, newfinfo))
                ldict['file'][fnickname] = newfinfo

    miscutils.fwdebug_print("END\n\n")



#####################################################################
def fix_master_lists(config, modname, masterdata):
    """ Replace filename for master data copied as depend for output file """

    miscutils.fwdebug_print("BEG %s" % modname)

    # create python list of files and lists for this module
    searchobj = config.combine_lists_files(modname)

    for (sname, sdict) in searchobj:
        #miscutils.fwdebug_print("sname=%s" % sname)
        if 'depends-newname' in sdict:   # depends
            miscutils.fwdebug_print("need to fix filenames %s" % sname)
            master = masterdata[modname][sname]
            checksect = sname
            if checksect.startswith(pfwdefs.SW_LISTSECT):
                columns = get_list_all_columns(sdict, False)
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("columns=%s" % columns)

                for collist in columns:
                    for col in collist:
                        match = re.search(r"(\S+).fullname", col)
                        if match:
                            flabel = match.group(1)
                            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                                miscutils.fwdebug_print("flabel=%s" % flabel)
                            create_new_depends_filenames(config, master, modname, flabel)
                        else:
                            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                                miscutils.fwdebug_print("skipping column %s since not file name" % col)
            else:  # file
                #miscutils.fwdebug_print("sname=%s" % sname)
                match = re.search(r"%s-(\S+)" % pfwdefs.SW_FILESECT, sname)
                if match:
                    flabel = match.group(1)
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("flabel=%s" % flabel)
                    create_new_depends_filenames(config, master, modname, flabel)
                else:
                    raise KeyError("Bad file section name %s" % sname)

            #with open('%s_%s_fix.list' % (modname, sname), 'w') as fh:
            #    miscutils.pretty_print_dict(master, fh)

    #sys.exit(1)
    miscutils.fwdebug_print("END\n\n")



#####################################################################
def read_master_lists(config, modname, masterdata):
    """ Read master lists and files from files created earlier """
    miscutils.fwdebug_print("BEG %s" % modname)

    # create python list of files and lists for this module
    searchobj = config.combine_lists_files(modname)

    #print "read master list order:  ", searchobj

    for (sname, sdict) in searchobj:
        #print sname
        # get filename for file containing dataset
        if 'qoutfile' in sdict:
            qoutfile = sdict['qoutfile']
            print "\t\t%s: reading master dataset from %s" % (sname, qoutfile)

            qouttype = intgdefs.DEFAULT_QUERY_OUTPUT_FORMAT
            if 'qouttype' in sdict:
                qouttype = sdict['qouttype']

            # read dataset file
            starttime = time.time()
            print "\t\t\tReading file - start ", starttime
            if qouttype == 'json':
                master = None
                with open(qoutfile, 'r') as jsonfh:
                    master = json.load(jsonfh)
            elif qouttype == 'xml':
                raise Exception("xml datasets not supported yet")
            elif qouttype == 'wcl':
                master = WCL()
                with open(qoutfile, 'r') as wclfh:
                    master.read(wclfh, filename=qoutfile)
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("master.keys() = " % master.keys())
            else:
                raise Exception("Unsupported dataset format in qoutfile for object %s in module %s (%s) " % (sname, modname, qoutfile))
            endtime = time.time()
            print "\t\t\tReading file - end ", endtime
            print "\t\t\tReading file took %s seconds" % (endtime - starttime)

            numlines = len(master['list'][intgdefs.LISTENTRY])
            print "\t\t\tNumber of lines in dataset %s: %s\n" % (sname, numlines)

            if numlines == 0:
                raise Exception("ERROR: 0 lines in dataset %s in module %s" % (sname, modname))

            #sdict['master'] = master
            if modname not in masterdata:
                masterdata[modname] = OrderedDict()
            masterdata[modname][sname] = master
        elif pfwdefs.DATA_DEPENDS in sdict or 'depends-newname' in sdict:   # depends
            # = modname.filesect.filelabel
            # = modname.listsect.listlabel.filelabel

            tempdict = {}
            if modname not in masterdata:
                masterdata[modname] = OrderedDict()

            print "\t\t%s-%s: depends in sdict" % (modname, sname)
            deplist = []
            if pfwdefs.DATA_DEPENDS in sdict:
                deplist = sdict[pfwdefs.DATA_DEPENDS].lower().split(",")
            else:
                deplist = sdict['depends-newname'].lower().split(",")
            count = 1
            for dep in deplist:
                depends = None
                depends = miscutils.fwsplit(dep, '.')

                dkey = "%s-%s" % (depends[1], depends[2])
                if depends[0] in masterdata and dkey in masterdata[depends[0]]:
                    #print '\nDD ',depends[0],dkey
                    #print '     ',depends
                    if len(depends) == 3:
                        #print '   NONE'
                        tempdict.update(copy_master(masterdata[depends[0]][dkey], None, count))
                    else:
                        #print '   ',depends[3]
                        tempdict.update(copy_master(masterdata[depends[0]][dkey], depends[3], count))
                    count = len(tempdict) + 1
                    #print 'END\n\n\n\n'
                    #print sname, masterdata[modname][sname]
                    #with open('%s_%s_read.list' % (modname, sname), 'w') as fh:
                    #    miscutils.pretty_print_dict(masterdata[modname][sname], fh)
                else:
                    print "Error.  Debugging info:"
                    print 'modname = ', modname
                    print 'sname = ', sname
                    print 'depends =', depends
                    print 'dkey =', dkey
                    print 'masterdata keys=', masterdata.keys()
                    if depends[0] in masterdata:
                        print 'masterdata[%s].keys()=%s' % (depends[0], masterdata[depends[0]].keys())
                    miscutils.fwdie("ERROR: Could not find data for depends", pfwdefs.PF_EXIT_FAILURE)
            masterdata[modname][sname] = {'list': {intgdefs.LISTENTRY: tempdict}}
            #print "\n\nLENGTH ",len(tempdict)

    miscutils.fwdebug_print("END\n\n")


#######################################################################
def remove_column_format(columns):
    """ Return columns minus any formatting specification """

    columns2 = []
    for col in columns:
        if col.startswith('$FMT{'):
            rmatch = re.match(r'\$FMT\{\s*([^,]+)\s*,\s*(\S+)\s*\}', col)
            if rmatch:
                columns2.append(rmatch.group(2).strip())
            else:
                miscutils.fwdie("Error: invalid FMT column: %s" % (col), pfwdefs.PF_EXIT_FAILURE)
        else:
            columns2.append(col)
    return columns2


#######################################################################
def convert_col_string_to_list(colstr, with_format=True):
    """ Convert a column string to list of columns """
    columns = re.findall(r'\$\S+\{.*\}|[^,\s]+', colstr)

    if not with_format:
        columns = remove_column_format(columns)
    return columns


#######################################################################
def get_list_all_columns(ldict, with_format=True):
    """ For a list definition, return list of columns in all list files """
    columns = []
    if pfwdefs.DIV_LIST_BY_COL in ldict:
        for divcoldict in ldict[pfwdefs.DIV_LIST_BY_COL].values():
            columns.append(convert_col_string_to_list(divcoldict['columns'], with_format))
    elif 'columns' in ldict:
        columns.append(convert_col_string_to_list(ldict['columns'], with_format))
    else:
        miscutils.fwdebug_print("columns not in ldict, so defaulting to fullname")
        columns.append(['fullname'])

    #print "get_list_all_columns: columns=", columns
    return columns



#######################################################################
def create_fullnames(config, modname, masterdata):
    """ add paths to filenames """    # what about compression extension

    miscutils.fwdebug_print("BEG %s" % modname)
    dataset = config.combine_lists_files(modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    for (sname, sdict) in dataset:
        if modname in masterdata and sname in masterdata[modname]:
            master = masterdata[modname][sname]
            numlines = len(master['list'][intgdefs.LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                miscutils.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)


            if pfwdefs.DIV_LIST_BY_COL in sdict or 'columns' in sdict:  # list
                miscutils.fwdebug_print("list sect: sname=%s" % sname)
                dictcurr = OrderedDict()
                columns = get_list_all_columns(sdict, False)
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("columns=%s" % columns)

                for collist in columns:
                    for col in collist:
                        match = re.search(r"(\S+).fullname", col)
                        if match:
                            flabel = match.group(1)
                            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                                miscutils.fwdebug_print("flabel=%s" % flabel)
                            if flabel in moddict[pfwdefs.SW_FILESECT]:
                                dictcurr[flabel] = copy.deepcopy(moddict[pfwdefs.SW_FILESECT][flabel])
                                dictcurr[flabel]['curr_module'] = modname
                            else:
                                print "list files = ", moddict[pfwdefs.SW_FILESECT].keys()
                                miscutils.fwdie("Error: Looking at list columns - could not find %s def in dataset" % flabel, pfwdefs.PF_EXIT_FAILURE)
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("dictcurr=%s" % dictcurr)

                for llabel, ldict in master['list'][intgdefs.LISTENTRY].items():
                    for flabel, fdict in ldict['file'].items():
                        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("flabel=%s, fdict=%s" % (flabel, fdict))
                        if 'fullname' not in fdict:
                            if flabel in dictcurr:
                                fdict['fullname'] = add_runtime_path(config, dictcurr[flabel],
                                                                     flabel, fdict,
                                                                     fdict['filename'])[0]
                            elif len(dictcurr) == 1:
                                fdict['fullname'] = add_runtime_path(config, dictcurr.values()[0],
                                                                     flabel, fdict,
                                                                     fdict['filename'])[0]
                            else:
                                print "dictcurr: ", dictcurr.keys()
                                miscutils.fwdie("Error: Looking at lines - could not find %s def in dictcurr" % flabel, pfwdefs.PF_EXIT_FAILURE)
                        elif miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("fullname already in fdict: flabel=%s" % flabel)


            else:  # file
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("file sect: sname=%s" % sname)
                currvals = copy.deepcopy(sdict)
                currvals['curr_module'] = modname

                for llabel, ldict in master['list'][intgdefs.LISTENTRY].items():
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("file sect: llabel=%s" % llabel)
                    for flabel, fdict in ldict['file'].items():
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("file sect: flabel=%s" % flabel)
                        if miscutils.fwdebug_check(10, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("fdict: fdict=%s" % fdict)
                        fdict['fullname'] = add_runtime_path(config, currvals, flabel,
                                                             fdict, fdict['filename'])[0]
        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    miscutils.fwdebug_print("END\n\n")



#######################################################################
def create_sublists(config, modname, masterdata):
    """ break master lists into sublists based upon match or divide_by """
    miscutils.fwdebug_print("BEG %s" % modname)
    dataset = config.combine_lists_files(modname)

    sublists = OrderedDict()
    for (sname, sdict) in dataset:
        if modname in masterdata and sname in masterdata[modname]:
            master = masterdata[modname][sname]
            numlines = len(master['list'][intgdefs.LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                miscutils.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)

            sublists[sname] = OrderedDict()
            keys = get_match_keys(sdict)

            if keys:
                sdict['keyvals'] = OrderedDict()
                print "\t%s-%s: dividing by %s" % (modname, sname, keys)
                for linenick, linedict in master['list'][intgdefs.LISTENTRY].items():
                    index = ""
                    listkeys = []
                    for key in keys:
                        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("key = %s" % key)
                            miscutils.fwdebug_print("linedict = %s" % linedict)
                        val = get_value_from_line(linedict, key, None, 1)
                        index += val + '_'
                        listkeys.append(val)
                    sdict['keyvals'][index] = listkeys
                    if index not in sublists[sname]:
                        sublists[sname][index] = {'list': {intgdefs.LISTENTRY: OrderedDict()}}
                    sublists[sname][index]['list'][intgdefs.LISTENTRY][linenick] = copy.deepcopy(linedict)
                    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("index = %s" % index)
                        miscutils.fwdebug_print("listkeys = %s" % listkeys)

            else:
                sublists[sname]['onlyone'] = copy.deepcopy(master)

        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    miscutils.fwdebug_print("END\n\n")
    return sublists


#######################################################################
def get_wrap_iter_obj_key(moddict):
    """ get wrapper iter object key """
    iter_obj_key = None
    if 'loopobj' in moddict:
        iter_obj_key = moddict['loopobj'].lower()
    else:
        miscutils.fwdebug_print("Could not find loopobj in modict %s" % moddict)
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Could not find loopobj. moddict keys = %s" % moddict.keys())
    return iter_obj_key


#######################################################################
def get_wrapper_loopvals(config, modname):
    """ get the values for the wrapper loop keys """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s" % modname)

    loopvals = []

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    (found, loopkeys) = config.search('wrapperloop',
                                      {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                       'required': False, intgdefs.REPLACE_VARS: True})
    if found:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("\tloopkeys = %s" % loopkeys)
        loopkeys = miscutils.fwsplit(loopkeys.lower())
        #loopkeys.sort()  # sort so can make same key easily


        ## determine which list/file would determine loop values
        iter_obj_key = get_wrap_iter_obj_key(moddict)
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("iter_obj_key=%s" % iter_obj_key)

        ## get wrapper loop values
        if iter_obj_key is not None:
            loopdict = pfwutils.get_wcl_value(iter_obj_key, moddict)
            ## check if loopobj has info from query
            if 'keyvals' in loopdict:
                loopvals = loopdict['keyvals'].values()
            else:
                miscutils.fwdebug_print("Warning: Couldn't find keyvals for loopobj %s" % moddict['loopobj'])
                if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("iter_obj_key=%s" % iter_obj_key)
                    miscutils.fwdebug_print("moddict=%s" % moddict)


        if loopvals:
            print "\tDefaulting to wcl values"
            loopvals = []
            for key in loopkeys:
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("key=%s" % key)
                (found, val) = config.search(key,
                                             {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                              'required': False,
                                              intgdefs.REPLACE_VARS: True})
                if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("found=%s" % found)
                if found:
                    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("val=%s" % val)
                    val = miscutils.fwsplit(val)
                    loopvals.append(val)
            loopvals = itertools.product(*loopvals)

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return loopvals


#############################################################
def get_value_from_line(line, key, nickname=None, numvals=None):
    """ Return value from a line in master list """
    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG: key = %s, nickname = %s, numvals = %s" % (key, nickname, numvals))

    # since values could be repeated across files in line,
    # create hash of values to get unique values
    valhash = OrderedDict()

    key = key.lower()

    if '.' in key:
        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Found nickname")
        (nickname, key) = key.split('.')

    # is value defined at line level?
    if key in line:
        valhash[line[key]] = True

    # check files
    if 'file' in line:
        if nickname is not None:
            if nickname in line['file'] and key in line['file'][nickname]:
                try:
                    valhash[line['file'][nickname][key]] = True
                except:
                    miscutils.fwdebug_print("ERROR")
                    miscutils.fwdebug_print("valhash=%s" % valhash)
                    miscutils.fwdebug_print("line['file'][%s]=%s" % (nickname, line['file'][nickname]))
                    miscutils.fwdebug_print("line['file'][%s][%s]=%s" % (nickname, key, line['file'][nickname][key]))
                    miscutils.fwdebug_print("type(x)=%s" % (type(line['file'][nickname][key])))
                    raise
        else:
            for _, fdict in line['file'].items():
                if key in fdict:
                    valhash[fdict[key]] = True

    valarr = valhash.keys()

    if numvals is not None and len(valarr) != numvals:
        miscutils.fwdebug_print("Error: in get_value_from_line:")
        print "\tnumber found (%s) doesn't match requested (%s)\n" % (len(valarr), numvals)
        if nickname is not None:
            print "\tnickname =", nickname

        print "\tvalue to find:", key
        print "\tline:",
        miscutils.pretty_print_dict(line)
        print "\tvalarr:", valarr
        miscutils.fwdie("Error: number found (%s) doesn't match requested (%s)" % \
                        (len(valarr), numvals), pfwdefs.PF_EXIT_FAILURE)

    if not valarr:
        retval = None
    elif numvals == 1 or len(valarr) == 1:
        retval = str(valarr[0])
    else:
        retval = str(valarr)

    if hasattr(retval, "strip"):
        retval = retval.strip()

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return retval

#######################################################################
def get_wcl_metadata_keys(filetype, config):
    """ Add to wrapper wcl any file metadata wcl values """

    wclkeys = set()
    for _, hdict in config['filetype_metadata'][filetype]['hdus'].items():
        for _, sdict in hdict.items():
            if 'w' in sdict:
                wclkeys.update(set(sdict['w'].keys()))

    return wclkeys

#######################################################################
def get_filetypes_output_files(moddict, outputfiles):
    """ Get the filetypes for all the output files """

    filetypes = []
    filesect = moddict[pfwdefs.SW_FILESECT]
    for ofile in outputfiles:
        ofsectkeys = ofile.split('.')
        ofsect = ofsectkeys[-1].lower()
        try:
            filetypes.append(filesect[ofsect]['filetype'])
        except:
            print 'ofile =', ofile
            print 'ofsect =', ofsect
            print "filesect.keys() = ", filesect.keys()
            raise


    return filetypes

#######################################################################
# Assumes currvals includes specific values (e.g., band, ccd)
def create_single_wrapper_wcl(config, modname, wrapinst):
    """ create single wrapper wcl """
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s %s" % (modname, wrapinst[pfwdefs.PF_WRAPNUM]))
    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("\twrapinst=%s" % wrapinst)
    files = {"infiles": [],
             "outfiles": []}

    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: wrapinst[pfwdefs.PF_WRAPNUM]}
    wrapperwcl = WCL({'modname': modname,
                      'wrapkeys': wrapinst['wrapkeys']})
    outfiles = []
    outlists = []
    infiles = []
    inlists = []
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    execs = intgmisc.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)
    for execkey, execval in execs.iteritems():
        if pfwdefs.IW_INPUTS in execval.keys():
            temp = replfuncs.replace_vars_single(execval[pfwdefs.IW_INPUTS], config,
                                                 {pfwdefs.PF_CURRVALS: currvals,
                                                  'searchobj': execval[pfwdefs.IW_INPUTS],
                                                  'required': True,
                                                  intgdefs.REPLACE_VARS: True})
            temp = temp.replace(' ', '')
            temp = temp.split(',')
            for item in temp:
                vals = item.split('.')
                if vals[0] == pfwdefs.SW_FILESECT:
                    infiles.append(vals[1])
                elif vals[0] == pfwdefs.SW_LISTSECT:
                    inlists.append(vals[1])

        if pfwdefs.IW_OUTPUTS in execval.keys():
            temp = replfuncs.replace_vars_single(execval[pfwdefs.IW_OUTPUTS], config,
                                                 {pfwdefs.PF_CURRVALS: currvals,
                                                  'searchobj': execval[pfwdefs.IW_OUTPUTS],
                                                  'required': True,
                                                  intgdefs.REPLACE_VARS: True})
            temp = temp.replace(' ', '')
            temp = temp.split(',')
            for item in temp:
                vals = item.split('.')
                if vals[0] == pfwdefs.SW_FILESECT:
                    outfiles.append(vals[1])
                elif vals[0] == pfwdefs.SW_LISTSECT:
                    outlists.append(vals[1])

    # file is optional
    if pfwdefs.IW_FILESECT in wrapinst:
        wrapperwcl[pfwdefs.IW_FILESECT] = copy.deepcopy(wrapinst[pfwdefs.IW_FILESECT])
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("\tfile=%s" % wrapperwcl[pfwdefs.IW_FILESECT])
        for (sectname, sectdict) in wrapperwcl[pfwdefs.IW_FILESECT].items():
            sectdict['sectname'] = sectname
            isanoutput = False
            isaninput = False
            if sectname in outfiles:
                isanoutput = True
            if sectname in infiles:
                isaninput = True
            if 'fullname' in sectdict:
                if isanoutput:
                    files['outfiles'] += sectdict['fullname'].split(',')
                elif isaninput:
                    files['infiles'] += sectdict['fullname'].split(',')
            elif 'listonly' in sectdict and sectdict['listonly'] == 'True':
                pass
            else:
                print "MISSING", sectdict.items()

    # list is optional
    if pfwdefs.IW_LISTSECT in wrapinst:
        wrapperwcl[pfwdefs.IW_LISTSECT] = copy.deepcopy(wrapinst[pfwdefs.IW_LISTSECT])
        for k, v in wrapperwcl[pfwdefs.IW_LISTSECT].iteritems():
            isoutlist = False
            isinlist = False
            if k in outlists:
                isoutlist = True
            elif k in inlists:
                isinlist = True

            if os.path.isfile(v['fullname']):
                cols = v['columns'].split(',')
                cc = -1
                for num, col in enumerate(cols):
                    if 'fullname' in col:
                        cc = num
                        break
                if cc != -1:
                    fl = open(v['fullname'], 'r')
                    rl = fl.readlines()
                    for line in rl:
                        temp = line.split()[cc]
                        temp = temp.replace(',', '')
                        if isoutlist:
                            files['outfiles'].append(temp.split('[')[0])
                        elif isinlist:
                            files['infiles'].append(temp.split('[')[0])

        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("\tlist=%s" % wrapperwcl[pfwdefs.IW_LISTSECT])

    for typ in ['outfiles', 'infiles']:
        for num, ff in enumerate(files[typ]):
            # drop any direstory structure
            files[typ][num] = ff.split('/')[-1]

    # do we want exec_list variable?
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("\tpfwdefs.SW_EXECPREFIX=%s" % pfwdefs.SW_EXECPREFIX)
    numexec = 0
    modname = currvals['curr_module']
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = intgmisc.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)
    for execkey in execs:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Working on exec section (%s)"% execkey)
        numexec += 1
        iwkey = execkey.replace(pfwdefs.SW_EXECPREFIX, pfwdefs.IW_EXECPREFIX)
        wrapperwcl[iwkey] = OrderedDict()
        execsect = moddict[execkey]
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("\t\t(%s)" % (execsect))

        # get filetypes for adding wcl metadata to wrapper input wcl
        if pfwdefs.SW_OUTPUTS in execsect:
            filetypes = get_filetypes_output_files(moddict, miscutils.fwsplit(execsect[pfwdefs.OW_OUTPUTS]))
            wclkeys = set()   # set to eliminate duplicates
            for ftype in filetypes:
                wclkeys.update(get_wcl_metadata_keys(ftype, config))

            for wkey in list(wclkeys):
                if wkey not in wrapperwcl:
                    wrapperwcl[wkey] = config.getfull(wkey,
                                                      {pfwdefs.PF_CURRVALS: currvals,
                                                       'searchobj': wrapinst})

        for key, val in execsect.items():
            if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("\t\t%s (%s)" % (key, val))
            if key == pfwdefs.SW_INPUTS:
                iwexkey = pfwdefs.IW_INPUTS
            elif key == pfwdefs.SW_OUTPUTS:
                iwexkey = pfwdefs.IW_OUTPUTS
            elif key == pfwdefs.SW_ANCESTRY:
                iwexkey = pfwdefs.IW_ANCESTRY
            else:
                iwexkey = key

            if key != 'cmdline':
                wrapperwcl[iwkey][iwexkey] = replfuncs.replace_vars_single(val, config,
                                                                           {pfwdefs.PF_CURRVALS: currvals,
                                                                            'searchobj': val,
                                                                            'required': True,
                                                                            intgdefs.REPLACE_VARS: True})
            else:
                wrapperwcl[iwkey]['cmdline'] = copy.deepcopy(val)
        if 'execnum' not in wrapperwcl[execkey]:
            result = re.match(r'%s(\d+)' % pfwdefs.IW_EXECPREFIX, execkey)
            if not result:
                miscutils.fwdie('Error:  Could not determine execnum from exec label %s' % execkey, pfwdefs.PF_EXIT_FAILURE)
            wrapperwcl[execkey]['execnum'] = result.group(1)

        execname = wrapperwcl[iwkey]['execname']
        if intgdefs.IW_EXEC_DEF in config:
            execdefs = config[intgdefs.IW_EXEC_DEF]
            if (execname.lower() in execdefs and
                    'version_flag' in execdefs[execname.lower()] and
                    'version_pattern' in execdefs[execname.lower()]):
                wrapperwcl[iwkey]['version_flag'] = execdefs[execname.lower()]['version_flag']
                wrapperwcl[iwkey]['version_pattern'] = execdefs[execname.lower()]['version_pattern']
            else:
                miscutils.fwdebug_print("Info:  Missing version keys for %s" % (execname))

        else:
            print "why %s" % intgdefs.IW_EXEC_DEF

    if pfwdefs.SW_WRAPSECT in config[pfwdefs.SW_MODULESECT][modname]:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("Copying wrapper section (%s)"% pfwdefs.SW_WRAPSECT)
        wrapperwcl[pfwdefs.IW_WRAPSECT] = copy.deepcopy(config[pfwdefs.SW_MODULESECT][modname][pfwdefs.SW_WRAPSECT])

    if pfwdefs.IW_WRAPSECT not in wrapperwcl:
        if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
            miscutils.fwdebug_print("%s (%s): Initializing wrapper section (%s)"% (modname, wrapinst[pfwdefs.PF_WRAPNUM], pfwdefs.IW_WRAPSECT))
        wrapperwcl[pfwdefs.IW_WRAPSECT] = OrderedDict()
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipeline'] = config.getfull('pipeline')
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipeprod'] = config.getfull('pipeprod')
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipever'] = config.getfull('pipever')

    wrapperwcl[pfwdefs.IW_WRAPSECT]['wrappername'] = wrapinst['wrappername']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['outputwcl'] = wrapinst['outputwcl']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['tmpfile_prefix'] = config.getfull('tmpfile_prefix', {pfwdefs.PF_CURRVALS: currvals})
    wrapperwcl['log'] = wrapinst['log']
    wrapperwcl['log_archive_path'] = wrapinst['log_archive_path']

    if numexec == 0:
        miscutils.pretty_print_dict(config[pfwdefs.SW_MODULESECT][modname])
        raise Exception("Error:  Could not find an exec section for module %s" % modname)


    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

    return wrapperwcl, files


# translate sw terms to iw terms in values if needed
def translate_sw_iw(wrapperwcl, modname, winst):
    """ Translate submit wcl keys to input wcl keys """

    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s" % modname)
    if miscutils.fwdebug_check(9, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("winst = %s" % winst.keys())
    if miscutils.fwdebug_check(9, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("wrapperwcl = %s" % wrapperwcl.keys())

    if (pfwdefs.SW_FILESECT == pfwdefs.IW_FILESECT and
            pfwdefs.SW_LISTSECT == pfwdefs.IW_LISTSECT):
        print "Skipping translation SW to IW"
    else:
        translation = [(pfwdefs.SW_FILESECT, pfwdefs.IW_FILESECT),
                       (pfwdefs.SW_LISTSECT, pfwdefs.IW_LISTSECT)]
        wcltodo = [wrapperwcl]
        while wcltodo:
            if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                miscutils.fwdebug_print("len(wcltodo) = %s" % (len(wcltodo)))
            wcl = wcltodo.pop()
            for key, val in wcl.items():
                if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                    miscutils.fwdebug_print("key = %s" % (key))
                if isinstance(val, dict):
                    if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("append key = %s (%s)" % (key, val.keys()))
                    wcltodo.append(val)
                elif isinstance(val, str):
                    if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("val = %s, %s" % (val, type(val)))
                    for (swkey, iwkey) in translation:
                        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("\tbefore swkey = %s, iwkey = %s, val = %s" % (swkey, iwkey, val))
                        val = re.sub(r'^%s\.' % swkey, '%s.' % iwkey, val)
                        val = val.replace(r'{%s.' % swkey, '{%s.' % iwkey)
                        val = val.replace(r' %s.' % swkey, ' %s.' % iwkey)
                        val = val.replace(r',%s.' % swkey, ',%s.' % iwkey)
                        val = val.replace(r':%s.' % swkey, ':%s.' % iwkey)

                        if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
                            miscutils.fwdebug_print("\tafter val = %s" % (val))
                    if miscutils.fwdebug_check(4, "PFWBLOCK_DEBUG"):
                        miscutils.fwdebug_print("final value = %s" % (val))
                    wcl[key] = val

    #print "new wcl = ", wrapperwcl.write(sys.stdout, True, 4)
    if miscutils.fwdebug_check(3, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")



#######################################################################
def create_module_wrapper_wcl(config, modname, winst):
    """ Create wcl for wrapper instances for a module """
    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG %s" % modname)

    if modname not in config[pfwdefs.SW_MODULESECT]:
        raise Exception("Error: Could not find module description for module %s\n" % (modname))

    wrapperwcl, files = create_single_wrapper_wcl(config, modname, winst)
    translate_sw_iw(wrapperwcl, modname, winst)
    add_needed_values(config, modname, winst, wrapperwcl)
    write_wrapper_wcl(winst['inputwcl'], wrapperwcl)

    (exists, val) = config.search(pfwdefs.SW_WRAPPER_DEBUG,
                                  {pfwdefs.PF_CURRVALS: {'curr_module': modname}})
    if exists:
        winst['wrapdebug'] = val
    else:
        winst['wrapdebug'] = 0

    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

    return files

#######################################################################
def divide_into_jobs(config, modname, winst, joblist):
    """ Divide wrapper instances into jobs """
    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("BEG")

    if pfwdefs.SW_DIVIDE_JOBS_BY not in config and len(joblist) > 1:
        miscutils.fwdie("Error: no %s in config, but already > 1 job" % pfwdefs.SW_DIVIDE_JOBS_BY, pfwdefs.PF_EXIT_FAILURE)

    key = '_nokey'
    if pfwdefs.SW_DIVIDE_JOBS_BY in config:
        key = ""
        for divb in miscutils.fwsplit(config.getfull(pfwdefs.SW_DIVIDE_JOBS_BY, {pfwdefs.PF_CURRVALS: {'curr_module':modname}, 'searchobj': winst}), ','):
            key += '_' + config.getfull(divb, {pfwdefs.PF_CURRVALS: {'curr_module':modname}, 'searchobj': winst})


    if key not in joblist:
        #joblist[key] = {'tasks':[], 'inwcl':[], 'inlist':[], 'wrapinputs':OrderedDict(), 'parlist':{}}
        joblist[key] = {'tasks':[], 'inwcl':[], 'inlist':[], 'parlist':{}}

    maxthread = pfwdefs.MAX_FWTHREADS_DEFAULT

    if modname not in joblist[key]['parlist']:
        joblist[key]['parlist'][modname] = {'wrapnums': [], 'fw_nthread': pfwdefs.MAX_FWTHREADS_DEFAULT}

        # check whether supposed to use FW multithreading  (check master on/off switch)
        usefwthreads = pfwdefs.MASTER_USE_FWTHREADS_DEFAULT
        if pfwdefs.MASTER_USE_FWTHREADS in config:
            usefwthreads = miscutils.convertBool(config.getfull('MASTER_USE_FWTHREADS'))

        # determine the number of fw threads for this module
        if usefwthreads:
            global_max_thread = config.getfull('fw_nmaxthread', default=maxthread)
            try:
                mthread = int(config.getfull(pfwdefs.MAX_FWTHREADS, {pfwdefs.PF_CURRVALS: {'curr_module': modname}}, default=1))
                if mthread is None:
                    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                        miscutils.fwdebug_print("%s not found for module %s, defaulting to %s" % (pfwdefs.MAX_FWTHREADS, modname, pfwdefs.MAX_FWTHREADS_DEFAULT))
                else:
                    maxthread = max(mthread, global_max_thread)
            except KeyError:
                if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                    miscutils.fwdebug_print("%s not found for module %s, defaulting to %s" % (pfwdefs.MAX_FWTHREADS, modname, pfwdefs.MAX_FWTHREADS_DEFAULT))
        joblist[key]['parlist'][modname]['fw_nthread'] = maxthread

    joblist[key]['parlist'][modname]['wrapnums'].append(winst[pfwdefs.PF_WRAPNUM])

    joblist[key]['tasks'].append([winst[pfwdefs.PF_WRAPNUM], winst['wrappername'], winst['inputwcl'], winst['wrapdebug'], winst['log']])
    joblist[key]['inwcl'].append(winst['inputwcl'])
    #if winst['wrapinputs'] is not None and len(winst['wrapinputs']) > 0:
    #    joblist[key]['wrapinputs'][winst[pfwdefs.PF_WRAPNUM]] = winst['wrapinputs']
    if pfwdefs.IW_LISTSECT in winst:
        for linfo in winst[pfwdefs.IW_LISTSECT].values():
            joblist[key]['inlist'].append(linfo['fullname'])

    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("number of job lists = %s " % len(joblist.keys()))
        miscutils.fwdebug_print("\tkeys = %s " % ', '.join(joblist.keys()))
        miscutils.fwdebug_print("END\n")
    return maxthread


def write_runjob_script(config):
    """ Write runjob script """

    miscutils.fwdebug_print("BEG")

    jobdir = config.get_filepath('runtime', 'jobdir', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:"$padjnum"}})
    print "The target jobdir =", jobdir

    scriptfile = config.get_filename('runjob')

    #      Since wcl's variable syntax matches shell variable syntax and
    #      underscores are used to separate name parts, have to use place
    #      holder for jobnum and replace later with shell variable
    #      Otherwise, get_filename fails to substitute for padjnum
    envfile = config.get_filename('envfile', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:"9999"}})
    envfile = envfile.replace("j9999", "j${padjnum}")

    scriptstr = """#!/usr/bin/env sh
echo "PFW: job_shell_script cmd: $0 $@";
if [ $# -ne 4 ]; then
    echo "Usage: $0 <input tar> <job wcl> <tasklist> <env file>";
    echo "PFW: job_shell_script exit_status: 1"
    exit 1;
fi

intar=$2
jobwcl=$3
tasklist=$4
envfile=$5
initdir=`pwd`

export SHELL=/bin/bash    # needed for setup to work in Condor environment
export PFW_JOB_START_EPOCH=`date "+%%s"`
echo "PFW: job_shell_script starttime: $PFW_JOB_START_EPOCH"
echo -n "PFW: job_shell_script exechost: "
hostname
echo ""

d1=`date "+%%s"`

jobdir=%(full_job_dir)s
echo "Making target job's directory ($jobdir)"
if [ -e $jobdir ]; then
    echo "Job scratch directory already exists ($jobdir).   Aborting";
    exit 1;
fi

mkdir -p $jobdir

if [ ! -e $jobdir ]; then
    echo "Could not make job scratch directory ($jobdir).   Aborting";
    exit 1;
fi

cd $jobdir
        """ % ({'full_job_dir': full_job_dir})

    # untar file containing input wcl files
    scriptstr += """
echo ""
echo "Untaring input tar: $intar"
d1=`date "+%s"`
echo "PFW: untaring_input_tar starttime: $d1"
tar -xzf $initdir/$intar
d2=`date "+%s"`
echo "PFW: untaring_input_tar endtime: $d2"
"""
    scriptstr += 'echo "DESDMTIME: untar_input_tar $((d2-d1)) secs"'

    # copy files so can test by hand after job
    # save initial directory to job wcl file
    scriptstr += """
echo "Copying job wcl and task list to job working directory"
d1=`date "+%s"`
echo "PFW: copy_job_setup starttime: $d1"
cp $initdir/$jobwcl $jobwcl
cp $initdir/$tasklist $tasklist
d2=`date "+%s"`
echo "PFW: copy_job_setup endtime: $d2"
"""
    scriptstr += 'echo "DESDMTIME: copy_jobwcl_tasklist $((d2-d1)) secs"'
    scriptstr += """
echo ""
echo "Calling pfwrunjob.py"
echo "cmd> ${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist"
d1=`date "+%s"`
echo "PFW: pfwrunjob starttime: $d1"
${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist
rjstat=$?
d2=`date "+%s"`
echo "PFW: pfwrunjob endtime: $d2"
echo ""
echo ""
"""


    scriptstr += """
shd2=`date "+%s"`
echo "PFW: job_shell_script endtime: $shd2"
echo "PFW: job_shell_script exit_status: $rjstat"
echo "DESDMTIME: pfwrunjob.py $((d2-d1)) secs"
echo "DESDMTIME: job_shell_script $((shd2-PFW_JOB_START_EPOCH)) secs"
"""

    scriptstr += "exit $rjstat"

    # write shell script to file
    with open(scriptfile, 'w') as scriptfh:
        scriptfh.write(scriptstr)

    os.chmod(scriptfile, stat.S_IRWXU | stat.S_IRWXG)

    miscutils.fwdebug_print("END\n\n")

    return scriptfile

#######################################################################
def tar_inputfiles(config, jobnum, inlist):
    """ Tar the input wcl files for a single job """
    inputtar = config.get_filename('inputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobnum}})
    tjpad = pfwutils.pad_jobnum(jobnum)
    miscutils.coremakedirs(tjpad)

    pfwutils.tar_list("%s/%s" % (tjpad, inputtar), inlist)
    return inputtar

#######################################################################
def stage_inputs(config, inputfiles):
    """ Transfer inputs to target archive if using one """

    miscutils.fwdebug_print("BEG")
    miscutils.fwdebug_print("number of input files needed at target = %s" % len(inputfiles))

    if miscutils.fwdebug_check(6, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("input files %s" % inputfiles)

    if (pfwdefs.USE_HOME_ARCHIVE_INPUT in config and
            (config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == pfwdefs.TARGET_ARCHIVE.lower() or
             config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == 'all')):

        miscutils.fwdebug_print("home_archive = %s" % config[pfwdefs.HOME_ARCHIVE])
        miscutils.fwdebug_print("target_archive = %s" % config[pfwdefs.TARGET_ARCHIVE])
        sys.stdout.flush()
        archive_transfer_utils.archive_copy(config[pfwdefs.SW_ARCHIVESECT][config[pfwdefs.HOME_ARCHIVE]],
                                            config[pfwdefs.SW_ARCHIVESECT][config[pfwdefs.TARGET_ARCHIVE]],
                                            config.getfull('archive_transfer'),
                                            inputfiles, config)

    miscutils.fwdebug_print("END\n\n")



#######################################################################
def write_output_list(config, outputfiles):
    """ Write output list """

    miscutils.fwdebug_print("BEG")

    if miscutils.fwdebug_check(1, "PFWBLOCK_DEBUG"):
        miscutils.fwdebug_print("output files %s" % outputfiles)

    if 'block_outputlist' not in config:
        miscutils.fwdie("Error:  Could not find block_outputlist in config.   Internal Error.", pfwdefs.PF_EXIT_FAILURE)

    with open(config.getfull('block_outputlist'), 'w') as outfh:
        for fname in outputfiles:
            outfh.write("%s\n" % miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME))

    miscutils.fwdebug_print("END")


#######################################################################
def write_wrapper_wcl(filename, wrapperwcl):
    """ Write wrapper input wcl to file """

    if os.path.exists(filename):
        print "Error:   input wcl file already exists (%s)" % filename
        print "\t\tCheck modnamepat vs wrapperloop for a missing term in modnamepat"
        miscutils.fwdie("Input wcl file already exists", pfwdefs.PF_EXIT_FAILURE)
    else:
        wcldir = os.path.dirname(filename)
        miscutils.coremakedirs(wcldir)
        with open(filename, 'w', 0) as wclfh:
            wrapperwcl.write(wclfh, True, 4)

######################################################################
def copy_input_lists_home_archive(config, filemgmt, archive_info, listfullnames):
    """ Copy list files to home archive """

    archdir = '%s' % config.getfull(pfwdefs.ATTEMPT_ARCHIVE_PATH)
    if miscutils.fwdebug_check(6, 'BEGRUN_DEBUG'):
        miscutils.fwdebug_print('archive rel path = %s' % archdir)

    # copy the files to the home archive
    files2copy = {}
    for lfname in listfullnames:
        relpath = os.path.dirname(lfname)
        filename = miscutils.parse_fullname(lfname, miscutils.CU_PARSE_FILENAME)
        archfname = '%s/%s/%s' % (archdir, relpath, filename)
        files2copy[lfname] = {'src': lfname,
                              'filename': filename,
                              'dst': archfname,
                              'fullname': archfname}

    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print('files2copy = %s' % files2copy)

    # load file mvmt class
    submit_files_mvmt = config.getfull('submit_files_mvmt')
    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print('submit_files_mvmt = %s' % submit_files_mvmt)
    filemvmt_class = miscutils.dynamically_load_class(submit_files_mvmt)
    valdict = fmutils.get_config_vals(config['job_file_mvmt'], config,
                                      filemvmt_class.requested_config_vals())
    filemvmt = filemvmt_class(archive_info, None, None, None, valdict)

    results = filemvmt.job2home(files2copy)
    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print('trans results = %s' % results)

    # save info for files that we just copied into archive
    files2register = []
    problemfiles = {}
    for fname, finfo in results.items():
        if 'err' in finfo:
            problemfiles[fname] = finfo
            print "Warning: Error trying to copy file %s to archive: %s" % (fname, finfo['err'])
        else:
            files2register.append(finfo)

    # call function to do the register
    if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print('files2register = %s' % files2register)
        miscutils.fwdebug_print('archive = %s' % archive_info['name'])
    filemgmt.register_file_in_archive(files2register, archive_info['name'])
