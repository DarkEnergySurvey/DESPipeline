"""
    .. _intgutils-intgmisc:

    **intgmisc**
    ------------

    Contains misc integration utilities
"""

import shlex
import os
import re
from despymisc import subprocess4
from despymisc import miscutils
from intgutils import intgdefs
import intgutils.replace_funcs as replfuncs


######################################################################
def check_files(fullnames):
    """ Check whether given files exist on disk

        Parameters
        ----------
        fullnames : list
            List of files to check.

        Returns
        -------
        tuple
            Tuple of two lists. The first is the files that exist and the
            second is the files that do not exist.
    """

    exists = []
    missing = []
    for fname in fullnames:
        if os.path.exists(fname):
            exists.append(fname)
        else:
            missing.append(fname)
    return (exists, missing)


#######################################################################
def get_cmd_hyphen(hyphen_type, cmd_option):
    """ Determine correct hyphenation for command line argument

        Parameters
        ----------
        hyphen_type : str
            The type of hyphen to use. Acceptable values are

            * 'alldouble' - all arguments will be prepended with '--'
            * 'allsingle' - all arguments will be prepended with '-'
            * 'mixed_gnu' - argument will be prepended with '--' or '-' depending on the length of `cmd_option`

        cmd_option : str
            The command line option, only used when `hyphen_type` is
            'mixed_gnu'. If this is a single character then the hyphen
            will be '-', if more than one character then the hyphen will be '--'

        Returns
        -------
        str
            The string representation of the hyphen to use, either '--' or '-'.

        Raises
        ------
        ValueError
            If an invalid `hyphen_type` is given.
    """

    hyphen = '-'

    if hyphen_type == 'alldouble':
        hyphen = '--'
    elif hyphen_type == 'allsingle':
        hyphen = '-'
    elif hyphen_type == 'mixed_gnu':
        if len(cmd_option) == 1:
            hyphen = '-'
        else:
            hyphen = '--'
    else:
        raise ValueError('Invalid cmd hyphen type (%s)' % hyphen_type)

    return hyphen

#######################################################################
def get_exec_sections(wcl, prefix):
    """ Returns exec sections appearing in given wcl

        Parameters
        ----------
        wcl : WCL
            The WCL object to look in.

        prefix : str
            The exec prefix to look for.

        Returns
        -------
        dict
            Dictionary of the found exec section names and their contents.
    """
    execs = {}
    for key, val in wcl.items():
        if miscutils.fwdebug_check(3, "DEBUG"):
            miscutils.fwdebug_print("\tsearching for exec prefix in %s" % key)

        if re.search(r"^%s\d+$" % prefix, key):
            if miscutils.fwdebug_check(4, "DEBUG"):
                miscutils.fwdebug_print("\tFound exec prefex %s" % key)
            execs[key] = val
    return execs


#######################################################################
def run_exec(cmd):
    """ Run an executable with given command returning process information

        Parameters
        ----------
        cmd : str
            The command to run

        Returns
        -------
        tuple
            Two element tuple containing the return code from the executable
            and a dictionary containing the process information.
    """

    procfields = ['ru_idrss', 'ru_inblock', 'ru_isrss', 'ru_ixrss',
                  'ru_majflt', 'ru_maxrss', 'ru_minflt', 'ru_msgrcv',
                  'ru_msgsnd', 'ru_nivcsw', 'ru_nsignals', 'ru_nswap',
                  'ru_nvcsw', 'ru_oublock', 'ru_stime', 'ru_utime']
    retcode = None
    procinfo = None

    subp = subprocess4.Popen(shlex.split(cmd), shell=False)
    retcode = subp.wait4()
    procinfo = dict((field, getattr(subp.rusage, field)) for field in procfields)

    return (retcode, procinfo)


#######################################################################
def remove_column_format(columns):
    """ Return columns minus any formatting specification

        Parameters
        ----------
        columns : list
            List of columns to process.
    """

    columns2 = []
    for col in columns:
        if col.startswith('$FMT{'):
            rmatch = re.match(r'\$FMT\{\s*([^,]+)\s*,\s*(\S+)\s*\}', col)
            if rmatch:
                columns2.append(rmatch.group(2).strip())
            else:
                miscutils.fwdie("Error: invalid FMT column: %s" % (col), 1)
        else:
            columns2.append(col)
    return columns2


#######################################################################
def convert_col_string_to_list(colstr, with_format=True):
    """ Convert a string of column headers to list of columns

        Parameters
        ----------
        colstr : str
            The column headers as a string.

        with_format : bool, optional
            Whether to return the columns with (``True``) or without
            (``False``) formatting information. Default is ``True``.

        Returns
        -------
        list
            The column headers as a list.
    """
    columns = re.findall(r'\$\S+\{.*\}|[^,\s]+', colstr)

    if not with_format:
        columns = remove_column_format(columns)
    return columns


#######################################################################
def read_fullnames_from_listfile(listfile, linefmt, colstr):
    """ Read a list file returning fullnames from the list

        Parameters
        ----------
        listfile : str
            The file to read

        linefmt : str
            The format of the lines. Acceptable formats are

            * 'textcsv' - a csv style file
            * 'texttab' - a tab separated style file
            * 'testsp' - a space separated style file

        colstr : str
            A string representation of the column headers.

        Returns
        -------
        dict
            Dictionary of the file full names and general info.
    """

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('colstr=%s' % colstr)

    columns = convert_col_string_to_list(colstr, False)

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('columns=%s' % columns)

    fullnames = {}
    pos2fsect = {}
    for pos, col in enumerate(columns):
        lcol = col.lower()
        if lcol.endswith('.fullname'):
            filesect = lcol[:-9]
            pos2fsect[pos] = filesect
            fullnames[filesect] = []
        # else a data column instead of a filename

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('pos2fsect=%s' % pos2fsect)

    if linefmt == 'config' or linefmt == 'wcl':
        miscutils.fwdie('Error:  wcl list format not currently supported (%s)' % listfile, 1)
    else:
        with open(listfile, 'r') as listfh:
            for line in listfh:
                line = line.strip()

                # convert line into python list
                lineinfo = []
                if linefmt == 'textcsv':
                    lineinfo = miscutils.fwsplit(line, ',')
                elif linefmt == 'texttab':
                    lineinfo = miscutils.fwsplit(line, '\t')
                elif linefmt == 'textsp':
                    lineinfo = miscutils.fwsplit(line, ' ')
                else:
                    miscutils.fwdie('Error:  unknown linefmt (%s)' % linefmt, 1)

                # save each fullname in line
                for pos in pos2fsect:
                    # use common routine to parse actual fullname (e.g., remove [0])
                    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | \
                                miscutils.CU_PARSE_COMPRESSION
                    (path, filename, compression) = miscutils.parse_fullname(lineinfo[pos],
                                                                             parsemask)
                    fname = "%s/%s" % (path, filename)
                    if compression is not None:
                        fname += compression
                    fullnames[pos2fsect[pos]].append(fname)

    if miscutils.fwdebug_check(6, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('fullnames = %s' % fullnames)
    return fullnames


######################################################################
def get_list_fullnames(sect, modwcl):
    """ Get the full name of the list file and the full names from the
        list file.

        Parameters
        ----------
        sect : str
            The section of the wcl to look in

        modwcl : WCL
            The WCL to look in

        Returns
        -------
        tuple
            Two element tuple containing the full name of the list file
            and a set of the files in the list file.
    """
    (_, listsect, filesect) = sect.split('.')
    ldict = modwcl[intgdefs.IW_LIST_SECT][listsect]

    # check list itself exists
    listname = ldict['fullname']
    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print("\tINFO: Checking existence of '%s'" % listname)

    if not os.path.exists(listname):
        miscutils.fwdebug_print("\tError: input list '%s' does not exist." % listname)
        raise IOError("List not found: %s does not exist" % listname)

    # get list format: space separated, csv, wcl, etc
    listfmt = intgdefs.DEFAULT_LIST_FORMAT
    if intgdefs.LIST_FORMAT in ldict:
        listfmt = ldict[intgdefs.LIST_FORMAT]

    setfnames = set()

    # read fullnames from list file
    fullnames = read_fullnames_from_listfile(listname, listfmt, ldict['columns'])
    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print("\tINFO: fullnames=%s" % fullnames)

    if filesect not in fullnames:
        columns = convert_col_string_to_list(ldict['columns'], False)

        if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
            miscutils.fwdebug_print('columns=%s' % columns)

        hasfullname = False
        for col in columns:
            lcol = col.lower()
            if lcol.endswith('.fullname') and lcol.startswith(filesect):
                hasfullname = True
        if hasfullname:
            miscutils.fwdebug_print("ERROR: Could not find sect %s in list" % (filesect))
            miscutils.fwdebug_print("\tcolumns = %s" % (columns))
            miscutils.fwdebug_print("\tlist keys = %s" % (fullnames.keys()))
        elif miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
            miscutils.fwdebug_print("WARN: Could not find sect %s in fullname list.   Not a problem if list (sect) has only data." % (filesect))
    else:
        setfnames = set(fullnames[filesect])
    return listname, setfnames


######################################################################
def get_file_fullnames(sect, filewcl, fullwcl):
    """ Get the full name of the files in the specified section.

        Parameters
        ----------
        sect : str
            The WCL section to use

        filewcl : WCL
            The WCl to use

        fullwcl : WCL
            The full WCL, used to generate the full names

        Returns
        -------
        set
            The full file names
    """
    sectkeys = sect.split('.')
    sectname = sectkeys[1]

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print("INFO: Beg sectname=%s" % sectname)

    fnames = []
    if sectname in filewcl:
        filesect = filewcl[sectname]
        if 'fullname' in filesect:
            fnames = replfuncs.replace_vars(filesect['fullname'], fullwcl)[0]
            fnames = miscutils.fwsplit(fnames, ',')
            if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
                miscutils.fwdebug_print("INFO: fullname = %s" % fnames)

    return set(fnames)



######################################################################
def get_fullnames(modwcl, fullwcl, exsect=None):
    """ Return dictionaries of input and output fullnames by section

        Parameters
        ----------
        modwcl : WCL
            The WCL used in this section

        fullwcl : WCL
            The full WCL, used to expand and variables found

        exsect : str, optional
            The exec section to look for. Default is ``None`` which indicates
            the section name is in the `modwcl`.

        Returns
        -------
        tuple
            Two element tuple of the input and output file data dictionaries
            for the specified section.
    """

    exec_sectnames = []
    if exsect is None:
        exec_sectnames = get_exec_sections(modwcl, intgdefs.IW_EXEC_PREFIX)
    else:
        exec_sectnames = [exsect]

    # intermediate files (output of 1 exec, but input for another exec
    # within same wrapper) are listed only with output files

    # get output file names first so can exclude intermediate files from inputs
    outputs = {}
    allouts = set()
    for esect in sorted(exec_sectnames):
        exwcl = modwcl[esect]
        if intgdefs.IW_OUTPUTS in exwcl:
            for sect in miscutils.fwsplit(exwcl[intgdefs.IW_OUTPUTS], ','):
                sectkeys = sect.split('.')
                outset = None
                if sectkeys[0] == intgdefs.IW_FILE_SECT:
                    outset = get_file_fullnames(sect, modwcl[intgdefs.IW_FILE_SECT], fullwcl)
                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    _, outset = get_list_fullnames(sect, modwcl)
                else:
                    print "exwcl[intgdefs.IW_OUTPUTS]=", exwcl[intgdefs.IW_OUTPUTS]
                    print "sect = ", sect
                    print "sectkeys = ", sectkeys
                    raise KeyError("Unknown data section %s" % sectkeys[0])
                outputs[sect] = outset
                allouts.union(outset)

    inputs = {}
    for esect in sorted(exec_sectnames):
        exwcl = modwcl[esect]
        if intgdefs.IW_INPUTS in exwcl:
            for sect in miscutils.fwsplit(exwcl[intgdefs.IW_INPUTS], ','):
                sectkeys = sect.split('.')
                inset = None
                if sectkeys[0] == intgdefs.IW_FILE_SECT:
                    inset = get_file_fullnames(sect, modwcl[intgdefs.IW_FILE_SECT], fullwcl)
                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    _, inset = get_list_fullnames(sect, modwcl)
                    #inset.add(listname)
                else:
                    print "exwcl[intgdefs.IW_INPUTS]=", exwcl[intgdefs.IW_INPUTS]
                    print "sect = ", sect
                    print "sectkeys = ", sectkeys
                    raise KeyError("Unknown data section %s" % sectkeys[0])

                # exclude intermediate files from inputs
                if inset is not None:
                    inset = inset - allouts
                    inputs[sect] = inset

    return inputs, outputs

######################################################################
def check_input_files(sect, filewcl):
    """ Check that the files for a single input file section exist

        Parameters
        ----------
        sect : str
            The WCL section to look in.

        filewcl : WCL
            The WCl to use

        Returns
        -------
        tuple
            Two element tuple of lists containing the existing and missing files.
    """

    sectkeys = sect.split('.')
    fnames = miscutils.fwsplit(filewcl[sectkeys[1]]['fullname'], ',')
    (exists1, missing1) = check_files(fnames)
    return (exists1, missing1)
