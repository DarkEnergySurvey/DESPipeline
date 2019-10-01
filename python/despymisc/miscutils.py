"""
    .. _despymisc-miscutils:

    **miscutils**
    -------------

    Miscellaneous support functions for framework
"""

import re
import os
import copy
import sys
import datetime
import inspect
import errno
from collections import Mapping
import logging


#######################################################################
def fwdebug(msglvl, envdbgvar, msgstr, msgprefix=''):
    """ Print debugging message based upon thresholds. If the environment
        variable (envdbgvar) value is equal to or above msglvl, then print
        the message. (see fwdebug_check for details)

        Parameters
        ----------
        msglvl : int
           The level of the message (the larger the number the less important
           the massage is: e.g. 0 = critial, 10 = only print this if you want
           to know in depth details for debugging)

        envdbgvar : str
           The environment variable to check the value of

        msgstr : str
            The message to print

        msgprefix : str
            A value to prepend to the message. Default is an empty string

    """
    # environment debug variable overrides code set level

    if fwdebug_check(msglvl, envdbgvar):
        fwdebug_print(msgstr, msgprefix)

#######################################################################
def fwdebug_check(msglvl, envdbgvar):
    """ Check whether the given msglvl is equal to or lower than the current
        debug level. The debug level is determined by (in order of precidence):

        - if 'DESDM_DEBUG' is an environment variable then it's value is used
        - if envdbgvar is an environment variable then it's value is used
        - if the first part of envdbgvar (test before the first '_') + '_DEBUG'
          is an environment variable then it's value is used. e.g: 'FWMG_TEST'
          would become 'FWMG_DEBUG'
        - use 0

        Parameters
        ----------
        msglvl : int
            The debug level to check

        envdbgvar : str
           The environment variable to check the value of

        Returns
        -------
        bool, True if msglvl is less than or equal to the current debug level,
        False otherwise
    """
    # environment debug variable overrides code set level

    dbglvl = 0

    if 'DESDM_DEBUG' in os.environ:   # global override
        dbglvl = os.environ['DESDM_DEBUG']
    elif envdbgvar in os.environ:
        dbglvl = os.environ[envdbgvar]
    elif '_' in envdbgvar:
        prefix = envdbgvar.split('_')[0]
        if '%s_DEBUG' % prefix in os.environ:
            dbglvl = os.environ['%s_DEBUG' % prefix]

    return int(dbglvl) >= int(msglvl)

#######################################################################
def fwdebug_print(msgstr, msgprefix=''):
    """ Print the given message to the screen with the given prefix, current
        system time, and calling file name.

        Parameters
        ----------
        msgstr : str
            The message to print

        msgprefix : str
            Text to prepend to the output line. Default is an empty string

    """
    print "%s%s - %s - %s" % (msgprefix, datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
                              inspect.stack()[1][3], msgstr)

#######################################################################
def fwdie(msg, exitcode, depth=1):
    """ Abort with the given exit code, after printing a short message,
        including some info from backtrace.

        Parameters
        ----------
        msg : str
            The message to print

        exitcode : int
            The code to exit python with

        depth : int
            How many line of backtrace to print. Default is 1
    """
    frame = inspect.stack()[depth]
    fle = os.path.basename(frame[1])
    print "\n\n%s:%s:%s: %s" % (fle, frame[3], frame[2], msg)

    sys.exit(exitcode)

#######################################################################
def fwsplit(fullstr, delim=','):
    """ Split the input string by delim and trim substrs, expand #:# into range

        Parameters
        ----------
        fullstr : str
            The string to split

        delim : str
            The delimiter to use for splitting the string. Default is a comma

        Returns
        -------
        List containing the elements from the split

        Examples
        --------
        fwsplit('1,2,3,4') returns ['1', '2', '3', '4']

        fwsplit('1,2,4:6') returns ['1', '2', '4', '5', '6']

    """
    #fullstr = re.sub('[()]', '', fullstr) # delete parens if exist
    fullstr = fullstr.replace('(', '')
    fullstr = fullstr.replace(')', '')
    fullstr = fullstr.replace('[', '')
    fullstr = fullstr.replace(']', '')
    items = []
    for item in [x.strip() for x in fullstr.split(delim)]:
        m = re.match(r"(\d+):(\d+)", item)
        if m:
            items.extend(map(str, range(int(m.group(1)),
                                        int(m.group(2))+1)))
        else:
            items.append(item)
    return items

#######################################################################
def coremakedirs(thedir):
    """ Create a directory, but just return gracefully if it already
        exists.

        Parameters
        ----------
        thedir : str
            The directory to make
    """
    if thedir and not os.path.exists(thedir):  # some parallel filesystems really don't like
                                               # trying to make directory if it already exists
        try:
            os.makedirs(thedir)
        except OSError as exc:      # go ahead and check for race condition
            if exc.errno == errno.EEXIST:
                pass
            else:
                print "Error: problems making directory: %s" % exc
                raise

#######################################################################
CU_PARSE_HDU = 8
CU_PARSE_PATH = 4
CU_PARSE_FILENAME = 2
CU_PARSE_EXTENSION = 1   # deprecating use CU_PARSE_COMPRESSION
CU_PARSE_COMPRESSION = 1
CU_PARSE_BASENAME = 0
VALID_COMPRESS_EXT = ['fz', 'gz']

def parse_fullname(fullname, retmask=CU_PARSE_FILENAME):
    """ Parse the given file name, returning the requested parts

        Parameters
        ----------
        fullname : str
            The name of the file (can include the path)

        retmask : int
            The requested return values. Acceptable values are:
            * CU_PARSE_BASENAME : return just the file name, including the extension(s), removing any path components
            * CU_PARSE_COMPRESSION : return the compression, valid compression value are '.fz' and '.gz'
            * CU_PARSE_FILENAME : return the filename with any compression extension removed as well as removing any path components
            * CU_PARSE_PATH : return just the directory path to the file
            * CU_PARSE_HDU : return the HDU of the file, this is specified by an int inside of [] at the end of the file. e.g. file.fits[2] has an HDU of 2

            These values can be logically or'd together to get multiple return values.
            e.g. CU_PARSE_FILENAME | CU_PARSE_COMPRESSION will return both the file name
            and the compression as a two element list

            Default value is CU_PARSE_FILENAME.

        Returns
        -------
        A single string if only one item was requested, or a list of the requested
        parts if retmask is an or'd value. None will be returned if there is no result.
    """
    fwdebug(3, 'MISCUTILS_DEBUG', "fullname = %s" % fullname)
    fwdebug(3, 'MISCUTILS_DEBUG', "retmask = %s" % retmask)

    hdu = None
    compress_ext = None
    filename = None
    path = None
    retval = []
    parse_basename = False

    # wants filename+compext returned as single string
    if retmask == CU_PARSE_BASENAME:
        parse_basename = True
        retmask = CU_PARSE_FILENAME | CU_PARSE_COMPRESSION

    # check for hdu
    m = re.match(r'(\S+)\[(\S+)\]$', fullname)
    if m:
        fullname = m.group(1)   # remove the hdu so it doesn't show up in filename
        if retmask & CU_PARSE_HDU:
            hdu = m.group(2)

    if retmask & CU_PARSE_PATH:
        #if '/' in fullname: # if given full path, canonicalize it
        #    fullname = os.path.realpath(fullname)
        path = os.path.dirname(fullname)
        if not path:   # return None instead of empty string
            retval.append(None)
        else:
            retval.append(path)

    filename = os.path.basename(fullname)
    fwdebug(3, 'MISCUTILS_DEBUG', "filename = %s" % filename)

    # check for compression extension on files, assumes extension + compression extension
    m = re.search(r'^(\S+\.\S+)\.([^.]+)$', filename)
    if m:
        fwdebug(3, 'MISCUTILS_DEBUG', "m.group(2)=%s" % m.group(2))
        fwdebug(3, 'MISCUTILS_DEBUG', "VALID_COMPRESS_EXT=%s" % VALID_COMPRESS_EXT)
        if m.group(2) in VALID_COMPRESS_EXT:
            filename = m.group(1)
            compress_ext = '.'+m.group(2)
        else:
            if retmask & CU_PARSE_COMPRESSION:
                fwdebug(3, 'MISCUTILS_DEBUG', "Not valid compressions extension (%s)  Assuming non-compressed file." % m.group(2))
            compress_ext = None
    else:
        fwdebug(3, 'MISCUTILS_DEBUG', "Didn't match pattern for fits file with compress extension")
        compress_ext = None

    if parse_basename:
        retval = filename
        if compress_ext is not None:
            retval += compress_ext
        fwdebug(3, 'MISCUTILS_DEBUG', "filename = %s, compress_ext = %s, retval = %s" % (filename, compress_ext, retval))
    else:
        if retmask & CU_PARSE_FILENAME:
            retval.append(filename)
        if retmask & CU_PARSE_COMPRESSION:
            retval.append(compress_ext)
        if retmask & CU_PARSE_HDU:
            retval.append(hdu)

    if not retval:
        retval = None
    elif len(retval) == 1:  # if only 1 entry in array, return as scalar
        retval = retval[0]

    return retval

#######################################################################
def convertBool(var):
    """ Convert the given item to a bool. If the item type is an int
        then it is cast to a bool (0 = False, any other value is True).
        If the item type is a string the 'y', 'yes', and 'true' will return
        True, and 'n', 'no', and 'false' will return False (case insensitive).
        If the given string is an int (e.g. '2') then the int value will be
        cast to a bool. If the item type is a bool, then it's value is returned.
        If the item is None, then False is returned. Any other data type will
        raise an exception.

        Parameters
        ----------
        var : various
            The item to convert to a bool

        Returns
        -------
        The bool value of the input item
    """
    #print "Before:", var, type(var)
    newvar = None
    if var is not None:
        tvar = type(var)
        if tvar == int:
            newvar = bool(var)
        elif tvar == str:
            try:
                newvar = bool(int(var))
            except ValueError:
                if var.lower() in ['y', 'yes', 'true']:
                    newvar = True
                elif var.lower() in ['n', 'no', 'false']:
                    newvar = False
        elif tvar == bool:
            newvar = var
        else:
            raise Exception("Type not handled (var, type): %s, %s" % (var, type(var)))
    else:
        newvar = False
    #print "After:", newvar, type(newvar)
    #print "\n\n"
    return newvar

# For consistent testing of boolean variables
#    Example: whether to use database or not
#    Function argument value overrides environment variable
#    Lower case key for arg lookup, Upper case for environ lookup
def checkTrue(key, arg, default=True):
    """ Check whether the given value is true. If arg is a dictionary then
        the boolean value of arg[key] is returned. If arg is a class or other structure
        then the boolean value of arg.key is returned. If arg is another type then
        arg is cast to a bool and that value is returned. Failing that, if DESDM_arg
        is an environment variable then that is cast to a bool. Otherwise the default
        value is returned.

        Parameters
        ----------
        key : str
            The dictionary key, or object attribute to look up.

        arg : various
            The item to search for the value os key in.

        default : bool
            The default value to return if key is not found. Defaults to True.

        Returns
        -------
        The bool value of the requested item, or the given default value.
    """
    ret_val = default

    args_val = None
    scalar_arg = None

    # handle cases where given arg is dict, argparse.Namespace
    if isinstance(arg, dict):
        if key.lower() in arg:
            scalar_arg = arg[key.lower()]
    elif hasattr(arg, key.lower()):
        scalar_arg = getattr(arg, key.lower())
    else:
        scalar_arg = arg

    if scalar_arg is not None:
        args_val = convertBool(scalar_arg)

    if args_val is not None:
        ret_val = args_val
    else:
        env_key = 'DESDM_%s' % key.upper()
        if env_key in os.environ and not convertBool(os.environ[env_key]):
            ret_val = False

    return ret_val


## PrettyPrinter doesn't work for certain nested dictionary (OrderedDict) cases
##     http://bugs.python.org/issue10592
def pretty_print_dict(the_dict, out_file=None, sortit=False, indent=4):
    """ Outputs a given dictionary in a format easier for human reading where items within
        the same sub-dictionary could be output in alphabetical order

        Parameters
        ----------
        the_dict : dict
            The dictionary to print nicely

        out_file : str
            The file handle to write the pretty printed dictionary to. Defaults to
            None, which is sys.stdout.

        sortit : bool
            Whether to sort the keys at each level alphabetically before printing
            them. Default is False.

        indent : int
            The indentation to use, in number of spaces, for each level (additive) of the dictionary
            as it is printed out. Default is 4.
    """
    if out_file is None:
        out_file = sys.stdout
    assert the_dict is not None, "Passed in None for dictionary arg"
    assert isinstance(the_dict, dict), "Passed in non-dictionary object for dictionary arg"
    _recurs_pretty_print_dict(the_dict, out_file, sortit, indent, 0)


def _recurs_pretty_print_dict(the_dict, out_file, sortit, inc_indent, curr_indent):
    """Internal recursive function to do actual WCL writing"""
    if the_dict:
        if sortit:
            dictitems = sorted(the_dict.items())
        else:
            dictitems = the_dict.items()

        for key, value in dictitems:
            if isinstance(value, dict):
                print >> out_file, ' ' * curr_indent + str(key)
                _recurs_pretty_print_dict(value, out_file, sortit, inc_indent,
                                          curr_indent + inc_indent)
            else:
                print >> out_file, ' ' * curr_indent + str(key) + \
                        " = " + str(value)


#######################################################################
def get_config_vals(extra_info, config, keylist):
    """ Search the given dictionaries for the given keys. Returning a
        dictionary of the retrieved values. This will exit python
        if one of the keys is marked as required, but is not found. If
        the requested key is found in extra_info, then that value will
        take precedence over its config value.

        Parameters
        ----------
        extra_info : dict
            Dictionary to search first for each key.

        config : dict
            Dictionary to search second for each key

        keylist : dict
            Dictionary containing key-value pairs of the keys to search
            for and values indicating whther they are required (value of
            'req', case insensitive)

        Returns
        -------
        Dict containing the found keys and their values.
    """
    info = {}
    for k, stat in keylist.items():
        if extra_info is not None and k in extra_info:
            info[k] = extra_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif stat.lower() == 'req':
            fwdebug(0, 'MISCUTILS_DEBUG', '******************************')
            fwdebug(0, 'MISCUTILS_DEBUG', 'keylist = %s' % keylist)
            fwdebug(0, 'MISCUTILS_DEBUG', 'extra_info = %s' % extra_info)
            fwdebug(0, 'MISCUTILS_DEBUG', 'config = %s' % config)
            fwdie('Error: Could not find required key (%s)' % k, 1, 2)
    return info

#######################################################################
def dynamically_load_class(class_desc):
    """ Loads class at runtime based upon given string description

        Parameters
        ----------
        class_desc : str
            The name of the class to load (but not instantiate)

        Returns
        -------
        Class of the requested type
    """

    fwdebug(3, 'MISCUTILS_DEBUG', "class_desc = %s" % class_desc)
    modparts = class_desc.split('.')
    fromname = '.'.join(modparts[0:-1])
    importname = modparts[-1]
    fwdebug(3, 'MISCUTILS_DEBUG', "\tfromname = %s" % fromname)
    fwdebug(3, 'MISCUTILS_DEBUG', "\timportname = %s" % importname)
    mod = __import__(fromname, fromlist=[importname])
    dynclass = getattr(mod, importname)
    return dynclass

#######################################################################
def updateOrderedDict(d, u):
    """ Update a dictionary. This is done recursively in the case where
        the dictionary value(s) is also a dictionary. If any of the input
        keys do not exist in the dictionary being updated, then they are
        added to that dictionary.

        Parameters
        ----------
        d : dict
            The dictionary being updated

        u : dict
            The dictionary containing the new key-value pairs for d.
    """

    for k, v in u.iteritems():
        if isinstance(v, Mapping):
            if d.__contains__(k):
                d2 = d.get(k)
                if isinstance(d2, Mapping):
                    updateOrderedDict(d2, v)
                else:
                    raise TypeError("Expected dictionary type")
            else:
                d[k] = copy.deepcopy(v)
        else:
            d[k] = copy.deepcopy(v)

#######################################################################
def get_list_directories(filelist):
    """ Get a list of directories from a list of files. This is a unique
        list of all directories in the path of each of the files.

        Parameters
        ----------
        filelist : list
            List of files including their paths

        Returns
        -------
        List of directories in the paths of the files.
    """
    dirlist = {}
    for f in filelist:
        filedir = parse_fullname(f, CU_PARSE_PATH)
        relparents = filedir.split('/')
        thedir = ""
        for i in range(1, len(relparents)):
            thedir += '/' + relparents[i]
            dirlist[thedir] = True

    return sorted(dirlist.keys())



#########################################################################
# Some functions added by Felipe Menanteau, coming from the old despyutils

def elapsed_time(t1, verbose=False):
    """ Return the elapsed time since t1 in a human readable format.

        Parameters
        ----------
        t1 : time instance
            The time from which the calculations are based

        verboase : bool
            Whether to print out the result to the screen. Default is False

        Returns
        -------
        str, representing the ellapsed time since t1.
    """
    import time
    t2 = time.time()
    stime = "%dm %2.2fs" % (int((t2 - t1) / 60.), (t2 - t1) - 60 * int((t2 - t1) / 60.))
    if verbose:
        print "# Elapsed time: %s" % stime
    return stime

def query2dict_of_lists(query, dbhandle):
    """ Transforms the result of an SQL query and a Database handle object [dhandle]
        into a dictionary of lists. Each dictionary key is a column and the memebers of
        the corresponding lists are the row values.

        Parameters
        ----------
        query : str
            The sql query to perform

        dbhandle : handle
            Handle to the database to operate on

        Returns
        -------
        Dict containing the values from the query.
    """

    # Get the cursor from the DB handle
    cur = dbhandle.cursor()
    # Execute
    cur.execute(query)
    # Get them all at once
    list_of_tuples = cur.fetchall()
    # Get the description of the columns to make the dictionary
    desc = [d[0] for d in cur.description]

    querydic = {} # We will populate this one
    cols = zip(*list_of_tuples)
    for i, val in enumerate(cols):
        key = desc[i]
        querydic[key] = val

    return querydic


def create_logger(level=logging.NOTSET, name='default'):
    """ Create a logging instance

        Parameters
        ----------
        level : int
            The cutoff logging level (any log messages with levels
            above this will not be logged). Default is logging.NOTSET,
            all messages are logged.

        name : str
            The name of the logger to use. Default is 'default'
    """
    logging.basicConfig(level=level,
                        format='[%(asctime)s] [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(name)
    # ensure the proper level, if logging was instantiated by any other module,
    # even an imported one, the level setting from that instance will take precedence
    # over the one given in basicConfig above.
    logger.setLevel(level)
    return logger
