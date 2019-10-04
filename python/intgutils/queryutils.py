"""
    .. _intgutils-queryutils:

    **queryutils**
    --------------

    Functions useful for query codes to be called by framework
"""

import intgutils.intgdefs as intgdefs

###########################################################
def convert_single_files_to_lines(filelist, initcnt=1):
    """ Convert single files to dict of lines in prep for output """

    count = initcnt
    linedict = {'list': {}}

    if isinstance(filelist, dict) and len(filelist) > 1 and \
            'filename' not in filelist.keys():
        filelist = filelist.values()
    elif isinstance(filelist, dict):  # single file
        filelist = [filelist]

    linedict = {'list': {intgdefs.LISTENTRY: {}}}
    for onefile in filelist:
        fname = "file%05d" % (count)
        lname = "line%05d" % (count)
        linedict['list'][intgdefs.LISTENTRY][lname] = {'file': {fname: onefile}}
        count += 1
    return linedict
