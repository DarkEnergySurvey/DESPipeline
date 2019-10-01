"""
    .. _despymisc-scamputil:

    **scamputil**
    -------------

    Module to take an input file of HDU's and output only those requested
"""

def split_ahead_by_ccd(infile, outfile, ccd_list):
    """
    Function to take an .ahead (for SCAMP) and splits only those individual
    HDUs/elements that correspond to CCDs requested.

    infile:  an ASCII file (typically and .ahead with WCS header descriptions
    for all possible HDUs)

    outfile: an ASCII file containing only those HDUs of the infile that
    are specified to be included by ccdlist

    ccdlist: an integer list of HDUs to include in the outfile

    If the keyword CCDNUM is present in HDU components of the infile then these
    will override how each element in the .ahead file is given.  This provides the
    ability to have an empty/missing element but to maintain ordering.

    If a requested element is missing then a non-zero return code is given.
    This case is checked for before the output file is written
    """

    # Optional list of keywords to be removed from input file before writing to output.
    remove_keywords = ['HISTORY', 'COMMENT', 'FLXSCALE', 'MAGZEROP', 'PHOTIRMS', 'PHOTINST', 'PHOTLINK']

    # Work through head file prior to writing out separate files.
    # Currently doing things this way to:
    #  - avoid opening a file that will be blank at the end of this.
    #  - can check that exactly the correct number of headers are present (prior to writing).
    icnt = 0
    f1 = open(infile, 'r')
    head_set = {}
    tmp_dict = {}
    tmp_lines = []
    ccdnum_found = False
    for line in f1:
        line = line.strip()
        keywd = line[0:8].split()
        if keywd[0] == "CCDNUM":
            columns = line.split()
            tmp_dict["ccd"] = int(columns[2])
            ccdnum_found = True
        if keywd[0] not in remove_keywords:
            tmp_lines.append(line)
        if keywd[0] == "END":
            if not ccdnum_found:
                tmp_dict["ccd"] = int(icnt + 1)
            head_set[tmp_dict["ccd"]] = tmp_lines
            tmp_lines = []
            tmp_dict = {}
            icnt += 1
    f1.close()

    # Check ahead of time that all HDUs needed are present.
    ccd_check = True
    for ccd in ccd_list:
        if ccd not in head_set:
            print "Warning: CCDNUM=%d is not present in %s " % (ccd, infile)
            ccd_check = False

    if not ccd_check:
        #  If all CCDs/HDUs needed are not present then set flag and exit
        return False

    # Step through the list of CCDs and write each .ahead:HDU-like piece.    print len(head_set)
    fout = open(outfile, 'w')
    for ccd in ccd_list:
        if ccd in head_set:
            tmp_header = head_set[ccd]
            for hline in tmp_header:
                fout.write("%s\n" % (hline))
    fout.close()
    return True
