#! /usr/bin/env python
"""
Routine to call function that will take an .ahead file (for SCAMP) and split
out individual HDUs/CCD elements based on set of CCDs requested.
"""

import sys
import argparse
import os
from despymisc.miscutils import fwsplit
from despymisc import scamputil

def parseArgs():
    """ Parse the command line arguments """
    parser = argparse.ArgumentParser(description='Split .ahead file to include only a specific set of CCDs/HDUs')
    parser.add_argument('-i', '--infile', action='store', type=str, default=None,
                        help="Input filename (and path).")
    parser.add_argument('-o', '--outfile', action='store', type=str, default=None,
                        help="Output filename (and path).")
    parser.add_argument('-c', '--ccdlist', action='store', type=str, default="All",
                        help="List of ccds to operate on (default=All).")
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help="Print progress messages to stdout")
    args = parser.parse_args()
    return args

def main(args):
    """ run the splitter """
    if args.verbose:
        print args
    #
    #  Make sure that required arguments are present and appear OK.
    #
    if args.infile is None:
        print "Input filename (-i) is required"
        print "Aborting!"
        return False

    if not os.path.isfile(args.infile):
        print "Missing input file: {:s}".format(args.infile)
        print "Aborting!"
        return False

    if args.outfile is None:
        print "Output filename (-o) is required"
        print "Aborting!"
        return False

    if args.ccdlist == "All":
        ccd_list = range(1, 63)
    else:
        ccd_list = fwsplit(args.ccdlist)

    ccd_list = [int(x) for x in ccd_list]
    status = scamputil.split_ahead_by_ccd(args.infile, args.outfile, ccd_list)
    return status

if __name__ == "__main__":
    if main(parseArgs):
        sys.exit(0)
    else:
        sys.exit(1)
