"""
    .. _despyfitsutils-fitsutils:

    **fitsutils**
    -------------

    Miscellaneous generic support functions for fits files

"""

import re
import os
import sys
from astropy.io import fits

import despymisc.miscutils as miscutils


class makeMEF(object):

    """
    A Class to create a MEF fits files using astropy.io.fits
    """

    # -----------------------------------
    # Translator for DES_EXT, being nice.
    DES_EXT = {}
    DES_EXT['SCI'] = 'IMAGE'
    DES_EXT['WGT'] = 'WEIGHT'
    DES_EXT['MSK'] = 'MASK'
    # -----------------------

    def __init__(self, **kwargs):


        self.filenames = kwargs.pop('filenames', False)
        self.outname = kwargs.pop('outname', False)
        self.clobber = kwargs.pop('clobber', False)
        self.extnames = kwargs.pop('extnames', None)
        self.verb = kwargs.pop('verb', False)

        # Make sure that filenames and outname are defined
        if not self.filenames:
            sys.exit("ERROR: must provide input file names")
        if not self.outname:
            sys.exit("ERROR: must provide output file name")

        # Output file exits
        if os.path.isfile(self.outname) and self.clobber is False:
            print " [WARNING]: Output file exists, try --clobber option, no file was created"
            return

        self.read()
        if self.extnames:
            self.addEXTNAME()
        self.write()

        return

    def addEXTNAME(self):

        """Add a user-provided list of extension names to the MEF"""

        if len(self.extnames) != len(self.filenames):
            sys.exit("ERROR: number of extension names doesn't match filenames")
            return

        k = 0
        for extname, hdu in zip(self.extnames, self.HDU):
            if self.verb:
                print "# Adding EXTNAME=%s to HDU %s" % (extname, k)
            hdu[0].header.set('EXTNAME', extname, 'Extension Name', after='NAXIS2')
            if extname in makeMEF.DES_EXT.keys():
                hdu[0].header.set('DES_EXT', makeMEF.DES_EXT[extname], 'DESDM Extension Name', after='EXTNAME')

            k += 1

    def read(self):

        """ Read in the HDUs """
        self.HDU = []
        k = 0
        for fname in self.filenames:
            if self.verb:
                print "# Reading %s --> HDU %s" % (fname, k)
            self.HDU.append(fits.open(fname))
            k = k + 1

    def write(self):

        """ Write MEF file with no Primary HDU """
        newhdu = fits.HDUList()

        for hdu in self.HDU:
            newhdu.append(hdu[0])# ,hdu[0].header)
        if self.verb:
            print "# Writing to: %s" % self.outname
        newhdu.writeto(self.outname, overwrite=self.clobber)


#######################################################################
def combine_cats(incats, outcat):
    """
    Combine all input catalogs (each with 3 hdus) into a single fits file
    """

    # if incats is comma-separated list, split into python list
    comma_re = re.compile(r"\s*,\s*")
    incat_lst = comma_re.split(incats)

    if miscutils.fwdebug_check(3, 'FITSUTILS_DEBUG'):
        miscutils.fwdebug_print("Constructing hdulist object for single fits file")
    # Construct hdulist object to append hdus from individual catalogs to
    hdulist = fits.HDUList()

    # Now append the hdus from each input catalog file to the hdulist
    for incat in incat_lst:
        if miscutils.fwdebug_check(3, 'FITSUTILS_DEBUG'):
            miscutils.fwdebug_print("Appending 3 HDUs from cat --> %s" % incat)
        hdulist1 = fits.open(incat, mode='readonly')
        hdulist.append(hdulist1[0])
        hdulist.append(hdulist1[1])
        hdulist.append(hdulist1[2])
        #hdulist1.close()

    # And write the full hdulist to the output file
    if os.path.exists(outcat):
        os.remove(outcat)
        miscutils.fwdebug_print("Removing pre-existing version of fullcat %s" % outcat)

    if miscutils.fwdebug_check(3, 'FITSUTILS_DEBUG'):
        miscutils.fwdebug_print("Writing results to fullcat --> %s" % outcat)
    hdulist.writeto(outcat)

    if miscutils.fwdebug_check(6, 'FITSUTILS_DEBUG'):
        miscutils.fwdebug_print("Using fits_close to close fullcat --> %s" % outcat)
    hdulist.close()


def splitScampHead(head_out, heads):
    """
    Split single SCAMP output head file into individual files
      head_out:  SCAMP output
      head_lst:  list of filenames to use for individual files
      reqheadcount: expected number of individual head files
    """

    comma_re = re.compile(r"\s*,\s*")
    head_lst = comma_re.split(heads)
    reqheadcount = len(head_lst)
    headcount = 0
    endcount = 0
    linecount = 0
    linecount_tot = 0
    filehead = None
    for line in open(head_out, 'r'):
        if re.match("^HISTORY   Astrometric solution by SCAMP.*", line):
            if filehead != None:
                filehead.close()
                if miscutils.fwdebug_check(3, 'FITSUTILS_DEBUG'):
                    miscutils.fwdebug_print("Closing .head file after writing %d lines." % linecount)
                if endcount != headcount:
                    miscutils.fwdebug_print("Error: problem when writing %s" % head_lst[headcount])
                    raise ValueError("Number of END lines (%d) does not match number of HISTORY lines (%d)" % (endcount, headcount))
            if miscutils.fwdebug_check(3, 'FITSUTILS_DEBUG'):
                miscutils.fwdebug_print("Opening .head file %d --> %s" % (headcount, head_lst[headcount]))
            filehead = open(head_lst[headcount], 'w')
            headcount += 1
            linecount = 0
        elif re.match(r"^END\s*", line):
            endcount += 1
        filehead.write(line)
        linecount += 1
        linecount_tot += 1
    filehead.close()

    if endcount != headcount:
        miscutils.fwdebug_print("Error: problem when writing %s" % head_lst[headcount])
        raise ValueError("Number of END lines (%d) does not match number of HISTORY lines (%d)" % \
                         (endcount, headcount))

    if miscutils.fwdebug_check(3, 'FITSUTILS_DEBUG'):
        miscutils.fwdebug_print("Closing .head file after writing %d lines.\n" % linecount)

    if headcount != reqheadcount:
        raise ValueError("Number of head files made (%d) does not match required number of head files (%d)" % (headcount, reqheadcount))




#######################################################################
def get_hdr(hdulist, whichhdu):
    """ doc """
    if whichhdu is None:
        whichhdu = 'Primary'

    try:
        whichhdu = int(whichhdu)  # if number, convert type
    except ValueError:
        whichhdu = whichhdu.upper()

    hdr = None
    if whichhdu == 'LDAC_IMHEAD':
        hdr = get_ldac_imhead_as_hdr(hdulist['LDAC_IMHEAD'])
    else:
        try:
            hdr = hdulist[whichhdu].header
        except KeyError:
            if hdulist[0].header['EXTNAME'] == whichhdu:
                hdr = hdulist[0].header
    return hdr


#######################################################################
def get_hdr_value(hdulist, key, whichhdu=None):
    """ doc """
    ukey = key.upper()

    hdr = get_hdr(hdulist, whichhdu)
    val = hdr[ukey]

    return val

#######################################################################
def get_hdr_extra(hdulist, key, whichhdu=None):
    """ doc """
    ukey = key.upper()

    hdr = get_hdr(hdulist, whichhdu)
    htype = type(hdr[ukey])
    hcomment = hdr.comments[ukey]

    return hcomment, htype

#######################################################################
def get_ldac_imhead_as_cardlist(imhead):
    """ doc """
    data = imhead.data
    cards = []
    for cd in data[0][0]:
        cards.append(fits.Card.fromstring(cd))
    return cards


#######################################################################
def get_ldac_imhead_as_hdr(imhead):
    """ doc """
    hdr = fits.Header(get_ldac_imhead_as_cardlist(imhead))
    return hdr
