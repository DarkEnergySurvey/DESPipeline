"""
    .. _despyfitsutils-fitsutils:

    **fitsutils**
    -------------

    Miscellaneous generic support functions for FITS files

"""

import re
import os
from astropy.io import fits

import despymisc.miscutils as miscutils


#######################################################################
def combine_cats(incats, outcat):
    """ Combine all input catalogs (each with 3 hdus) into a single FITS file.

        Parameters
        ----------
        incats : str
            Comma separated list of FITS files to combine.

        outcat : str
            The name of the catalog FITS file to create.
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
    """ Split single SCAMP output head file into individual files

        Parameters
        ----------
        head_out : str
            The input SCAMP file name

        head_lst : str
            Comma separated list of filenames to write out the individual
            SCAMP heads to

        Raises
        ------
        ValueError
            If there is a mismatch in the data in the input file or if the
            expected number of outputs is not found in the input file.
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
    """ Get a specific header from an astropy.io.fits.HDUList

        Parameters
        ----------
        hdulist : astropy.io.fits.HDUList
            The list of HDU objects to search

        whichhdu : various
            The HDU being searched for, this can be an int for the HDU index,
            a string for the HDU name, or ``None`` in which case the primary
            HDU is used.

        Returns
        -------
        astropy.io.fits.header
            The requested header

    """
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
    """ Look up the value of `key` from the requested HDU header.

        Parameters
        ----------
        hdulist : astropy.io.fits.HDUList
            The list of HDU objects to search

        key : str
            The keyword whose value is returned.

        whichhdu : various, optional
            The HDU being searched for, this can be an int for the HDU index,
            a string for the HDU name, or ``None`` in which case the primary
            HDU is used. The default is ``None``.

        Returns
        -------
        various
            The value of the requested key word
    """
    ukey = key.upper()

    hdr = get_hdr(hdulist, whichhdu)
    val = hdr[ukey]

    return val

#######################################################################
def get_hdr_extra(hdulist, key, whichhdu=None):
    """ Look up information about `key` in the specified HDU header. Any
        comments and the type of the value of `key` are returned.

        Parameters
        ----------
        hdulist : astropy.io.fits.HDUList
            The list of HDU objects to search

        key : str
            The keyword whose information is returned

        whichhdu : various, optional
            The HDU being searched for, this can be an int for the HDU index,
            a string for the HDU name, or ``None`` in which case the primary
            HDU is used. The default is ``None``.

        Returns
        -------
        tuple
            Contains any comment and the data type.
    """
    ukey = key.upper()

    hdr = get_hdr(hdulist, whichhdu)
    htype = type(hdr[ukey])
    hcomment = hdr.comments[ukey]

    return hcomment, htype

#######################################################################
def get_ldac_imhead_as_cardlist(imhead):
    """ Convert an HDU to a list of Cards.

        Parameters
        ----------
        imhead : astropy.io.fits.HDU
            The HDU to convert

        Returns
        -------
        list
            The cards from the HDU data

    """
    data = imhead.data
    cards = []
    for cd in data[0][0]:
        cards.append(fits.Card.fromstring(cd))
    return cards


#######################################################################
def get_ldac_imhead_as_hdr(imhead):
    """ Convert an HDU to a header

        Parameters
        ----------
        imhead : astropy.io.fits.HDU
            The HDU to convert

        Returns
        -------
        astropy.io.fits.header
            Contains the data from the input.
    """
    hdr = fits.Header(get_ldac_imhead_as_cardlist(imhead))
    return hdr
