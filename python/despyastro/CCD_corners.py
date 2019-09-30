#!/usr/bin/env python

# $Id: CCD_corners.py 44632 2016-11-28 18:23:25Z felipe $
# $Rev:: 44632                            $:  # Revision of last commit.
# $LastChangedBy:: felipe                 $:  # Author of last commit.
# $LastChangedDate:: 2016-11-28 12:23:25 #$:  # Date of last commit.

import numpy
from despyastro import wcsutil

def update_DESDM_corners(hdr, border=0, get_extent=False, verbose=False, logger=None):
    """ Recompute the corners and center of the CCD """
    mess = "Using header WCS to compute coordinates for CCD center and corners"
    if logger:
        logger.info(mess)
    elif verbose:
        print mess


    # Get the values
    try:
        ra0, dec0, rac1, decc1, rac2, decc2, rac3, decc3, rac4, decc4 = DESDM_corners(hdr, border=border)
    except:
        mess = "WARNING: Problem computing the DESDM_corners(), will return unchanged header"
        if logger:
            logger.info(mess)
        else:
            print mess
        return hdr


    # Build a list  of records in format accepted by fitsio
    reclist = [{'name': 'RA_CENT',
                'value': ra0,
                'comment':'RA center'},
               {'name': 'DEC_CENT',
                'value': dec0,
                'comment':'DEC center'},
               {'name': 'RAC1',
                'value': rac1,
                'comment':'RA corner 1'},
               {'name': 'DECC1',
                'value': decc1,
                'comment':'DEC corner 1'},
               {'name': 'RAC2',
                'value': rac2,
                'comment':'RA corner 2'},
               {'name': 'DECC2',
                'value': decc2,
                'comment':'DEC corner 2'},
               {'name': 'RAC3',
                'value': rac3,
                'comment':'RA corner 3'},
               {'name': 'DECC3',
                'value': decc3,
                'comment':'DEC corner 3'},
               {'name': 'RAC4',
                'value': rac4,
                'comment':'RA corner 4'},
               {'name': 'DECC4',
                'value': decc4,
                'comment':'DEC corner 4'}
              ]

    # Compute RA/DEC MINMAC and where RA crosses zero.
    if get_extent:
        ras = numpy.array([rac1, rac2, rac3, rac4])
        decs = numpy.array([decc1, decc2, decc3, decc4])
        try:
            RACMIN, RACMAX, DECCMIN, DECCMAX, CROSSRA0 = get_DESDM_corners_extent(ras, decs)
        except:
            mess = "WARNING: Problem computing the get_DESDM_corners_extent(), will return unchanged header"
            if loggger:
                logger.info(mess)
            else:
                print mess
            return hdr

        reclist += [{'name': 'RACMIN',
                     'value': RACMIN,
                     'comment':'Minimum extent of image in RA'},
                    {'name': 'RACMAX',
                     'value': RACMAX,
                     'comment':'Maximum extent of image in RA'},
                    {'name': 'DECCMIN',
                     'value': DECCMIN,
                     'comment':'Minimum extent of image in Declination'},
                    {'name': 'DECCMAX',
                     'value': DECCMAX,
                     'comment':'Maximum extent of image in Declination'},
                    {'name': 'CROSSRA0',
                     'value': CROSSRA0,
                     'comment':'Does Image Span RA 0h (Y/N)'},
                   ]

    # Add each of the records to the header
    for rec in reclist:
        hdr.add_record(rec)

    if verbose:
        print " RA_CEN,DEC_CEN = {:12.7f},{:12.7f} ".format(hdr['RA_CENT'], hdr['DEC_CENT'])
        print "     RAC1,DECC1 = {:12.7f},{:12.7f} ".format(hdr['RAC1'], hdr['DECC1'])
        print "     RAC2,DECC2 = {:12.7f},{:12.7f} ".format(hdr['RAC2'], hdr['DECC2'])
        print "     RAC3,DECC3 = {:12.7f},{:12.7f} ".format(hdr['RAC3'], hdr['DECC3'])
        print "     RAC4,DECC4 = {:12.7f},{:12.7f} ".format(hdr['RAC4'], hdr['DECC4'])
        print "  RACMIN,RACMAX = {:12.7f},{:12.7f} ".format(hdr['RACMIN'], hdr['RACMAX'])
        print "DECCMIN,DECCMAX = {:12.7f},{:12.7f} ".format(hdr['DECCMIN'], hdr['DECCMAX'])
        print "       CROSSRA0 = {:s} ".format(hdr['CROSSRA0'])

    return hdr

def get_DESDM_corners_extent(ras, decs):
    """
    Additional quantities for future COADD queries
    Some may not yet be supported in DB.
    """

    # Make sure that they are numpy objetcs
    ras = numpy.array(ras)
    decs = numpy.array(decs)

    racmin = ras.min()
    racmax = ras.max()
    if (racmax - racmin) > 180.:
        # Currently we switch order. Perhaps better to +/-360.0?
        # Note we want the total extent which is not necessarily the maximum and minimum in this case
        ras2 = ras
        wsm = numpy.where(ras < 180.0)
        ras2[wsm] = ras[wsm] + 360.
        CROSSRA0 = 'Y'
        RACMIN = ras2.min()
        RACMAX = ras2.max() - 360
    else:
        CROSSRA0 = 'N'
        RACMIN = ras.min()
        RACMAX = ras.max()

    DECCMIN = decs.min()
    DECCMAX = decs.max()

    return RACMIN, RACMAX, DECCMIN, DECCMAX, CROSSRA0

def DESDM_corners(hdr, border=0):
    """ set the DESDM corners
        DESDM CCD Image Corner Coordinates definitions for DECam
        see: https://desdb.cosmology.illinois.edu/confluence/display/PUB/CCD+Image+Corners+Coordinates


                         x-axis
          (RA4,DEC4)                   (RA3,DEC3)
          Corner 4 +-----------------+ Corner 3
        (1,NAXIS2) |                 | (NAXIS1,NAXIS2)
                   |                 |
                   |                 |
                   |                 |
                   |                 |
                   |                 |
                   |                 |
                   |                 |
                   |                 |   y-axis
                   |                 |
                   |                 |
                   |                 |
                   |                 |
                   |                 |
                   |                 |
         (RA1,DEC1)|                 | (RA2,DEC2)
          Corner 1 +-----------------+ Corner 2
             (1,1)                     (NAXIS1,1)

        For fpacked images, NAXIS1/NAXIS2 do not represent the true
        dimensions of the image, so we need to use ZNAXIS1/ZNAXIS2
        instead when they are present.
    """
    if hdr.get('znaxis1') and  hdr.get('znaxis2'):
        nx = hdr['znaxis1']
        ny = hdr['znaxis2']
    else:
        nx = hdr['naxis1']
        ny = hdr['naxis2']
    wcs = wcsutil.WCS(hdr)
    rac1, decc1 = wcs.image2sky(1 + border, 1 + border)
    rac2, decc2 = wcs.image2sky(nx - border, 1 + border)
    rac3, decc3 = wcs.image2sky(nx - border, ny - border)
    rac4, decc4 = wcs.image2sky(1 + border, ny - border)
    ra0, dec0 = wcs.image2sky(nx / 2.0, ny / 2.0)
    return ra0, dec0, rac1, decc1, rac2, decc2, rac3, decc3, rac4, decc4
