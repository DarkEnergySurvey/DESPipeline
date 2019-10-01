"""
    .. _despymisc-create-special-metadata:

    **create-special-metadata**
    ---------------------------

    Specialized functions for computing metadata
"""
import math


VALID_BANDS = ['u', 'g', 'r', 'i', 'z', 'Y', 'VR', 'N964', 'N662']

######################################################################
def fwhm_arcsec(farglist):
#
#   This is derived from "calc_pixscale" in runSExtractor.c.  This python version is different
#   from the original c code in that it checks to see if cd1_1 and cd2_2 are both non-zero, otherwise
#   it skips the calculation of rho_a and rho_b to avoid ZeroDivisionError.
#
    # check number of arguments
    nargs = len(farglist)
    if nargs != 7:
        raise TypeError("fwhm_arcsec() takes exactly 7 arguments (% given)" % nargs)

    # store values in farglist in local variables
    fwhm = float(farglist[0])
    cd1_1 = float(farglist[1])
    cd1_2 = float(farglist[2])
    cd2_1 = float(farglist[3])
    cd2_2 = float(farglist[4])
    pixscale1 = float(farglist[5])
    pixscale2 = float(farglist[6])

    flag_pixscale_exist = False

    # if the pixscal keywords exist, then take the average
    if pixscale1 != 0.0 and pixscale1 != 0.0:
        pixscale_tem = 0.5 * (pixscale1 + pixscale2)
        flag_pixscale_exist = True

    # evaluate rho_a and rho_b as in Calabretta & Greisen (2002), eq 191
    flag_cd11_or_cd22_zero = False
    if cd1_1 == 0 or cd2_2 == 0:
        flag_cd11_or_cd22_zero = True
    else:
        if cd2_1 > 0:
            rho_a = math.atan(cd2_1 / cd1_1)
        elif cd2_1 < 0:
            rho_a = math.atan(-cd2_1 / -cd1_1)
        else:
            rho_a = 0.0

        if cd1_2 > 0:
            rho_b = math.atan(cd1_2 / -cd2_2)
        elif cd1_2 < 0:
            rho_b = math.atan(-cd1_2 / cd2_2)
        else:
            rho_b = 0.0

        # evaluate rho and CDELTi as in Calabretta & Greisen (2002), eq 193
        rho = 0.5 * (math.fabs(rho_a) + math.fabs(rho_b))
        #rho=0.5*(rho_a+rho_b)
        cdelt1 = cd1_1 / math.cos(rho)
        cdelt2 = cd2_2 / math.cos(rho)
        # convert the pixel to arcsec
        pixscale = 0.5 * (math.fabs(cdelt1) + math.fabs(cdelt2)) * 3600

    if flag_pixscale_exist: #check if the pixscale is within 10% of the values given in header
        if not flag_cd11_or_cd22_zero:
            if math.fabs(pixscale_tem - pixscale) / pixscale_tem > 0.10:
                pixscale = pixscale_tem
        else:
            pixscale = pixscale_tem
    elif flag_cd11_or_cd22_zero:
        raise KeyError("pixscale doesn't exist and cd1_1 and/or cd2_2 zero")

    return fwhm * pixscale
