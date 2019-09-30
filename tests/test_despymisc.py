#!/usr/bin/env python2
import unittest
from contextlib import contextmanager
from collections import OrderedDict
import sys
import time
import errno
import signal
import psutil
import os
import copy

from StringIO import StringIO
from mock import patch, mock_open
from despymisc.xmlslurp import Xmlslurper
import despymisc.subprocess4 as sub4
import despymisc.scamputil as scu
import despymisc.misctime as mt
import despymisc.create_special_metadata as csm
import despymisc.http_requests as hrq
import despymisc.miscutils as mut
import despymisc.provdefs as provdefs
import split_ahead_by_ccd as sabc
import remove_duplicates_from_list as rdfl

FILENAME = 'test.ahead'
@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

def makeAheadFile():
    f = open(FILENAME, 'w')
    f.write("""CCDNUM  =                    1 / CCD number
BAND    = 'g       '           / Band
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   1.342320000000E+04 / Reference pixel on this axis
CRPIX2  =   6.307333000000E+03 / Reference pixel on this axis
HISTORY = 'blah'               / History
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   1.702666640427E-03 / Projection distortion parameter
PV1_1   =   1.014791674198E+00 / Projection distortion parameter
PV1_2   =  -2.476905090418E-03 / Projection distortion parameter
PV1_4   =   7.863368840022E-03 / Projection distortion parameter
PV1_5   =  -1.979051788641E-02 / Projection distortion parameter
PV1_6   =   1.904172784479E-03 / Projection distortion parameter
PV1_7   =   2.087520149181E-04 / Projection distortion parameter
PV1_8   =  -7.273769983855E-03 / Projection distortion parameter
PV1_9   =   7.872055622797E-03 / Projection distortion parameter
PV1_10  =  -5.311615314207E-04 / Projection distortion parameter
PV2_0   =  -3.106132502177E-03 / Projection distortion parameter
PV2_1   =   1.013893660495E+00 / Projection distortion parameter
PV2_2   =  -1.024868308565E-02 / Projection distortion parameter
PV2_4   =  -1.626066870652E-02 / Projection distortion parameter
PV2_5   =   2.024741215737E-02 / Projection distortion parameter
PV2_6   =  -1.122631616274E-02 / Projection distortion parameter
PV2_7   =   6.422711612000E-03 / Projection distortion parameter
PV2_8   =  -1.015554463106E-02 / Projection distortion parameter
PV2_9   =   8.962633753844E-03 / Projection distortion parameter
PV2_10  =  -2.654354928400E-03 / Projection distortion parameter
END
CCDNUM  =                    3 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   1.342320000000E+04 / Reference pixel on this axis
CRPIX2  =  -2.211333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
FLXSCALE=   2.500000000000E-03 / Flux scale
PV1_0   =  -4.077178781650E-03 / Projection distortion parameter
PV1_1   =   1.014810635193E+00 / Projection distortion parameter
PV1_2   =   1.013988841311E-02 / Projection distortion parameter
PV1_4   =  -8.238255965512E-03 / Projection distortion parameter
PV1_5   =  -1.965132642103E-02 / Projection distortion parameter
PV1_6   =  -1.036758618139E-02 / Projection distortion parameter
PV1_7   =   2.295169096299E-05 / Projection distortion parameter
PV1_8   =   7.804390558384E-03 / Projection distortion parameter
PV1_9   =   7.703940442989E-03 / Projection distortion parameter
PV1_10  =   3.607922426785E-03 / Projection distortion parameter
PV2_0   =  -7.789802476517E-04 / Projection distortion parameter
PV2_1   =   1.005843019366E+00 / Projection distortion parameter
PV2_2   =   9.375931859537E-03 / Projection distortion parameter
PV2_4   =  -6.662955642768E-03 / Projection distortion parameter
PV2_5   =  -1.784704015185E-02 / Projection distortion parameter
PV2_6   =  -1.191619020466E-02 / Projection distortion parameter
PV2_7   =   2.638218282759E-03 / Projection distortion parameter
PV2_8   =   8.740108791846E-03 / Projection distortion parameter
PV2_9   =   9.218189532538E-03 / Projection distortion parameter
PV2_10  =   3.149285818261E-03 / Projection distortion parameter
END
CCDNUM  =                    4 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   1.116880000000E+04 / Reference pixel on this axis
CRPIX2  =   8.437000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   6.171554926358E-03 / Projection distortion parameter
PV1_1   =   1.017641707992E+00 / Projection distortion parameter
PV1_2   =  -1.636424629185E-02 / Projection distortion parameter
PV1_4   =   1.452083453970E-02 / Projection distortion parameter
PV1_5   =  -2.038930299310E-02 / Projection distortion parameter
PV1_6   =   1.893207720912E-02 / Projection distortion parameter
PV1_7   =   2.992504259665E-03 / Projection distortion parameter
PV1_8   =  -1.118947301510E-02 / Projection distortion parameter
PV1_9   =   6.456942317474E-03 / Projection distortion parameter
PV1_10  =  -7.753088478146E-03 / Projection distortion parameter
PV2_0   =  -6.923036345353E-03 / Projection distortion parameter
PV2_1   =   1.025434104073E+00 / Projection distortion parameter
PV2_2   =  -1.007784726832E-02 / Projection distortion parameter
PV2_4   =  -2.581579542509E-02 / Projection distortion parameter
PV2_5   =   2.281436988398E-02 / Projection distortion parameter
PV2_6   =  -8.066330723027E-03 / Projection distortion parameter
PV2_7   =   8.328310653635E-03 / Projection distortion parameter
PV2_8   =  -1.388858280562E-02 / Projection distortion parameter
PV2_9   =   4.848238913138E-03 / Projection distortion parameter
PV2_10  =  -3.001608839639E-03 / Projection distortion parameter
END
CCDNUM  =                    5 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   1.116880000000E+04 / Reference pixel on this axis
CRPIX2  =   4.177667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   8.001455540581E-04 / Projection distortion parameter
PV1_1   =   1.008131872844E+00 / Projection distortion parameter
PV1_2   =  -9.512393315364E-04 / Projection distortion parameter
PV1_4   =   1.664368623865E-03 / Projection distortion parameter
PV1_5   =  -7.427842099010E-03 / Projection distortion parameter
PV1_6   =   5.824258565962E-04 / Projection distortion parameter
PV1_7   =  -2.711252903228E-03 / Projection distortion parameter
PV1_8   =  -2.231801473890E-03 / Projection distortion parameter
PV1_9   =   1.946669074798E-03 / Projection distortion parameter
PV1_10  =  -2.258480997618E-04 / Projection distortion parameter
PV2_0   =  -1.593575936209E-03 / Projection distortion parameter
PV2_1   =   1.008013049040E+00 / Projection distortion parameter
PV2_2   =  -1.871257867421E-03 / Projection distortion parameter
PV2_4   =  -5.945320989899E-03 / Projection distortion parameter
PV2_5   =   5.479358382361E-03 / Projection distortion parameter
PV2_6   =  -4.330250464701E-03 / Projection distortion parameter
PV2_7   =   8.580001244460E-04 / Projection distortion parameter
PV2_8   =  -3.355346795955E-03 / Projection distortion parameter
PV2_9   =   2.205721763514E-03 / Projection distortion parameter
PV2_10  =  -1.274269826824E-03 / Projection distortion parameter
END
CCDNUM  =                    6 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   1.116880000000E+04 / Reference pixel on this axis
CRPIX2  =  -8.166665000000E+01 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.053210002141E-03 / Projection distortion parameter
PV1_1   =   1.007996610272E+00 / Projection distortion parameter
PV1_2   =   2.199068347681E-03 / Projection distortion parameter
PV1_4   =  -1.872026636561E-03 / Projection distortion parameter
PV1_5   =  -7.140667259720E-03 / Projection distortion parameter
PV1_6   =  -2.478688528200E-03 / Projection distortion parameter
PV1_7   =  -2.600778767789E-03 / Projection distortion parameter
PV1_8   =   2.420199470571E-03 / Projection distortion parameter
PV1_9   =   1.857211608438E-03 / Projection distortion parameter
PV1_10  =   1.056500204509E-03 / Projection distortion parameter
PV2_0   =  -1.382560750987E-03 / Projection distortion parameter
PV2_1   =   1.007245348794E+00 / Projection distortion parameter
PV2_2   =   1.391048157145E-03 / Projection distortion parameter
PV2_4   =  -5.183517888761E-03 / Projection distortion parameter
PV2_5   =  -4.609629528472E-03 / Projection distortion parameter
PV2_6   =  -2.860811623513E-03 / Projection distortion parameter
PV2_7   =   6.287326808815E-04 / Projection distortion parameter
PV2_8   =   3.221693706359E-03 / Projection distortion parameter
PV2_9   =   6.831463149483E-04 / Projection distortion parameter
PV2_10  =   7.443355272871E-04 / Projection distortion parameter
END
CCDNUM  =                    7 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   1.116880000000E+04 / Reference pixel on this axis
CRPIX2  =  -4.341000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -4.350486871770E-03 / Projection distortion parameter
PV1_1   =   1.017059747179E+00 / Projection distortion parameter
PV1_2   =   8.755812684998E-03 / Projection distortion parameter
PV1_4   =  -1.278054907125E-02 / Projection distortion parameter
PV1_5   =  -2.082019089335E-02 / Projection distortion parameter
PV1_6   =  -8.496127011399E-03 / Projection distortion parameter
PV1_7   =   2.345062610420E-03 / Projection distortion parameter
PV1_8   =   9.965486593570E-03 / Projection distortion parameter
PV1_9   =   7.465232588198E-03 / Projection distortion parameter
PV1_10  =   2.793395301294E-03 / Projection distortion parameter
PV2_0   =  -4.093651511715E-03 / Projection distortion parameter
PV2_1   =   1.012717939540E+00 / Projection distortion parameter
PV2_2   =   1.208840864424E-02 / Projection distortion parameter
PV2_4   =  -7.676664330576E-03 / Projection distortion parameter
PV2_5   =  -2.477454984642E-02 / Projection distortion parameter
PV2_6   =  -1.048282717191E-02 / Projection distortion parameter
PV2_7   =   4.782685902048E-05 / Projection distortion parameter
PV2_8   =   1.372995434011E-02 / Projection distortion parameter
PV2_9   =   7.017475071030E-03 / Projection distortion parameter
PV2_10  =   3.534328516793E-03 / Projection distortion parameter
END
CCDNUM  =                    8 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   8.914400000000E+03 / Reference pixel on this axis
CRPIX2  =   1.056667000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   4.946678203989E-03 / Projection distortion parameter
PV1_1   =   1.017498755449E+00 / Projection distortion parameter
PV1_2   =  -8.415504420570E-03 / Projection distortion parameter
PV1_4   =   1.534059766754E-02 / Projection distortion parameter
PV1_5   =  -1.824571684879E-02 / Projection distortion parameter
PV1_6   =   7.660576132164E-03 / Projection distortion parameter
PV1_7   =   3.776682054715E-03 / Projection distortion parameter
PV1_8   =  -1.018500998521E-02 / Projection distortion parameter
PV1_9   =   5.707785864389E-03 / Projection distortion parameter
PV1_10  =  -2.548426870571E-03 / Projection distortion parameter
PV2_0   =  -3.824345116231E-03 / Projection distortion parameter
PV2_1   =   1.012892017252E+00 / Projection distortion parameter
PV2_2   =  -9.579556377312E-03 / Projection distortion parameter
PV2_4   =  -6.828511689219E-03 / Projection distortion parameter
PV2_5   =   1.899934949343E-02 / Projection distortion parameter
PV2_6   =  -1.049056435204E-02 / Projection distortion parameter
PV2_7   =  -1.271129496798E-03 / Projection distortion parameter
PV2_8   =  -1.061623890868E-02 / Projection distortion parameter
PV2_9   =   5.759215246844E-03 / Projection distortion parameter
PV2_10  =  -4.021646771716E-03 / Projection distortion parameter
END
CCDNUM  =                    9 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   8.914400000000E+03 / Reference pixel on this axis
CRPIX2  =   6.307333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   9.869816112348E-04 / Projection distortion parameter
PV1_1   =   1.006927732462E+00 / Projection distortion parameter
PV1_2   =  -2.392060128360E-03 / Projection distortion parameter
PV1_4   =   2.175990262762E-03 / Projection distortion parameter
PV1_5   =  -3.600252762842E-03 / Projection distortion parameter
PV1_6   =   4.835136262000E-03 / Projection distortion parameter
PV1_7   =  -2.327321488017E-03 / Projection distortion parameter
PV1_8   =  -2.610083313227E-03 / Projection distortion parameter
PV1_9   =  -9.527108363527E-04 / Projection distortion parameter
PV1_10  =  -2.735386098762E-03 / Projection distortion parameter
PV2_0   =  -1.970882028129E-03 / Projection distortion parameter
PV2_1   =   1.008765246834E+00 / Projection distortion parameter
PV2_2   =  -2.778280328136E-03 / Projection distortion parameter
PV2_4   =  -5.362136533701E-03 / Projection distortion parameter
PV2_5   =   6.128774825955E-03 / Projection distortion parameter
PV2_6   =  -2.740934106701E-03 / Projection distortion parameter
PV2_7   =  -3.422806382298E-04 / Projection distortion parameter
PV2_8   =  -4.610934890286E-03 / Projection distortion parameter
PV2_9   =  -3.098731352155E-04 / Projection distortion parameter
PV2_10  =  -1.379545046092E-03 / Projection distortion parameter
END
CCDNUM  =                   10 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   8.914400000000E+03 / Reference pixel on this axis
CRPIX2  =   2.048000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   3.032359587615E-04 / Projection distortion parameter
PV1_1   =   1.006361172193E+00 / Projection distortion parameter
PV1_2   =  -1.407057739581E-03 / Projection distortion parameter
PV1_4   =  -5.613880949224E-04 / Projection distortion parameter
PV1_5   =  -2.905145601904E-03 / Projection distortion parameter
PV1_6   =   2.165433157206E-03 / Projection distortion parameter
PV1_7   =  -3.522073008982E-03 / Projection distortion parameter
PV1_8   =   9.392728391660E-04 / Projection distortion parameter
PV1_9   =  -8.538482424429E-04 / Projection distortion parameter
PV1_10  =  -1.300127024430E-03 / Projection distortion parameter
PV2_0   =  -9.476012693585E-04 / Projection distortion parameter
PV2_1   =   1.004676612349E+00 / Projection distortion parameter
PV2_2   =   2.944360074353E-04 / Projection distortion parameter
PV2_4   =   8.794937554456E-04 / Projection distortion parameter
PV2_5   =  -4.394044964579E-04 / Projection distortion parameter
PV2_6   =  -1.577770678267E-03 / Projection distortion parameter
PV2_7   =  -3.549301939431E-03 / Projection distortion parameter
PV2_8   =   3.906759246538E-04 / Projection distortion parameter
PV2_9   =  -9.361634877205E-04 / Projection distortion parameter
PV2_10  =   4.222615756322E-04 / Projection distortion parameter
END
CCDNUM  =                   11 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   8.914400000000E+03 / Reference pixel on this axis
CRPIX2  =  -2.211333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.265813027765E-03 / Projection distortion parameter
PV1_1   =   1.007692759776E+00 / Projection distortion parameter
PV1_2   =   1.521428788508E-03 / Projection distortion parameter
PV1_4   =  -2.878419163534E-03 / Projection distortion parameter
PV1_5   =  -5.445612339290E-03 / Projection distortion parameter
PV1_6   =  -1.796571578916E-03 / Projection distortion parameter
PV1_7   =  -2.329085553036E-03 / Projection distortion parameter
PV1_8   =   3.813065751288E-03 / Projection distortion parameter
PV1_9   =  -5.006199606618E-06 / Projection distortion parameter
PV1_10  =   8.093000154716E-04 / Projection distortion parameter
PV2_0   =  -6.988240811750E-04 / Projection distortion parameter
PV2_1   =   1.002746610276E+00 / Projection distortion parameter
PV2_2   =   3.075598076607E-03 / Projection distortion parameter
PV2_4   =   5.793256189683E-03 / Projection distortion parameter
PV2_5   =  -8.404633971908E-03 / Projection distortion parameter
PV2_6   =  -3.689134051307E-03 / Projection distortion parameter
PV2_7   =  -7.051184886181E-03 / Projection distortion parameter
PV2_8   =   5.737970357390E-03 / Projection distortion parameter
PV2_9   =   1.417655006953E-03 / Projection distortion parameter
PV2_10  =   1.264065441136E-03 / Projection distortion parameter
END
CCDNUM  =                   12 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   8.914400000000E+03 / Reference pixel on this axis
CRPIX2  =  -6.470667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -6.322980842279E-03 / Projection distortion parameter
PV1_1   =   1.020366821051E+00 / Projection distortion parameter
PV1_2   =   1.268460392898E-02 / Projection distortion parameter
PV1_4   =  -1.832325790551E-02 / Projection distortion parameter
PV1_5   =  -2.135289874297E-02 / Projection distortion parameter
PV1_6   =  -1.362748245447E-02 / Projection distortion parameter
PV1_7   =   4.850347039196E-03 / Projection distortion parameter
PV1_8   =   1.181898949165E-02 / Projection distortion parameter
PV1_9   =   6.391409132639E-03 / Projection distortion parameter
PV1_10  =   5.716993320353E-03 / Projection distortion parameter
PV2_0   =  -3.434910660209E-03 / Projection distortion parameter
PV2_1   =   1.010100634303E+00 / Projection distortion parameter
PV2_2   =   1.018943616429E-02 / Projection distortion parameter
PV2_4   =  -1.067312122971E-03 / Projection distortion parameter
PV2_5   =  -2.076940708375E-02 / Projection distortion parameter
PV2_6   =  -1.017112696523E-02 / Projection distortion parameter
PV2_7   =  -4.959295162883E-03 / Projection distortion parameter
PV2_8   =   1.190276365379E-02 / Projection distortion parameter
PV2_9   =   5.849749290131E-03 / Projection distortion parameter
PV2_10  =   3.772184034489E-03 / Projection distortion parameter
END
CCDNUM  =                   13 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   6.660000000000E+03 / Reference pixel on this axis
CRPIX2  =   1.269633000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   7.806444785225E-03 / Projection distortion parameter
PV1_1   =   1.027272241642E+00 / Projection distortion parameter
PV1_2   =  -1.137106344734E-02 / Projection distortion parameter
PV1_4   =   2.829795353616E-02 / Projection distortion parameter
PV1_5   =  -2.155068734186E-02 / Projection distortion parameter
PV1_6   =   1.240101743936E-02 / Projection distortion parameter
PV1_7   =   9.688237527883E-03 / Projection distortion parameter
PV1_8   =  -1.141946893989E-02 / Projection distortion parameter
PV1_9   =   7.697675123127E-03 / Projection distortion parameter
PV1_10  =  -4.792662069675E-03 / Projection distortion parameter
PV2_0   =  -3.318582768195E-03 / Projection distortion parameter
PV2_1   =   1.009460053525E+00 / Projection distortion parameter
PV2_2   =  -8.846535630460E-03 / Projection distortion parameter
PV2_4   =   3.274823749025E-04 / Projection distortion parameter
PV2_5   =   1.504659725136E-02 / Projection distortion parameter
PV2_6   =  -1.004234521345E-02 / Projection distortion parameter
PV2_7   =  -6.261982490477E-03 / Projection distortion parameter
PV2_8   =  -6.651664598782E-03 / Projection distortion parameter
PV2_9   =   5.605256995776E-03 / Projection distortion parameter
PV2_10  =  -3.824393487372E-03 / Projection distortion parameter
END
CCDNUM  =                   14 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   6.660000000000E+03 / Reference pixel on this axis
CRPIX2  =   8.437000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   1.336464883270E-03 / Projection distortion parameter
PV1_1   =   1.007828521975E+00 / Projection distortion parameter
PV1_2   =  -1.673217978346E-03 / Projection distortion parameter
PV1_4   =   4.304422981360E-03 / Projection distortion parameter
PV1_5   =  -3.796041814612E-03 / Projection distortion parameter
PV1_6   =   3.020147416431E-03 / Projection distortion parameter
PV1_7   =  -6.201993019262E-04 / Projection distortion parameter
PV1_8   =  -2.443782938276E-03 / Projection distortion parameter
PV1_9   =  -2.696563845969E-04 / Projection distortion parameter
PV1_10  =  -1.649696586291E-03 / Projection distortion parameter
PV2_0   =  -1.642188987656E-03 / Projection distortion parameter
PV2_1   =   1.009598734103E+00 / Projection distortion parameter
PV2_2   =  -2.143460972309E-03 / Projection distortion parameter
PV2_4   =  -6.938298088524E-03 / Projection distortion parameter
PV2_5   =   6.531973184095E-03 / Projection distortion parameter
PV2_6   =  -2.258578251507E-03 / Projection distortion parameter
PV2_7   =   3.672845916157E-04 / Projection distortion parameter
PV2_8   =  -5.725882576940E-03 / Projection distortion parameter
PV2_9   =  -5.601829894834E-04 / Projection distortion parameter
PV2_10  =  -1.065942727741E-03 / Projection distortion parameter
END
CCDNUM  =                   15 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   6.660000000000E+03 / Reference pixel on this axis
CRPIX2  =   4.177667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   4.849238616643E-04 / Projection distortion parameter
PV1_1   =   1.005901564430E+00 / Projection distortion parameter
PV1_2   =  -1.092795674678E-03 / Projection distortion parameter
PV1_4   =   5.128634664751E-04 / Projection distortion parameter
PV1_5   =  -1.106188904216E-03 / Projection distortion parameter
PV1_6   =   2.243752851657E-03 / Projection distortion parameter
PV1_7   =  -2.904625002574E-03 / Projection distortion parameter
PV1_8   =  -2.885178301798E-04 / Projection distortion parameter
PV1_9   =  -2.428480006544E-03 / Projection distortion parameter
PV1_10  =  -1.765462958246E-03 / Projection distortion parameter
PV2_0   =  -1.078919570654E-03 / Projection distortion parameter
PV2_1   =   1.008607075872E+00 / Projection distortion parameter
PV2_2   =  -4.569078818179E-04 / Projection distortion parameter
PV2_4   =  -7.428343873135E-03 / Projection distortion parameter
PV2_5   =   2.618854331137E-03 / Projection distortion parameter
PV2_6   =  -3.887953564132E-04 / Projection distortion parameter
PV2_7   =   2.490049114499E-03 / Projection distortion parameter
PV2_8   =  -2.908308970063E-03 / Projection distortion parameter
PV2_9   =  -2.484136263831E-03 / Projection distortion parameter
PV2_10  =   3.310837922737E-04 / Projection distortion parameter
END
CCDNUM  =                   16 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   6.660000000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.166665000000E+01 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -4.132559252347E-04 / Projection distortion parameter
PV1_1   =   1.005796244431E+00 / Projection distortion parameter
PV1_2   =   7.748998260209E-04 / Projection distortion parameter
PV1_4   =  -6.979091916199E-04 / Projection distortion parameter
PV1_5   =  -4.648005678813E-04 / Projection distortion parameter
PV1_6   =  -2.663776825948E-03 / Projection distortion parameter
PV1_7   =  -3.267580370584E-03 / Projection distortion parameter
PV1_8   =   1.199263439735E-03 / Projection distortion parameter
PV1_9   =  -3.442821524714E-03 / Projection distortion parameter
PV1_10  =   2.173642318504E-03 / Projection distortion parameter
PV2_0   =  -8.016797294818E-04 / Projection distortion parameter
PV2_1   =   1.004548194599E+00 / Projection distortion parameter
PV2_2   =   8.581393172238E-04 / Projection distortion parameter
PV2_4   =   2.734627092053E-03 / Projection distortion parameter
PV2_5   =  -2.151538997057E-03 / Projection distortion parameter
PV2_6   =  -9.952252950176E-04 / Projection distortion parameter
PV2_7   =  -5.929918458324E-03 / Projection distortion parameter
PV2_8   =   1.997988378400E-03 / Projection distortion parameter
PV2_9   =  -1.539911445831E-03 / Projection distortion parameter
PV2_10  =   2.151010656275E-04 / Projection distortion parameter
END
CCDNUM  =                   17 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   6.660000000000E+03 / Reference pixel on this axis
CRPIX2  =  -4.341000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.959690073251E-03 / Projection distortion parameter
PV1_1   =   1.008658798041E+00 / Projection distortion parameter
PV1_2   =   3.111203397919E-03 / Projection distortion parameter
PV1_4   =  -5.282520520013E-03 / Projection distortion parameter
PV1_5   =  -5.356836564345E-03 / Projection distortion parameter
PV1_6   =  -4.521254543941E-03 / Projection distortion parameter
PV1_7   =  -3.348286553916E-04 / Projection distortion parameter
PV1_8   =   3.794416793847E-03 / Projection distortion parameter
PV1_9   =  -5.936903034564E-05 / Projection distortion parameter
PV1_10  =   2.613221315052E-03 / Projection distortion parameter
PV2_0   =  -3.618733469795E-04 / Projection distortion parameter
PV2_1   =   1.002881012288E+00 / Projection distortion parameter
PV2_2   =   1.195614713561E-03 / Projection distortion parameter
PV2_4   =   8.274449165765E-03 / Projection distortion parameter
PV2_5   =  -4.409438736289E-03 / Projection distortion parameter
PV2_6   =  -1.981104051659E-03 / Projection distortion parameter
PV2_7   =  -1.133384169167E-02 / Projection distortion parameter
PV2_8   =   4.152792430030E-03 / Projection distortion parameter
PV2_9   =  -1.450775533846E-03 / Projection distortion parameter
PV2_10  =   1.104233552617E-03 / Projection distortion parameter
END
CCDNUM  =                   18 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   6.660000000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.600334000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -6.800122609404E-03 / Projection distortion parameter
PV1_1   =   1.027198671935E+00 / Projection distortion parameter
PV1_2   =   5.784746565343E-03 / Projection distortion parameter
PV1_4   =  -2.880538011709E-02 / Projection distortion parameter
PV1_5   =  -1.862800229497E-02 / Projection distortion parameter
PV1_6   =  -2.300239361764E-03 / Projection distortion parameter
PV1_7   =   1.009831212823E-02 / Projection distortion parameter
PV1_8   =   1.014666299885E-02 / Projection distortion parameter
PV1_9   =   6.308746781085E-03 / Projection distortion parameter
PV1_10  =  -2.550209458481E-03 / Projection distortion parameter
PV2_0   =  -4.117402679513E-03 / Projection distortion parameter
PV2_1   =   1.012583333166E+00 / Projection distortion parameter
PV2_2   =   1.041680254870E-02 / Projection distortion parameter
PV2_4   =  -3.789220372997E-03 / Projection distortion parameter
PV2_5   =  -1.886303914434E-02 / Projection distortion parameter
PV2_6   =  -1.041218367853E-02 / Projection distortion parameter
PV2_7   =  -3.170368063454E-03 / Projection distortion parameter
PV2_8   =   7.273664940367E-03 / Projection distortion parameter
PV2_9   =   7.644469500831E-03 / Projection distortion parameter
PV2_10  =   3.568212545474E-03 / Projection distortion parameter
END
CCDNUM  =                   19 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   4.405600000000E+03 / Reference pixel on this axis
CRPIX2  =   1.269633000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   5.551269046860E-03 / Projection distortion parameter
PV1_1   =   1.021135780741E+00 / Projection distortion parameter
PV1_2   =  -4.215960711980E-03 / Projection distortion parameter
PV1_4   =   2.300926931641E-02 / Projection distortion parameter
PV1_5   =  -1.011662068997E-02 / Projection distortion parameter
PV1_6   =   3.782432474042E-03 / Projection distortion parameter
PV1_7   =   8.183670851242E-03 / Projection distortion parameter
PV1_8   =  -6.673448708063E-03 / Projection distortion parameter
PV1_9   =   1.307183406003E-03 / Projection distortion parameter
PV1_10  =  -1.574521449460E-03 / Projection distortion parameter
PV2_0   =  -1.309820467828E-03 / Projection distortion parameter
PV2_1   =   1.008279106069E+00 / Projection distortion parameter
PV2_2   =  -3.795738946210E-03 / Projection distortion parameter
PV2_4   =   1.933960445229E-03 / Projection distortion parameter
PV2_5   =   1.027098828646E-02 / Projection distortion parameter
PV2_6   =  -4.698304241468E-03 / Projection distortion parameter
PV2_7   =  -8.303931670309E-03 / Projection distortion parameter
PV2_8   =  -3.026654621980E-03 / Projection distortion parameter
PV2_9   =   3.669054320178E-03 / Projection distortion parameter
PV2_10  =  -1.800092885984E-03 / Projection distortion parameter
END
CCDNUM  =                   20 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   4.405600000000E+03 / Reference pixel on this axis
CRPIX2  =   8.437000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   9.867456958861E-04 / Projection distortion parameter
PV1_1   =   1.006351658044E+00 / Projection distortion parameter
PV1_2   =   4.326329474053E-05 / Projection distortion parameter
PV1_4   =   2.007775839297E-03 / Projection distortion parameter
PV1_5   =  -1.091964555693E-03 / Projection distortion parameter
PV1_6   =  -8.488339735464E-05 / Projection distortion parameter
PV1_7   =  -2.025834713866E-03 / Projection distortion parameter
PV1_8   =  -1.505543980228E-03 / Projection distortion parameter
PV1_9   =  -3.295012610709E-03 / Projection distortion parameter
PV1_10  =  -1.209881436812E-05 / Projection distortion parameter
PV2_0   =  -6.241161864667E-04 / Projection distortion parameter
PV2_1   =   1.005019942708E+00 / Projection distortion parameter
PV2_2   =  -2.913882507075E-04 / Projection distortion parameter
PV2_4   =   3.086580723582E-03 / Projection distortion parameter
PV2_5   =   9.134305791087E-04 / Projection distortion parameter
PV2_6   =  -3.141061871566E-04 / Projection distortion parameter
PV2_7   =  -8.712940793272E-03 / Projection distortion parameter
PV2_8   =  -1.622944565154E-03 / Projection distortion parameter
PV2_9   =  -3.144457829430E-03 / Projection distortion parameter
PV2_10  =  -2.801230224966E-04 / Projection distortion parameter
END
CCDNUM  =                   21 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   4.405600000000E+03 / Reference pixel on this axis
CRPIX2  =   4.177667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   2.494066299693E-04 / Projection distortion parameter
PV1_1   =   1.005618262408E+00 / Projection distortion parameter
PV1_2   =   4.180833354887E-04 / Projection distortion parameter
PV1_4   =   1.985943891084E-04 / Projection distortion parameter
PV1_5   =   4.452215969932E-04 / Projection distortion parameter
PV1_6   =  -2.003522129009E-03 / Projection distortion parameter
PV1_7   =  -3.590219432716E-03 / Projection distortion parameter
PV1_8   =  -1.653471431490E-05 / Projection distortion parameter
PV1_9   =  -4.709202719173E-03 / Projection distortion parameter
PV1_10  =   2.437677611796E-03 / Projection distortion parameter
PV2_0   =  -3.324826206675E-04 / Projection distortion parameter
PV2_1   =   1.004934045582E+00 / Projection distortion parameter
PV2_2   =  -4.717354812651E-05 / Projection distortion parameter
PV2_4   =   3.562374812014E-03 / Projection distortion parameter
PV2_5   =   1.481201093474E-03 / Projection distortion parameter
PV2_6   =   1.949740284785E-04 / Projection distortion parameter
PV2_7   =  -9.278070729901E-03 / Projection distortion parameter
PV2_8   =  -3.018769759167E-03 / Projection distortion parameter
PV2_9   =  -4.002951649437E-03 / Projection distortion parameter
PV2_10  =   3.334738531209E-04 / Projection distortion parameter
END
CCDNUM  =                   22 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   4.405600000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.166665000000E+01 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -4.802382795515E-04 / Projection distortion parameter
PV1_1   =   1.005791377021E+00 / Projection distortion parameter
PV1_2   =   6.200505608406E-04 / Projection distortion parameter
PV1_4   =  -5.218790573073E-04 / Projection distortion parameter
PV1_5   =  -6.303908813379E-04 / Projection distortion parameter
PV1_6   =  -1.322475209721E-03 / Projection distortion parameter
PV1_7   =  -3.305049379323E-03 / Projection distortion parameter
PV1_8   =   9.291940546385E-04 / Projection distortion parameter
PV1_9   =  -3.167682897183E-03 / Projection distortion parameter
PV1_10  =   1.618629987955E-03 / Projection distortion parameter
PV2_0   =  -2.188688904995E-04 / Projection distortion parameter
PV2_1   =   1.004886239269E+00 / Projection distortion parameter
PV2_2   =  -2.596926179020E-04 / Projection distortion parameter
PV2_4   =   3.126253494759E-03 / Projection distortion parameter
PV2_5   =   4.473871140220E-04 / Projection distortion parameter
PV2_6   =  -3.749557493804E-04 / Projection distortion parameter
PV2_7   =  -7.817686436040E-03 / Projection distortion parameter
PV2_8   =  -1.789740906807E-03 / Projection distortion parameter
PV2_9   =  -2.479801591491E-03 / Projection distortion parameter
PV2_10  =  -3.241445741926E-05 / Projection distortion parameter
END
CCDNUM  =                   23 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   4.405600000000E+03 / Reference pixel on this axis
CRPIX2  =  -4.341000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -9.314361729386E-04 / Projection distortion parameter
PV1_1   =   1.006142656162E+00 / Projection distortion parameter
PV1_2   =   5.860954586200E-05 / Projection distortion parameter
PV1_4   =  -1.641378109468E-03 / Projection distortion parameter
PV1_5   =  -9.815568246380E-05 / Projection distortion parameter
PV1_6   =  -1.508347141588E-04 / Projection distortion parameter
PV1_7   =  -2.214635352033E-03 / Projection distortion parameter
PV1_8   =   5.267999572555E-04 / Projection distortion parameter
PV1_9   =  -3.447742790173E-03 / Projection distortion parameter
PV1_10  =   1.833814788562E-04 / Projection distortion parameter
PV2_0   =  -4.342426930581E-04 / Projection distortion parameter
PV2_1   =   1.006656249108E+00 / Projection distortion parameter
PV2_2   =   1.155668663090E-04 / Projection distortion parameter
PV2_4   =  -4.523366641944E-03 / Projection distortion parameter
PV2_5   =  -8.383972301371E-05 / Projection distortion parameter
PV2_6   =  -5.723616198518E-04 / Projection distortion parameter
PV2_7   =   3.951757416420E-03 / Projection distortion parameter
PV2_8   =  -1.775878984978E-03 / Projection distortion parameter
PV2_9   =  -2.398058446063E-03 / Projection distortion parameter
PV2_10  =   2.755738075230E-04 / Projection distortion parameter
END
CCDNUM  =                   24 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   4.405600000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.600334000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -5.317098852066E-03 / Projection distortion parameter
PV1_1   =   1.021216446879E+00 / Projection distortion parameter
PV1_2   =   3.581533339242E-03 / Projection distortion parameter
PV1_4   =  -2.310629606251E-02 / Projection distortion parameter
PV1_5   =  -8.860923110588E-03 / Projection distortion parameter
PV1_6   =  -4.373135978223E-03 / Projection distortion parameter
PV1_7   =   8.273001597073E-03 / Projection distortion parameter
PV1_8   =   5.543717004947E-03 / Projection distortion parameter
PV1_9   =   2.684054882094E-03 / Projection distortion parameter
PV1_10  =   9.984505662335E-04 / Projection distortion parameter
PV2_0   =  -1.660396511509E-03 / Projection distortion parameter
PV2_1   =   1.008073542182E+00 / Projection distortion parameter
PV2_2   =   5.035421179989E-03 / Projection distortion parameter
PV2_4   =   3.231431716551E-04 / Projection distortion parameter
PV2_5   =  -8.734985838172E-03 / Projection distortion parameter
PV2_6   =  -6.378659152859E-03 / Projection distortion parameter
PV2_7   =  -5.113677808741E-03 / Projection distortion parameter
PV2_8   =   2.114983715839E-03 / Projection distortion parameter
PV2_9   =   3.004856743929E-03 / Projection distortion parameter
PV2_10  =   2.602830558854E-03 / Projection distortion parameter
END
CCDNUM  =                   25 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   2.151200000000E+03 / Reference pixel on this axis
CRPIX2  =   1.482600000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   1.015959903837E-02 / Projection distortion parameter
PV1_1   =   1.037448613174E+00 / Projection distortion parameter
PV1_2   =  -1.794427837880E-03 / Projection distortion parameter
PV1_4   =   4.286900263086E-02 / Projection distortion parameter
PV1_5   =  -3.494487237680E-03 / Projection distortion parameter
PV1_6   =   5.393407704152E-03 / Projection distortion parameter
PV1_7   =   1.630380951614E-02 / Projection distortion parameter
PV1_8   =  -2.072685314848E-03 / Projection distortion parameter
PV1_9   =   3.838817688057E-03 / Projection distortion parameter
PV1_10  =  -1.631304701798E-03 / Projection distortion parameter
PV2_0   =  -8.248871068974E-04 / Projection distortion parameter
PV2_1   =   1.013154846833E+00 / Projection distortion parameter
PV2_2   =  -1.958912107748E-03 / Projection distortion parameter
PV2_4   =  -3.921132426737E-03 / Projection distortion parameter
PV2_5   =   1.824392112487E-02 / Projection distortion parameter
PV2_6   =  -2.236647191099E-03 / Projection distortion parameter
PV2_7   =  -2.334156572042E-03 / Projection distortion parameter
PV2_8   =  -4.485407680460E-03 / Projection distortion parameter
PV2_9   =   7.800022933543E-03 / Projection distortion parameter
PV2_10  =  -7.517570501120E-04 / Projection distortion parameter
END
CCDNUM  =                   26 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   2.151200000000E+03 / Reference pixel on this axis
CRPIX2  =   1.056667000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   2.172954860801E-03 / Projection distortion parameter
PV1_1   =   1.010255655953E+00 / Projection distortion parameter
PV1_2   =  -3.815600517792E-04 / Projection distortion parameter
PV1_4   =   8.824358169992E-03 / Projection distortion parameter
PV1_5   =  -1.089021936279E-03 / Projection distortion parameter
PV1_6   =   9.360448228936E-04 / Projection distortion parameter
PV1_7   =   2.003469109921E-03 / Projection distortion parameter
PV1_8   =  -8.919008477096E-04 / Projection distortion parameter
PV1_9   =  -1.596667018555E-03 / Projection distortion parameter
PV1_10  =   1.870436503832E-05 / Projection distortion parameter
PV2_0   =  -2.090645159369E-04 / Projection distortion parameter
PV2_1   =   1.006310431398E+00 / Projection distortion parameter
PV2_2   =  -5.512825303608E-04 / Projection distortion parameter
PV2_4   =  -1.534995902508E-03 / Projection distortion parameter
PV2_5   =   2.128971458920E-03 / Projection distortion parameter
PV2_6   =  -9.392832208158E-04 / Projection distortion parameter
PV2_7   =  -1.118721324507E-03 / Projection distortion parameter
PV2_8   =  -1.226906515632E-03 / Projection distortion parameter
PV2_9   =  -1.736678474153E-03 / Projection distortion parameter
PV2_10  =  -4.980229825328E-04 / Projection distortion parameter
END
CCDNUM  =                   27 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   2.151200000000E+03 / Reference pixel on this axis
CRPIX2  =   6.307333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   7.106698275718E-04 / Projection distortion parameter
PV1_1   =   1.005927161352E+00 / Projection distortion parameter
PV1_2   =  -5.113122206259E-05 / Projection distortion parameter
PV1_4   =   9.403237532395E-04 / Projection distortion parameter
PV1_5   =  -4.793068615005E-04 / Projection distortion parameter
PV1_6   =   1.262412966228E-03 / Projection distortion parameter
PV1_7   =  -2.891491008308E-03 / Projection distortion parameter
PV1_8   =  -3.346280664450E-04 / Projection distortion parameter
PV1_9   =  -1.985032378367E-03 / Projection distortion parameter
PV1_10  =  -2.892247890223E-03 / Projection distortion parameter
PV2_0   =  -3.103468572609E-04 / Projection distortion parameter
PV2_1   =   1.005679205790E+00 / Projection distortion parameter
PV2_2   =  -1.646546510137E-04 / Projection distortion parameter
PV2_4   =  -1.494564141724E-04 / Projection distortion parameter
PV2_5   =   9.351791751716E-05 / Projection distortion parameter
PV2_6   =   3.492259364289E-05 / Projection distortion parameter
PV2_7   =   6.227543212258E-06 / Projection distortion parameter
PV2_8   =   2.802538897443E-03 / Projection distortion parameter
PV2_9   =  -2.934305207047E-03 / Projection distortion parameter
PV2_10  =   1.962505499659E-04 / Projection distortion parameter
END
CCDNUM  =                   28 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   2.151200000000E+03 / Reference pixel on this axis
CRPIX2  =   2.048000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   2.623983125037E-04 / Projection distortion parameter
PV1_1   =   1.005638214476E+00 / Projection distortion parameter
PV1_2   =  -4.549351194293E-04 / Projection distortion parameter
PV1_4   =  -1.283999808155E-04 / Projection distortion parameter
PV1_5   =   1.557123357706E-04 / Projection distortion parameter
PV1_6   =   6.590023230155E-04 / Projection distortion parameter
PV1_7   =  -4.204232233227E-03 / Projection distortion parameter
PV1_8   =   1.522930017791E-03 / Projection distortion parameter
PV1_9   =  -4.501568049983E-03 / Projection distortion parameter
PV1_10  =  -2.506106630587E-03 / Projection distortion parameter
PV2_0   =  -8.467042501392E-05 / Projection distortion parameter
PV2_1   =   1.005673147253E+00 / Projection distortion parameter
PV2_2   =   3.637708479791E-04 / Projection distortion parameter
PV2_4   =   3.999304179250E-04 / Projection distortion parameter
PV2_5   =   2.191552426490E-04 / Projection distortion parameter
PV2_6   =  -6.223046163280E-05 / Projection distortion parameter
PV2_7   =  -6.479953545471E-03 / Projection distortion parameter
PV2_8   =  -7.753655720253E-04 / Projection distortion parameter
PV2_9   =  -3.371247634021E-03 / Projection distortion parameter
PV2_10  =   2.680415010989E-04 / Projection distortion parameter
END
CCDNUM  =                   29 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   2.151200000000E+03 / Reference pixel on this axis
CRPIX2  =  -2.211333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -6.609251001728E-04 / Projection distortion parameter
PV1_1   =   1.005828949012E+00 / Projection distortion parameter
PV1_2   =  -2.540072581484E-04 / Projection distortion parameter
PV1_4   =  -6.181270074456E-04 / Projection distortion parameter
PV1_5   =  -2.614923397195E-04 / Projection distortion parameter
PV1_6   =  -4.995098846936E-04 / Projection distortion parameter
PV1_7   =  -3.264173109707E-03 / Projection distortion parameter
PV1_8   =   6.189383807489E-04 / Projection distortion parameter
PV1_9   =  -4.568779887567E-03 / Projection distortion parameter
PV1_10  =   3.134226907193E-03 / Projection distortion parameter
PV2_0   =  -3.176012907504E-04 / Projection distortion parameter
PV2_1   =   1.005419306282E+00 / Projection distortion parameter
PV2_2   =   4.239784820666E-04 / Projection distortion parameter
PV2_4   =   4.681484595168E-03 / Projection distortion parameter
PV2_5   =  -6.765925910775E-04 / Projection distortion parameter
PV2_6   =  -3.287377125942E-04 / Projection distortion parameter
PV2_7   =  -2.275269990724E-02 / Projection distortion parameter
PV2_8   =   5.191534076570E-04 / Projection distortion parameter
PV2_9   =  -2.960101066359E-03 / Projection distortion parameter
PV2_10  =   2.761999203575E-04 / Projection distortion parameter
END
CCDNUM  =                   30 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =   2.151200000000E+03 / Reference pixel on this axis
CRPIX2  =  -6.470667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.966922719254E-03 / Projection distortion parameter
PV1_1   =   1.010378911686E+00 / Projection distortion parameter
PV1_2   =   3.571092361936E-04 / Projection distortion parameter
PV1_4   =  -8.895919368293E-03 / Projection distortion parameter
PV1_5   =  -1.913946192864E-03 / Projection distortion parameter
PV1_6   =  -1.216286399987E-03 / Projection distortion parameter
PV1_7   =   1.986713447172E-03 / Projection distortion parameter
PV1_8   =   1.636097408947E-03 / Projection distortion parameter
PV1_9   =  -1.727480659181E-03 / Projection distortion parameter
PV1_10  =   7.733719344955E-04 / Projection distortion parameter
PV2_0   =  -4.197725431434E-04 / Projection distortion parameter
PV2_1   =   1.006359592569E+00 / Projection distortion parameter
PV2_2   =   8.039420879323E-04 / Projection distortion parameter
PV2_4   =   1.840352180908E-04 / Projection distortion parameter
PV2_5   =  -2.768441582525E-03 / Projection distortion parameter
PV2_6   =  -9.025589632402E-04 / Projection distortion parameter
PV2_7   =  -9.027904542979E-03 / Projection distortion parameter
PV2_8   =   2.010440203624E-03 / Projection distortion parameter
PV2_9   =  -1.274708602120E-03 / Projection distortion parameter
PV2_10  =   4.536926217723E-04 / Projection distortion parameter
END
CCDNUM  =                   32 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.032001000000E+02 / Reference pixel on this axis
CRPIX2  =   1.482600000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   1.055504059256E-02 / Projection distortion parameter
PV1_1   =   1.039065592093E+00 / Projection distortion parameter
PV1_2   =   1.710748107636E-03 / Projection distortion parameter
PV1_4   =   4.458668527022E-02 / Projection distortion parameter
PV1_5   =   2.870250470353E-03 / Projection distortion parameter
PV1_6   =   7.367090278630E-03 / Projection distortion parameter
PV1_7   =   1.691949907182E-02 / Projection distortion parameter
PV1_8   =   1.459268526858E-03 / Projection distortion parameter
PV1_9   =   7.318153730911E-03 / Projection distortion parameter
PV1_10  =  -3.518548219858E-03 / Projection distortion parameter
PV2_0   =   1.883579547727E-04 / Projection distortion parameter
PV2_1   =   1.011425276120E+00 / Projection distortion parameter
PV2_2   =   1.267767884969E-03 / Projection distortion parameter
PV2_4   =  -6.991691104728E-04 / Projection distortion parameter
PV2_5   =   1.549518261816E-02 / Projection distortion parameter
PV2_6   =   1.780368336305E-03 / Projection distortion parameter
PV2_7   =  -1.422537035475E-02 / Projection distortion parameter
PV2_8   =   2.975085155952E-03 / Projection distortion parameter
PV2_9   =   6.581442950231E-03 / Projection distortion parameter
PV2_10  =   6.303209198885E-04 / Projection distortion parameter
END
CCDNUM  =                   33 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.032001000000E+02 / Reference pixel on this axis
CRPIX2  =   1.056667000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   2.136332281481E-03 / Projection distortion parameter
PV1_1   =   1.010458538272E+00 / Projection distortion parameter
PV1_2   =  -1.616795870155E-05 / Projection distortion parameter
PV1_4   =   9.244511407107E-03 / Projection distortion parameter
PV1_5   =  -4.165293928012E-05 / Projection distortion parameter
PV1_6   =   4.557561267631E-04 / Projection distortion parameter
PV1_7   =   2.275209828126E-03 / Projection distortion parameter
PV1_8   =  -4.893608962824E-05 / Projection distortion parameter
PV1_9   =  -1.758363852401E-03 / Projection distortion parameter
PV1_10  =  -2.204876305502E-03 / Projection distortion parameter
PV2_0   =   2.458033808613E-04 / Projection distortion parameter
PV2_1   =   1.006283325674E+00 / Projection distortion parameter
PV2_2   =   6.692377795410E-04 / Projection distortion parameter
PV2_4   =  -1.557053101454E-03 / Projection distortion parameter
PV2_5   =   2.890627100270E-03 / Projection distortion parameter
PV2_6   =   1.116934752590E-03 / Projection distortion parameter
PV2_7   =  -1.030032581283E-02 / Projection distortion parameter
PV2_8   =   1.219042243995E-04 / Projection distortion parameter
PV2_9   =  -9.406234748353E-04 / Projection distortion parameter
PV2_10  =   5.663651550285E-04 / Projection distortion parameter
END
CCDNUM  =                   34 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.032001000000E+02 / Reference pixel on this axis
CRPIX2  =   6.307333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   5.964904672653E-04 / Projection distortion parameter
PV1_1   =   1.005874379312E+00 / Projection distortion parameter
PV1_2   =  -4.447154019962E-04 / Projection distortion parameter
PV1_4   =   8.079902979698E-04 / Projection distortion parameter
PV1_5   =  -1.319262662035E-04 / Projection distortion parameter
PV1_6   =  -5.642706069477E-04 / Projection distortion parameter
PV1_7   =  -3.050114747282E-03 / Projection distortion parameter
PV1_8   =   8.621872473218E-05 / Projection distortion parameter
PV1_9   =  -4.533751309320E-03 / Projection distortion parameter
PV1_10  =  -1.369998880302E-03 / Projection distortion parameter
PV2_0   =   2.539512686717E-04 / Projection distortion parameter
PV2_1   =   1.005492921081E+00 / Projection distortion parameter
PV2_2   =   4.464590297866E-04 / Projection distortion parameter
PV2_4   =  -2.174890637008E-03 / Projection distortion parameter
PV2_5   =  -1.161403546244E-04 / Projection distortion parameter
PV2_6   =   3.117908222510E-04 / Projection distortion parameter
PV2_7   =  -1.084171409689E-02 / Projection distortion parameter
PV2_8   =  -2.106099227521E-03 / Projection distortion parameter
PV2_9   =  -3.440365197007E-03 / Projection distortion parameter
PV2_10  =   2.592306987006E-04 / Projection distortion parameter
END
CCDNUM  =                   35 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.032001000000E+02 / Reference pixel on this axis
CRPIX2  =   2.048000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -6.079446644544E-05 / Projection distortion parameter
PV1_1   =   1.005627422450E+00 / Projection distortion parameter
PV1_2   =  -6.854987527402E-05 / Projection distortion parameter
PV1_4   =   1.667764889736E-05 / Projection distortion parameter
PV1_5   =  -1.558903074901E-06 / Projection distortion parameter
PV1_6   =   7.207879471522E-04 / Projection distortion parameter
PV1_7   =  -3.886110509388E-03 / Projection distortion parameter
PV1_8   =   2.911468716895E-04 / Projection distortion parameter
PV1_9   =  -3.908630311075E-03 / Projection distortion parameter
PV1_10  =   2.015687303584E-03 / Projection distortion parameter
PV2_0   =   1.826404555300E-04 / Projection distortion parameter
PV2_1   =   1.005424502873E+00 / Projection distortion parameter
PV2_2   =   1.148208105712E-04 / Projection distortion parameter
PV2_4   =  -3.088907817224E-03 / Projection distortion parameter
PV2_5   =  -3.058672439069E-05 / Projection distortion parameter
PV2_6   =   1.331727037438E-04 / Projection distortion parameter
PV2_7   =  -1.635437393122E-02 / Projection distortion parameter
PV2_8   =  -7.958387496510E-06 / Projection distortion parameter
PV2_9   =  -2.670839357019E-03 / Projection distortion parameter
PV2_10  =  -4.380525856027E-04 / Projection distortion parameter
END
CCDNUM  =                   36 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.032001000000E+02 / Reference pixel on this axis
CRPIX2  =  -2.211333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.002765450343E-03 / Projection distortion parameter
PV1_1   =   1.006003401723E+00 / Projection distortion parameter
PV1_2   =  -1.002045085150E-03 / Projection distortion parameter
PV1_4   =  -1.098661258936E-03 / Projection distortion parameter
PV1_5   =   8.308457150221E-04 / Projection distortion parameter
PV1_6   =  -2.983116198807E-04 / Projection distortion parameter
PV1_7   =  -2.799222007942E-03 / Projection distortion parameter
PV1_8   =  -1.270859197290E-03 / Projection distortion parameter
PV1_9   =  -3.475525096672E-03 / Projection distortion parameter
PV1_10  =  -3.975685946259E-04 / Projection distortion parameter
PV2_0   =  -1.072412494961E-04 / Projection distortion parameter
PV2_1   =   1.005757997072E+00 / Projection distortion parameter
PV2_2   =   6.710240055656E-04 / Projection distortion parameter
PV2_4   =  -8.196350996651E-04 / Projection distortion parameter
PV2_5   =  -7.261119812432E-04 / Projection distortion parameter
PV2_6   =   5.638202347730E-04 / Projection distortion parameter
PV2_7   =  -9.399800256525E-03 / Projection distortion parameter
PV2_8   =  -8.831800589204E-04 / Projection distortion parameter
PV2_9   =  -3.091875651003E-03 / Projection distortion parameter
PV2_10  =  -5.262606263209E-04 / Projection distortion parameter
END
CCDNUM  =                   37 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.032001000000E+02 / Reference pixel on this axis
CRPIX2  =  -6.470667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -2.287610985804E-03 / Projection distortion parameter
PV1_1   =   1.010049638147E+00 / Projection distortion parameter
PV1_2   =  -7.150982778618E-04 / Projection distortion parameter
PV1_4   =  -8.394689070704E-03 / Projection distortion parameter
PV1_5   =   1.840957984876E-03 / Projection distortion parameter
PV1_6   =  -1.645575874890E-03 / Projection distortion parameter
PV1_7   =   1.745894065370E-03 / Projection distortion parameter
PV1_8   =  -1.544056980305E-03 / Projection distortion parameter
PV1_9   =  -1.594552047137E-03 / Projection distortion parameter
PV1_10  =  -2.533113078707E-03 / Projection distortion parameter
PV2_0   =   3.570205158170E-04 / Projection distortion parameter
PV2_1   =   1.006512326356E+00 / Projection distortion parameter
PV2_2   =  -6.198816987220E-04 / Projection distortion parameter
PV2_4   =   2.741531209410E-04 / Projection distortion parameter
PV2_5   =  -3.267549383990E-03 / Projection distortion parameter
PV2_6   =   1.158709624376E-03 / Projection distortion parameter
PV2_7   =  -2.895206952728E-03 / Projection distortion parameter
PV2_8   =  -9.925537350163E-05 / Projection distortion parameter
PV2_9   =  -6.206460547606E-04 / Projection distortion parameter
PV2_10  =  -5.873058774718E-04 / Projection distortion parameter
END
CCDNUM  =                   38 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.032001000000E+02 / Reference pixel on this axis
CRPIX2  =  -1.073000000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -9.186117483546E-03 / Projection distortion parameter
PV1_1   =   1.033985176917E+00 / Projection distortion parameter
PV1_2   =  -7.015936390099E-04 / Projection distortion parameter
PV1_4   =  -3.865196870837E-02 / Projection distortion parameter
PV1_5   =   1.832982220760E-03 / Projection distortion parameter
PV1_6   =  -7.202061740173E-03 / Projection distortion parameter
PV1_7   =   1.461785921185E-02 / Projection distortion parameter
PV1_8   =  -9.898999535971E-04 / Projection distortion parameter
PV1_9   =   5.837131724431E-03 / Projection distortion parameter
PV1_10  =  -1.266146310264E-03 / Projection distortion parameter
PV2_0   =   5.668590335428E-04 / Projection distortion parameter
PV2_1   =   1.011209228200E+00 / Projection distortion parameter
PV2_2   =  -7.762537106617E-04 / Projection distortion parameter
PV2_4   =   3.729948990695E-03 / Projection distortion parameter
PV2_5   =  -1.403328152513E-02 / Projection distortion parameter
PV2_6   =   5.925960565907E-04 / Projection distortion parameter
PV2_7   =   9.878156579519E-04 / Projection distortion parameter
PV2_8   =  -3.184515192960E-03 / Projection distortion parameter
PV2_9   =   5.640280520083E-03 / Projection distortion parameter
PV2_10  =  -2.028332780272E-04 / Projection distortion parameter
END
CCDNUM  =                   39 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -2.357600000000E+03 / Reference pixel on this axis
CRPIX2  =   1.269633000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   4.675318911297E-03 / Projection distortion parameter
PV1_1   =   1.019744787610E+00 / Projection distortion parameter
PV1_2   =   2.153617290020E-03 / Projection distortion parameter
PV1_4   =   2.141811353455E-02 / Projection distortion parameter
PV1_5   =   8.178709432154E-03 / Projection distortion parameter
PV1_6   =   1.502770091609E-03 / Projection distortion parameter
PV1_7   =   7.584043436749E-03 / Projection distortion parameter
PV1_8   =   5.376337147757E-03 / Projection distortion parameter
PV1_9   =   1.888360333825E-03 / Projection distortion parameter
PV1_10  =  -2.249218787741E-03 / Projection distortion parameter
PV2_0   =   1.841171551744E-03 / Projection distortion parameter
PV2_1   =   1.011381684391E+00 / Projection distortion parameter
PV2_2   =   3.699055796835E-03 / Projection distortion parameter
PV2_4   =   1.047022487256E-02 / Projection distortion parameter
PV2_5   =   1.005827860677E-02 / Projection distortion parameter
PV2_6   =   3.805284620909E-03 / Projection distortion parameter
PV2_7   =   5.446595227335E-03 / Projection distortion parameter
PV2_8   =   5.040619640563E-03 / Projection distortion parameter
PV2_9   =   2.886062748082E-03 / Projection distortion parameter
PV2_10  =   1.505768129427E-03 / Projection distortion parameter
END
CCDNUM  =                   40 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -2.357600000000E+03 / Reference pixel on this axis
CRPIX2  =   8.437000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   9.043752539534E-04 / Projection distortion parameter
PV1_1   =   1.006526051511E+00 / Projection distortion parameter
PV1_2   =   8.051543931386E-05 / Projection distortion parameter
PV1_4   =   2.266917707116E-03 / Projection distortion parameter
PV1_5   =   1.140308798735E-03 / Projection distortion parameter
PV1_6   =   1.380829628653E-03 / Projection distortion parameter
PV1_7   =  -1.933918369026E-03 / Projection distortion parameter
PV1_8   =   1.493676497756E-03 / Projection distortion parameter
PV1_9   =  -3.140681612606E-03 / Projection distortion parameter
PV1_10  =   1.602046940110E-03 / Projection distortion parameter
PV2_0   =   6.615771867070E-04 / Projection distortion parameter
PV2_1   =   1.005669123655E+00 / Projection distortion parameter
PV2_2   =   6.423945836229E-04 / Projection distortion parameter
PV2_4   =  -1.551492447306E-03 / Projection distortion parameter
PV2_5   =   1.596540259279E-03 / Projection distortion parameter
PV2_6   =   3.240766215279E-04 / Projection distortion parameter
PV2_7   =  -7.571136564597E-03 / Projection distortion parameter
PV2_8   =   1.823264645485E-03 / Projection distortion parameter
PV2_9   =  -2.655156283042E-03 / Projection distortion parameter
PV2_10  =   1.668422773127E-04 / Projection distortion parameter
END
CCDNUM  =                   41 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -2.357600000000E+03 / Reference pixel on this axis
CRPIX2  =   4.177667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   2.728844306898E-04 / Projection distortion parameter
PV1_1   =   1.005775588200E+00 / Projection distortion parameter
PV1_2   =   9.901755472518E-05 / Projection distortion parameter
PV1_4   =   1.579884154133E-04 / Projection distortion parameter
PV1_5   =   9.565642986533E-04 / Projection distortion parameter
PV1_6   =   2.169137694672E-03 / Projection distortion parameter
PV1_7   =  -4.305411662119E-03 / Projection distortion parameter
PV1_8   =   1.441731262866E-03 / Projection distortion parameter
PV1_9   =  -2.834411506190E-03 / Projection distortion parameter
PV1_10  =   2.790218217928E-03 / Projection distortion parameter
PV2_0   =   6.480210075378E-04 / Projection distortion parameter
PV2_1   =   1.005005437819E+00 / Projection distortion parameter
PV2_2   =   6.980236889059E-04 / Projection distortion parameter
PV2_4   =  -3.367022044547E-03 / Projection distortion parameter
PV2_5   =   1.838164303334E-03 / Projection distortion parameter
PV2_6   =   1.529033116763E-04 / Projection distortion parameter
PV2_7   =  -9.314212352992E-03 / Projection distortion parameter
PV2_8   =   3.179825022351E-03 / Projection distortion parameter
PV2_9   =  -2.984827428637E-03 / Projection distortion parameter
PV2_10  =  -2.400660057577E-04 / Projection distortion parameter
END
CCDNUM  =                   42 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -2.357600000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.166665000000E+01 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -2.909541492998E-04 / Projection distortion parameter
PV1_1   =   1.005652270645E+00 / Projection distortion parameter
PV1_2   =   3.914852672501E-04 / Projection distortion parameter
PV1_4   =   4.373992699107E-05 / Projection distortion parameter
PV1_5   =   2.927418833610E-04 / Projection distortion parameter
PV1_6   =   2.664655252303E-03 / Projection distortion parameter
PV1_7   =  -4.185421268851E-03 / Projection distortion parameter
PV1_8   =  -6.037543779902E-04 / Projection distortion parameter
PV1_9   =  -3.711708096583E-03 / Projection distortion parameter
PV1_10  =   3.575202672248E-03 / Projection distortion parameter
PV2_0   =   4.273692313465E-04 / Projection distortion parameter
PV2_1   =   1.005026979872E+00 / Projection distortion parameter
PV2_2   =   2.702354488315E-04 / Projection distortion parameter
PV2_4   =  -2.539611545174E-03 / Projection distortion parameter
PV2_5   =   2.468689106547E-04 / Projection distortion parameter
PV2_6   =  -1.802786263105E-05 / Projection distortion parameter
PV2_7   =  -7.372701011146E-03 / Projection distortion parameter
PV2_8   =  -2.614198195760E-04 / Projection distortion parameter
PV2_9   =  -5.028102035908E-03 / Projection distortion parameter
PV2_10  =  -5.470722831146E-04 / Projection distortion parameter
END
CCDNUM  =                   43 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -2.357600000000E+03 / Reference pixel on this axis
CRPIX2  =  -4.341000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.239906577608E-03 / Projection distortion parameter
PV1_1   =   1.007100044635E+00 / Projection distortion parameter
PV1_2   =  -7.608489173881E-04 / Projection distortion parameter
PV1_4   =  -3.316123883757E-03 / Projection distortion parameter
PV1_5   =   1.957507175777E-03 / Projection distortion parameter
PV1_6   =  -9.128087934595E-04 / Projection distortion parameter
PV1_7   =  -1.234420970003E-03 / Projection distortion parameter
PV1_8   =  -2.050409282483E-03 / Projection distortion parameter
PV1_9   =  -2.469979189864E-03 / Projection distortion parameter
PV1_10  =  -6.824304227659E-04 / Projection distortion parameter
PV2_0   =   4.919750851207E-04 / Projection distortion parameter
PV2_1   =   1.004977857891E+00 / Projection distortion parameter
PV2_2   =  -5.695246496490E-04 / Projection distortion parameter
PV2_4   =  -4.077582581114E-03 / Projection distortion parameter
PV2_5   =  -1.929100463251E-03 / Projection distortion parameter
PV2_6   =   1.448692028367E-03 / Projection distortion parameter
PV2_7   =  -1.022044619505E-02 / Projection distortion parameter
PV2_8   =  -2.045715134103E-03 / Projection distortion parameter
PV2_9   =  -2.317533348286E-03 / Projection distortion parameter
PV2_10  =  -9.794472179784E-04 / Projection distortion parameter
END
CCDNUM  =                   44 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -2.357600000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.600334000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -5.425355109420E-03 / Projection distortion parameter
PV1_1   =   1.021414624034E+00 / Projection distortion parameter
PV1_2   =  -3.188608753136E-03 / Projection distortion parameter
PV1_4   =  -2.326664362354E-02 / Projection distortion parameter
PV1_5   =   9.628201277879E-03 / Projection distortion parameter
PV1_6   =  -1.079308995792E-03 / Projection distortion parameter
PV1_7   =   8.257244677293E-03 / Projection distortion parameter
PV1_8   =  -6.614301982714E-03 / Projection distortion parameter
PV1_9   =   3.534748501049E-04 / Projection distortion parameter
PV1_10  =   7.814217080565E-04 / Projection distortion parameter
PV2_0   =   1.730299360902E-03 / Projection distortion parameter
PV2_1   =   1.008836215029E+00 / Projection distortion parameter
PV2_2   =  -4.662348147394E-03 / Projection distortion parameter
PV2_4   =   1.132338466306E-03 / Projection distortion parameter
PV2_5   =  -9.647202758770E-03 / Projection distortion parameter
PV2_6   =   5.762271213374E-03 / Projection distortion parameter
PV2_7   =  -5.040608380075E-03 / Projection distortion parameter
PV2_8   =  -3.726933311480E-03 / Projection distortion parameter
PV2_9   =   3.062320946167E-03 / Projection distortion parameter
PV2_10  =  -2.316139280391E-03 / Projection distortion parameter
END
CCDNUM  =                   45 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -4.612000000000E+03 / Reference pixel on this axis
CRPIX2  =   1.269633000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   6.877992401288E-03 / Projection distortion parameter
PV1_1   =   1.027347127357E+00 / Projection distortion parameter
PV1_2   =   6.362862712579E-03 / Projection distortion parameter
PV1_4   =   2.876688903906E-02 / Projection distortion parameter
PV1_5   =   1.999702265354E-02 / Projection distortion parameter
PV1_6   =   3.049565192084E-03 / Projection distortion parameter
PV1_7   =   9.856729787385E-03 / Projection distortion parameter
PV1_8   =   1.151984579163E-02 / Projection distortion parameter
PV1_9   =   5.735574152267E-03 / Projection distortion parameter
PV1_10  =  -1.604183318198E-03 / Projection distortion parameter
PV2_0   =   4.060477057334E-03 / Projection distortion parameter
PV2_1   =   1.010367643742E+00 / Projection distortion parameter
PV2_2   =   1.073865245928E-02 / Projection distortion parameter
PV2_4   =  -2.126124050023E-03 / Projection distortion parameter
PV2_5   =   1.957512229947E-02 / Projection distortion parameter
PV2_6   =   1.054254786547E-02 / Projection distortion parameter
PV2_7   =  -9.557817291871E-03 / Projection distortion parameter
PV2_8   =   9.837691309815E-03 / Projection distortion parameter
PV2_9   =   6.852016201685E-03 / Projection distortion parameter
PV2_10  =   3.793821664234E-03 / Projection distortion parameter
END
CCDNUM  =                   46 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -4.612000000000E+03 / Reference pixel on this axis
CRPIX2  =   8.437000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   1.273889386513E-03 / Projection distortion parameter
PV1_1   =   1.008701748362E+00 / Projection distortion parameter
PV1_2   =   2.797665908533E-03 / Projection distortion parameter
PV1_4   =   4.800733001094E-03 / Projection distortion parameter
PV1_5   =   6.591013240321E-03 / Projection distortion parameter
PV1_6   =   5.484748990127E-03 / Projection distortion parameter
PV1_7   =  -7.781843348242E-04 / Projection distortion parameter
PV1_8   =   3.865498449985E-03 / Projection distortion parameter
PV1_9   =   1.404438473406E-03 / Projection distortion parameter
PV1_10  =   3.147605677229E-03 / Projection distortion parameter
PV2_0   =   1.223751960114E-03 / Projection distortion parameter
PV2_1   =   1.003774696269E+00 / Projection distortion parameter
PV2_2   =   3.131053222761E-03 / Projection distortion parameter
PV2_4   =  -8.598533537568E-03 / Projection distortion parameter
PV2_5   =   7.971567351230E-03 / Projection distortion parameter
PV2_6   =   2.874588666428E-03 / Projection distortion parameter
PV2_7   =  -1.277299386312E-02 / Projection distortion parameter
PV2_8   =   6.076319161388E-03 / Projection distortion parameter
PV2_9   =   5.940945456044E-04 / Projection distortion parameter
PV2_10  =   1.143926125105E-03 / Projection distortion parameter
END
CCDNUM  =                   47 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -4.612000000000E+03 / Reference pixel on this axis
CRPIX2  =   4.177667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   5.977877952269E-04 / Projection distortion parameter
PV1_1   =   1.005477830722E+00 / Projection distortion parameter
PV1_2   =   1.941370675862E-03 / Projection distortion parameter
PV1_4   =   4.020141290666E-04 / Projection distortion parameter
PV1_5   =  -7.907836364262E-04 / Projection distortion parameter
PV1_6   =   4.510534214345E-03 / Projection distortion parameter
PV1_7   =  -3.339770524945E-03 / Projection distortion parameter
PV1_8   =   4.681985687139E-04 / Projection distortion parameter
PV1_9   =  -4.805764796875E-03 / Projection distortion parameter
PV1_10  =   3.849736074056E-03 / Projection distortion parameter
PV2_0   =   8.195035497265E-05 / Projection distortion parameter
PV2_1   =   1.000071842974E+00 / Projection distortion parameter
PV2_2   =  -1.978839307338E-04 / Projection distortion parameter
PV2_4   =  -1.364615549554E-02 / Projection distortion parameter
PV2_5   =  -3.946868553127E-05 / Projection distortion parameter
PV2_6   =   4.315967360567E-04 / Projection distortion parameter
PV2_7   =  -1.492365586433E-02 / Projection distortion parameter
PV2_8   =   1.111615596941E-04 / Projection distortion parameter
PV2_9   =  -3.437421886405E-03 / Projection distortion parameter
PV2_10  =   5.236834013300E-04 / Projection distortion parameter
END
CCDNUM  =                   48 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -4.612000000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.166665000000E+01 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -8.837110251098E-04 / Projection distortion parameter
PV1_1   =   1.005654098145E+00 / Projection distortion parameter
PV1_2   =  -1.246187284710E-03 / Projection distortion parameter
PV1_4   =  -8.855909152590E-04 / Projection distortion parameter
PV1_5   =  -3.101183081012E-04 / Projection distortion parameter
PV1_6   =  -2.041163801693E-03 / Projection distortion parameter
PV1_7   =  -3.246727862222E-03 / Projection distortion parameter
PV1_8   =  -1.782718202348E-03 / Projection distortion parameter
PV1_9   =  -4.732565421565E-03 / Projection distortion parameter
PV1_10  =  -1.928729102477E-03 / Projection distortion parameter
PV2_0   =   4.978544788765E-04 / Projection distortion parameter
PV2_1   =   1.003718676687E+00 / Projection distortion parameter
PV2_2   =   4.023899951410E-04 / Projection distortion parameter
PV2_4   =  -4.397789821816E-03 / Projection distortion parameter
PV2_5   =  -4.294388704645E-04 / Projection distortion parameter
PV2_6   =   2.387047545555E-04 / Projection distortion parameter
PV2_7   =  -7.115532343943E-03 / Projection distortion parameter
PV2_8   =  -6.268243771384E-04 / Projection distortion parameter
PV2_9   =  -3.261497108282E-03 / Projection distortion parameter
PV2_10  =  -2.386760518501E-05 / Projection distortion parameter
END
CCDNUM  =                   49 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -4.612000000000E+03 / Reference pixel on this axis
CRPIX2  =  -4.341000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.507467589951E-03 / Projection distortion parameter
PV1_1   =   1.009081977924E+00 / Projection distortion parameter
PV1_2   =  -7.492124071277E-04 / Projection distortion parameter
PV1_4   =  -5.864812204324E-03 / Projection distortion parameter
PV1_5   =   6.269468148827E-03 / Projection distortion parameter
PV1_6   =   8.751963252879E-04 / Projection distortion parameter
PV1_7   =  -1.979531958791E-04 / Projection distortion parameter
PV1_8   =  -4.910231958075E-03 / Projection distortion parameter
PV1_9   =  -1.893844713890E-04 / Projection distortion parameter
PV1_10  =   1.566631784939E-03 / Projection distortion parameter
PV2_0   =   1.096786838766E-03 / Projection distortion parameter
PV2_1   =   1.004679582773E+00 / Projection distortion parameter
PV2_2   =  -2.304876103063E-03 / Projection distortion parameter
PV2_4   =  -5.597126541425E-03 / Projection distortion parameter
PV2_5   =  -7.153391352176E-03 / Projection distortion parameter
PV2_6   =   2.606600459853E-03 / Projection distortion parameter
PV2_7   =  -1.009918484487E-02 / Projection distortion parameter
PV2_8   =  -6.075354230409E-03 / Projection distortion parameter
PV2_9   =  -1.628388613457E-04 / Projection distortion parameter
PV2_10  =  -1.229530440681E-03 / Projection distortion parameter
END
CCDNUM  =                   50 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -4.612000000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.600334000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -7.690274308006E-03 / Projection distortion parameter
PV1_1   =   1.028026707834E+00 / Projection distortion parameter
PV1_2   =  -9.196746805749E-03 / Projection distortion parameter
PV1_4   =  -2.960225726376E-02 / Projection distortion parameter
PV1_5   =   2.022408306215E-02 / Projection distortion parameter
PV1_6   =  -8.009946145850E-03 / Projection distortion parameter
PV1_7   =   1.032904975227E-02 / Projection distortion parameter
PV1_8   =  -1.101236328205E-02 / Projection distortion parameter
PV1_9   =   6.768088722412E-03 / Projection distortion parameter
PV1_10  =  -1.888346091771E-03 / Projection distortion parameter
PV2_0   =   3.597353398313E-03 / Projection distortion parameter
PV2_1   =   1.011140408951E+00 / Projection distortion parameter
PV2_2   =  -9.438228224953E-03 / Projection distortion parameter
PV2_4   =   1.215973250894E-03 / Projection distortion parameter
PV2_5   =  -1.795116720577E-02 / Projection distortion parameter
PV2_6   =   1.020142910103E-02 / Projection distortion parameter
PV2_7   =  -5.095110614520E-03 / Projection distortion parameter
PV2_8   =  -7.203930194038E-03 / Projection distortion parameter
PV2_9   =   7.056085706882E-03 / Projection distortion parameter
PV2_10  =  -3.640291310698E-03 / Projection distortion parameter
END
CCDNUM  =                   51 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -6.866400000000E+03 / Reference pixel on this axis
CRPIX2  =   1.056667000000E+04 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   3.851426025149E-03 / Projection distortion parameter
PV1_1   =   1.018338025651E+00 / Projection distortion parameter
PV1_2   =   2.460516925138E-03 / Projection distortion parameter
PV1_4   =   1.751943515072E-02 / Projection distortion parameter
PV1_5   =   1.648659714208E-02 / Projection distortion parameter
PV1_6   =  -1.426133068785E-03 / Projection distortion parameter
PV1_7   =   5.305006343817E-03 / Projection distortion parameter
PV1_8   =   8.997105114913E-03 / Projection distortion parameter
PV1_9   =   5.445782770170E-03 / Projection distortion parameter
PV1_10  =  -2.717161753916E-03 / Projection distortion parameter
PV2_0   =   2.246267265671E-03 / Projection distortion parameter
PV2_1   =   1.005065850037E+00 / Projection distortion parameter
PV2_2   =   9.121602311254E-03 / Projection distortion parameter
PV2_4   =  -5.754074048078E-03 / Projection distortion parameter
PV2_5   =   1.698908329224E-02 / Projection distortion parameter
PV2_6   =   1.024008887964E-02 / Projection distortion parameter
PV2_7   =  -8.119655847344E-03 / Projection distortion parameter
PV2_8   =   9.241860809056E-03 / Projection distortion parameter
PV2_9   =   5.504357138834E-03 / Projection distortion parameter
PV2_10  =   3.956399272311E-03 / Projection distortion parameter
END
CCDNUM  =                   52 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -6.866400000000E+03 / Reference pixel on this axis
CRPIX2  =   6.307333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   1.832073567829E-03 / Projection distortion parameter
PV1_1   =   1.008135740301E+00 / Projection distortion parameter
PV1_2   =   5.818423380960E-03 / Projection distortion parameter
PV1_4   =   3.533475084968E-03 / Projection distortion parameter
PV1_5   =   6.506094263627E-03 / Projection distortion parameter
PV1_6   =   9.599425719383E-03 / Projection distortion parameter
PV1_7   =  -1.599865548501E-03 / Projection distortion parameter
PV1_8   =   3.841855065160E-03 / Projection distortion parameter
PV1_9   =   9.403711691510E-04 / Projection distortion parameter
PV1_10  =   5.176230120628E-03 / Projection distortion parameter
PV2_0   =   1.475854899722E-03 / Projection distortion parameter
PV2_1   =   1.005911370228E+00 / Projection distortion parameter
PV2_2   =   2.406506477368E-03 / Projection distortion parameter
PV2_4   =   3.938181255061E-04 / Projection distortion parameter
PV2_5   =   6.346983810833E-03 / Projection distortion parameter
PV2_6   =   2.647662155233E-03 / Projection distortion parameter
PV2_7   =  -3.242987537735E-03 / Projection distortion parameter
PV2_8   =   4.624166468982E-03 / Projection distortion parameter
PV2_9   =   3.882023733567E-04 / Projection distortion parameter
PV2_10  =   8.806358609112E-04 / Projection distortion parameter
END
CCDNUM  =                   53 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -6.866400000000E+03 / Reference pixel on this axis
CRPIX2  =   2.048000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   3.837407909008E-04 / Projection distortion parameter
PV1_1   =   1.005782140090E+00 / Projection distortion parameter
PV1_2   =   1.522855545078E-03 / Projection distortion parameter
PV1_4   =   4.565387446231E-04 / Projection distortion parameter
PV1_5   =   8.842261877428E-04 / Projection distortion parameter
PV1_6   =   2.685066400646E-03 / Projection distortion parameter
PV1_7   =  -3.163967999683E-03 / Projection distortion parameter
PV1_8   =   7.780804681432E-04 / Projection distortion parameter
PV1_9   =  -2.726661985181E-03 / Projection distortion parameter
PV1_10  =   1.538571932470E-03 / Projection distortion parameter
PV2_0   =  -8.010432120992E-05 / Projection distortion parameter
PV2_1   =   9.982650111759E-01 / Projection distortion parameter
PV2_2   =   1.513570555794E-04 / Projection distortion parameter
PV2_4   =  -1.201147409715E-02 / Projection distortion parameter
PV2_5   =   6.469071053663E-04 / Projection distortion parameter
PV2_6   =   1.016441485531E-03 / Projection distortion parameter
PV2_7   =  -1.000923504480E-02 / Projection distortion parameter
PV2_8   =   6.012844280666E-04 / Projection distortion parameter
PV2_9   =  -1.831131201009E-03 / Projection distortion parameter
PV2_10  =   1.596746146047E-04 / Projection distortion parameter
END
CCDNUM  =                   54 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -6.866400000000E+03 / Reference pixel on this axis
CRPIX2  =  -2.211333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -7.364538275842E-04 / Projection distortion parameter
PV1_1   =   1.007846263395E+00 / Projection distortion parameter
PV1_2   =   1.236269100487E-04 / Projection distortion parameter
PV1_4   =  -3.675481511792E-03 / Projection distortion parameter
PV1_5   =   5.170896797365E-03 / Projection distortion parameter
PV1_6   =   5.506910466507E-04 / Projection distortion parameter
PV1_7   =  -1.593613612328E-03 / Projection distortion parameter
PV1_8   =  -4.012030848794E-03 / Projection distortion parameter
PV1_9   =  -3.789241613053E-04 / Projection distortion parameter
PV1_10  =   4.980292328577E-04 / Projection distortion parameter
PV2_0   =   1.716933430401E-03 / Projection distortion parameter
PV2_1   =   1.007952684046E+00 / Projection distortion parameter
PV2_2   =  -2.106135617983E-03 / Projection distortion parameter
PV2_4   =   4.129332069153E-03 / Projection distortion parameter
PV2_5   =  -5.800926564546E-03 / Projection distortion parameter
PV2_6   =   2.015727613655E-03 / Projection distortion parameter
PV2_7   =  -1.108961885353E-03 / Projection distortion parameter
PV2_8   =  -4.953413841262E-03 / Projection distortion parameter
PV2_9   =  -1.183166125697E-03 / Projection distortion parameter
PV2_10  =  -1.108566516200E-03 / Projection distortion parameter
END
CCDNUM  =                   55 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -6.866400000000E+03 / Reference pixel on this axis
CRPIX2  =  -6.470667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -4.229020575149E-03 / Projection distortion parameter
PV1_1   =   1.018602523224E+00 / Projection distortion parameter
PV1_2   =  -4.097456152514E-03 / Projection distortion parameter
PV1_4   =  -1.729793181392E-02 / Projection distortion parameter
PV1_5   =   1.732837454170E-02 / Projection distortion parameter
PV1_6   =  -1.036615783765E-03 / Projection distortion parameter
PV1_7   =   4.644311514811E-03 / Projection distortion parameter
PV1_8   =  -1.053680567065E-02 / Projection distortion parameter
PV1_9   =   4.283703961990E-03 / Projection distortion parameter
PV1_10  =   8.155571866763E-04 / Projection distortion parameter
PV2_0   =   2.846511016253E-03 / Projection distortion parameter
PV2_1   =   1.005930964237E+00 / Projection distortion parameter
PV2_2   =  -1.053531242459E-02 / Projection distortion parameter
PV2_4   =  -5.915375977714E-03 / Projection distortion parameter
PV2_5   =  -2.070692395696E-02 / Projection distortion parameter
PV2_6   =   1.061553691890E-02 / Projection distortion parameter
PV2_7   =  -8.493388465846E-03 / Projection distortion parameter
PV2_8   =  -1.053621084219E-02 / Projection distortion parameter
PV2_9   =   7.233313777910E-03 / Projection distortion parameter
PV2_10  =  -3.578497451445E-03 / Projection distortion parameter
END
CCDNUM  =                   56 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -9.120800000000E+03 / Reference pixel on this axis
CRPIX2  =   8.437000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =   6.082787863406E-03 / Projection distortion parameter
PV1_1   =   1.018900830961E+00 / Projection distortion parameter
PV1_2   =   1.462957612843E-02 / Projection distortion parameter
PV1_4   =   1.444428840175E-02 / Projection distortion parameter
PV1_5   =   2.397167333444E-02 / Projection distortion parameter
PV1_6   =   1.523606771341E-02 / Projection distortion parameter
PV1_7   =   2.465808450933E-03 / Projection distortion parameter
PV1_8   =   1.207233343673E-02 / Projection distortion parameter
PV1_9   =   8.347697696531E-03 / Projection distortion parameter
PV1_10  =   5.626153091781E-03 / Projection distortion parameter
PV2_0   =  -4.429005043565E-03 / Projection distortion parameter
PV2_1   =   9.770510490528E-01 / Projection distortion parameter
PV2_2   =   1.217897511264E-02 / Projection distortion parameter
PV2_4   =  -4.101338046725E-02 / Projection distortion parameter
PV2_5   =   2.339641718342E-02 / Projection distortion parameter
PV2_6   =   1.223336337685E-02 / Projection distortion parameter
PV2_7   =  -2.178749927042E-02 / Projection distortion parameter
PV2_8   =   1.125794891738E-02 / Projection distortion parameter
PV2_9   =   9.593930319333E-03 / Projection distortion parameter
PV2_10  =   3.411618441501E-03 / Projection distortion parameter
END
CCDNUM  =                   57 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -9.120800000000E+03 / Reference pixel on this axis
CRPIX2  =   4.177667000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -2.816092273833E-03 / Projection distortion parameter
PV1_1   =   1.007502374263E+00 / Projection distortion parameter
PV1_2   =  -1.185218892105E-02 / Projection distortion parameter
PV1_4   =   2.690367045053E-03 / Projection distortion parameter
PV1_5   =   5.244222892903E-03 / Projection distortion parameter
PV1_6   =  -1.569093983080E-02 / Projection distortion parameter
PV1_7   =  -1.250030324677E-03 / Projection distortion parameter
PV1_8   =   2.528141478024E-03 / Projection distortion parameter
PV1_9   =   3.377999207357E-04 / Projection distortion parameter
PV1_10  =  -6.994135266474E-03 / Projection distortion parameter
PV2_0   =  -5.462712767681E-03 / Projection distortion parameter
PV2_1   =   9.781370474075E-01 / Projection distortion parameter
PV2_2   =   6.286085803365E-04 / Projection distortion parameter
PV2_4   =  -3.415934938634E-02 / Projection distortion parameter
PV2_5   =   1.136378006522E-03 / Projection distortion parameter
PV2_6   =   3.043539625884E-03 / Projection distortion parameter
PV2_7   =  -1.707676806855E-02 / Projection distortion parameter
PV2_8   =   8.275982508985E-04 / Projection distortion parameter
PV2_9   =   7.457375142444E-04 / Projection distortion parameter
PV2_10  =   7.308089249349E-04 / Projection distortion parameter
END
CCDNUM  =                   58 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -9.120800000000E+03 / Reference pixel on this axis
CRPIX2  =  -8.166665000000E+01 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -1.386968059045E-04 / Projection distortion parameter
PV1_1   =   1.008362509813E+00 / Projection distortion parameter
PV1_2   =   2.401472321848E-03 / Projection distortion parameter
PV1_4   =  -1.643873767134E-03 / Projection distortion parameter
PV1_5   =   8.216099130443E-03 / Projection distortion parameter
PV1_6   =   4.357562740756E-03 / Projection distortion parameter
PV1_7   =  -3.046865799489E-03 / Projection distortion parameter
PV1_8   =  -2.485157908180E-03 / Projection distortion parameter
PV1_9   =   2.512630844199E-03 / Projection distortion parameter
PV1_10  =   2.004128163636E-03 / Projection distortion parameter
PV2_0   =   1.797379993351E-03 / Projection distortion parameter
PV2_1   =   1.008708144227E+00 / Projection distortion parameter
PV2_2   =   1.114803625352E-03 / Projection distortion parameter
PV2_4   =   7.777879764064E-03 / Projection distortion parameter
PV2_5   =   1.588604847471E-03 / Projection distortion parameter
PV2_6   =   3.834033351634E-03 / Projection distortion parameter
PV2_7   =   2.078784252694E-03 / Projection distortion parameter
PV2_8   =   1.212431248446E-03 / Projection distortion parameter
PV2_9   =   1.680222253150E-03 / Projection distortion parameter
PV2_10  =  -1.083938781267E-03 / Projection distortion parameter
END
CCDNUM  =                   59 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -9.120800000000E+03 / Reference pixel on this axis
CRPIX2  =  -4.341000000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -4.720168784024E-03 / Projection distortion parameter
PV1_1   =   1.015885340830E+00 / Projection distortion parameter
PV1_2   =  -9.804646671496E-03 / Projection distortion parameter
PV1_4   =  -1.201887822883E-02 / Projection distortion parameter
PV1_5   =   1.840931805715E-02 / Projection distortion parameter
PV1_6   =  -9.795477426395E-03 / Projection distortion parameter
PV1_7   =   1.773796751722E-03 / Projection distortion parameter
PV1_8   =  -9.928442862296E-03 / Projection distortion parameter
PV1_9   =   5.740225493430E-03 / Projection distortion parameter
PV1_10  =  -3.714602601707E-03 / Projection distortion parameter
PV2_0   =   8.500632690344E-03 / Projection distortion parameter
PV2_1   =   1.029521672622E+00 / Projection distortion parameter
PV2_2   =  -1.254690418240E-02 / Projection distortion parameter
PV2_4   =   2.943237961156E-02 / Projection distortion parameter
PV2_5   =  -2.644596143233E-02 / Projection distortion parameter
PV2_6   =   1.122210156924E-02 / Projection distortion parameter
PV2_7   =   9.476565718220E-03 / Projection distortion parameter
PV2_8   =  -1.431489446608E-02 / Projection distortion parameter
PV2_9   =   8.032547273245E-03 / Projection distortion parameter
PV2_10  =  -3.405578589183E-03 / Projection distortion parameter
END
CCDNUM  =                   60 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.137520000000E+04 / Reference pixel on this axis
CRPIX2  =   6.307333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -8.767658845545E-04 / Projection distortion parameter
PV1_1   =   1.013776622349E+00 / Projection distortion parameter
PV1_2   =  -6.526058749912E-03 / Projection distortion parameter
PV1_4   =   8.481406393180E-03 / Projection distortion parameter
PV1_5   =   1.724200915724E-02 / Projection distortion parameter
PV1_6   =  -7.872136679118E-03 / Projection distortion parameter
PV1_7   =   4.069663243650E-04 / Projection distortion parameter
PV1_8   =   7.823578289878E-03 / Projection distortion parameter
PV1_9   =   6.319112797479E-03 / Projection distortion parameter
PV1_10  =  -2.918500902167E-03 / Projection distortion parameter
PV2_0   =  -1.231294908913E-03 / Projection distortion parameter
PV2_1   =   9.995102311626E-01 / Projection distortion parameter
PV2_2   =   7.008318933049E-03 / Projection distortion parameter
PV2_4   =  -1.668247396543E-04 / Projection distortion parameter
PV2_5   =   1.567489412689E-02 / Projection distortion parameter
PV2_6   =   8.618484261576E-03 / Projection distortion parameter
PV2_7   =   7.269065637938E-05 / Projection distortion parameter
PV2_8   =   8.631354977974E-03 / Projection distortion parameter
PV2_9   =   6.416365866044E-03 / Projection distortion parameter
PV2_10  =   2.379627262705E-03 / Projection distortion parameter
END
CCDNUM  =                   62 / CCD number
EQUINOX =        2000.00000000 / Mean equinox
RADESYS = 'ICRS    '           / Astrometric system
CTYPE1  = 'RA---TPV'           / WCS projection type for this axis
CTYPE2  = 'DEC--TPV'           / WCS projection type for this axis
CUNIT1  = 'deg     '           / Axis unit
CUNIT2  = 'deg     '           / Axis unit
CRPIX1  =  -1.137520000000E+04 / Reference pixel on this axis
CRPIX2  =  -2.211333000000E+03 / Reference pixel on this axis
CD1_1   =   8.825829163601E-09 / Linear projection matrix
CD1_2   =   7.286111641842E-05 / Linear projection matrix
CD2_1   =  -7.285667022714E-05 / Linear projection matrix
CD2_2   =   1.037520891423E-08 / Linear projection matrix
PV1_0   =  -3.977031817015E-03 / Projection distortion parameter
PV1_1   =   1.016356418151E+00 / Projection distortion parameter
PV1_2   =  -1.029536341028E-02 / Projection distortion parameter
PV1_4   =  -1.000959527064E-02 / Projection distortion parameter
PV1_5   =   2.201651057100E-02 / Projection distortion parameter
PV1_6   =  -1.051986562404E-02 / Projection distortion parameter
PV1_7   =   9.799474308577E-04 / Projection distortion parameter
PV1_8   =  -8.851741013181E-03 / Projection distortion parameter
PV1_9   =   8.657471135330E-03 / Projection distortion parameter
PV1_10  =  -3.589113506822E-03 / Projection distortion parameter
PV2_0   =   1.011819897539E-02 / Projection distortion parameter
PV2_1   =   1.035810736657E+00 / Projection distortion parameter
PV2_2   =  -9.331060640631E-03 / Projection distortion parameter
PV2_4   =   3.938334478655E-02 / Projection distortion parameter
PV2_5   =  -1.811322343871E-02 / Projection distortion parameter
PV2_6   =   1.035482490942E-02 / Projection distortion parameter
PV2_7   =   1.447354485873E-02 / Projection distortion parameter
PV2_8   =  -9.367231614536E-03 / Projection distortion parameter
PV2_9   =   7.834910380586E-03 / Projection distortion parameter
PV2_10  =  -2.864578065441E-03 / Projection distortion parameter
END
""")
    f.close()


class MockDbi(object):
    def __init__(self, *args, **kwargs):
        if 'data' in kwargs.keys() and kwargs['data']:
            self.data = kwargs['data']
        else:
            self.data = [(('the_root',),)]
        if 'descr' in kwargs.keys():
            self.descr = kwargs['descr']
        else :
            self.descr = []
        self.con = self.Connection()
        self._curs = None
        self.count = {'cursor': 0,
                      'commit': 0,
                      'get_positional_bind_string': 0}

    def get_positional_bind_string(self, *args, **kwargs):
        self.count['get_positional_bind_string'] += 1

    def getCount(self, attrib):
        if attrib in self.count.keys():
            return self.count[attrib]
        try:
            return self.con.getCount(attrib)
        except:
            return self._curs.getCount(attrib)

    def cursor(self):
        self._curs = self.Cursor(self.data, self.descr)
        self.count['cursor'] += 1
        return self._curs

    def setThrow(self, value):
        self.con.throw = value

    class Connection(object):
        def __init__(self):
            self.throw = False
            self.count = {'ping': 0}

        def getCount(self, attrib):
            return self.count[attrib]

        def ping(self):
            self.count['ping'] += 1
            if self.throw:
                raise Exception()
            return True

    class Cursor(object):
        def __init__(self, data=[], descr=[]):
            self.data = data
            self.current_data = None
            self.select = False
            self.data.reverse()
            self.description = None
            self.descr_data = descr
            self.descr_data.reverse()
            self.count = {'execute': 0,
                          'fetchall': 0,
                          'prepare': 0,
                          'executemany': 0}
            self._idx = 0

        def next(self):
            if self._idx < len(self.current_data):
                self._idx += 1
                return self.current_data[self._idx - 1]
            raise StopIteration

        def __iter__(self):
            return self

        def getCount(self, attrib):
            return self.count[attrib]

        def setData(self, data):
            self.data = data
            self.data.reverse()

        def execute(self,*args, **kwargs):
            self._idx = 0
            if args:
                if isinstance(args[0], str):
                    if args[0].lower().startswith('select'):
                        try:
                            self.description = self.descr_data.pop()
                        except:
                            self.description = None
                        try:
                            self.current_data = self.data.pop()
                        except:
                            self.current_data = None
            elif self.select:
                try:
                    self.description = self.descr_data.pop()
                except:
                    self.description = None
                try:
                    self.current_data = self.data.pop()
                except:
                    self.current_data = None
            self.count['execute'] += 1

        def fetchall(self):
            #print "DATA",self.data
            self.count['fetchall'] += 1
            if self.current_data:
                return self.current_data
            return None

        def prepare(self, *args, **kwargs):
            self.select = False
            if args:
                if isinstance(args[0], str):
                    if args[0].lower().startswith('select'):
                        self.select = True
            self.count['prepare'] += 1

        def executemany(self, *args, **kwargs):
            self._idx = 0
            self.select = False
            self.count['executemany'] += 1

    def setReturn(self, data):
        self.data = data

    def setDescription(self, descr):
        self.descr = descr

    def commit(self):
        self.count['commit'] += 1


class TestXmlslurper(unittest.TestCase):
    tablelist = ("Astrometric_Instruments",
                 "FGroups",
                 "Fields",
                 "Photometric_Instruments",
                 "PSF_Extensions",
                 "PSF_Fields",
                 "Warnings")
    xmldata = """<main><TABLE name="FGroups">
<FIELD name="Field1" datatype="float"/>
<field name="Field2" datatype="int"/>
<field name="Field3" datatype="str"/>
<field name="Field4" datatype="int" arraysize="5"/>
<field name="Field5" datatype="char" arraysize="2"/>
<field name="field6" datatype="float" arraysize="2"/>
<field name="field7" datatype="bool" arraysize="3"/>
<TR>
 <TD>12345.6</TD><td>25</td><td>Blah</td><td>2 4 6 8 10</td><td>first second</td><td>2.5 6.7</td><td>1 0 1</td>
</TR>
<hi/>
</TABLE>
<table name="bad_table">
<field name="f1" datatype="int"/>
<tr><td>5</td></tr>
</table>
</main>
"""
    def test_all(self):
        with patch('despymisc.xmlslurp.open', mock_open(read_data=self.xmldata)) as mo:
            data = Xmlslurper('filename', self.tablelist)
            self.assertTrue('FGroups' in data.gettables().keys())
            self.assertTrue(len(data.gettables().keys()), 1)
            self.assertEqual(len(data.keys()), 1)
            tab = data.gettables()
            self.assertEqual(tab['FGroups'][0]['field1'], 12345.6)

class TestSubprocess4(unittest.TestCase):
    class TestError(OSError):
        def __init__(self, text=''):
            OSError.__init__(self, text)
            self.errno = errno.ECHILD

    def test_init(self):
        _ = sub4.Popen(['ls','-1'])

    def test_wait(self):
        start = time.time()
        po = sub4.Popen(['sleep', '10'])
        po.wait4()
        self.assertTrue(time.time() - start >= 10.)

    def test_waitError(self):
        with patch('despymisc.subprocess4.os.wait4', side_effect=self.TestError()):
            start = time.time()
            po = sub4.Popen(['sleep', '10'])
            self.assertTrue(po.wait4() == 0)
            self.assertTrue(time.time() - start < 10.)
        with patch('despymisc.subprocess4.os.wait4', side_effect=OSError()):
            with self.assertRaises(OSError):
                po = sub4.Popen(['sleep', '10'])
                po.wait4()

    def test_fastReturn(self):
        po = sub4.Popen(['ls','-1'])
        time.sleep(3)
        po.returncode = 0
        self.assertIsNotNone(po.returncode)
        self.assertIsNotNone(po.wait4())

    def test_segfault(self):
        with capture_output() as (out, err):
            po = sub4.Popen(['ls','-1'])
            po.returncode = -signal.SIGSEGV
            self.assertTrue(-signal.SIGSEGV == po.wait4())
            output = out.getvalue().strip()
            self.assertTrue('SEGMENTATION' in output)

    def test_mismatchPID(self):
        with patch('despymisc.subprocess4.os.wait4', return_value=(0,1,{})) as osw:
            po = sub4.Popen(['ls'])
            pu = psutil.Process(po.pid)
            pu.terminate()
            pu.wait()
            self.assertEqual(po.wait4(), 1)

class TestScamputil(unittest.TestCase):
    def setUp(self):
        makeAheadFile()

    def tearDown(self):
        os.unlink(FILENAME)

    def test_all_good_headers(self):
        output = 'test.out'
        ccds = [1, 3, 4, 5, 6, 7, 8, 9]
        self.assertTrue(scu.split_ahead_by_ccd(FILENAME, output, ccds))
        found_ccds = []
        found_hist = False
        found_flux = False
        found_other = False
        f = open(output, 'r')
        for line in f.readlines():
            if line.startswith('CCDNUM'):
                found_ccds.append(int(line.split()[2]))
            elif line.startswith('HISTORY'):
                found_hist = True
            elif line.startswith('FLXSCALE'):
                found_flux = True
            else:
                found_other = True
        f.close()
        os.unlink(output)
        ccds.sort()
        found_ccds.sort()
        self.assertTrue(found_other)
        self.assertFalse(found_hist)
        self.assertFalse(found_flux)
        self.assertEqual(ccds, found_ccds)

    def test_missing_ccd(self):
        output = 'test.junk'
        ccds = [1,2,3]
        with capture_output() as (out, err):
            self.assertFalse(scu.split_ahead_by_ccd(FILENAME, output, ccds))
            outp = out.getvalue().strip()
            self.assertTrue('2 is not present' in outp)
        self.assertFalse(os.path.exists(output))

    def test_bad_input(self):
        with self.assertRaises(Exception) as ra:
            scu.split_ahead_by_ccd('', '', [])

    def test_malformed_file(self):
        f = open('malformed.ahead', 'w')
        f.write("END\n")
        f.close()
        output = 'test.junk'
        self.assertTrue(scu.split_ahead_by_ccd('malformed.ahead', output, []))
        self.assertTrue(os.path.exists(output))
        self.assertEqual(os.stat(output).st_size, 0)
        os.unlink(output)
        os.unlink('malformed.ahead')

class TestMisctime(unittest.TestCase):
    def test_good_date(self):
        datestr = '2019-09-15T10:35:22'
        self.assertEqual('20190914', mt.convert_utc_str_to_nite(datestr))
        datestr = '2019-09-15T14:59:59.99'
        self.assertEqual('20190914', mt.convert_utc_str_to_nite(datestr))
        datestr = '2019-09-15T15:00:01.00'
        self.assertEqual('20190915', mt.convert_utc_str_to_nite(datestr))

class Test_create_special_metadata(unittest.TestCase):
    def test_band(self):
        self.assertEqual(csm.create_band('u 3'), 'u')
        with self.assertRaises(KeyError):
            csm.create_band('Z')
        self.assertEqual(csm.create_band('z'), 'z')

    def test_create_camsym(self):
        self.assertEqual(csm.create_camsym('g asdf23'), 'g')

    def test_create_nite(self):
        datestr = '2019-09-15T10:35:22'
        self.assertEqual('20190914', csm.create_nite(datestr))
        datestr = '2019-09-15T14:59:59.99'
        self.assertEqual('20190914', csm.create_nite(datestr))
        datestr = '2019-09-15T15:00:01.00'
        self.assertEqual('20190915', csm.create_nite(datestr))
        datestr = '2019-09-01T01:00:01.00'
        self.assertEqual('20190831', csm.create_nite(datestr))
        datestr = '2019-01-01T01:00:01.00'
        self.assertEqual('20181231', csm.create_nite(datestr))

    def test_create_field(self):
        fld = '12,34,56'
        obj = " hex " + fld
        self.assertEqual(csm.create_field(obj), fld)
        with self.assertRaises(KeyError):
            csm.create_field('hex ' + fld)
        with self.assertRaises(KeyError):
            csm.create_field(' hex  ' + fld)

    def test_convert_ra_to_deg(self):
        self.assertEqual(csm.convert_ra_to_deg('06:00:00'), 90.0)
        self.assertEqual(csm.convert_ra_to_deg('12:00:01'), round(180.0 + 15./3600., 6))

    def test_convert_dec_to_deg(self):
        self.assertEqual(csm.convert_dec_to_deg('27:30:00'), 27.5)
        self.assertEqual(csm.convert_dec_to_deg('-27:30:00'), -27.5)

    def test_fwhm_arcsec(self):
        args = [0.5, 1, 1, 1, 1, 1, 1]
        self.assertEqual(csm.fwhm_arcsec(args), 0.5)

        args = [0.5, 2, 1, 1, 1, 1, 1]
        self.assertEqual(csm.fwhm_arcsec(args), 0.5)

        args = [0.5, 1, 1, 1, 1, 1, 0]
        self.assertEqual(csm.fwhm_arcsec(args), 0.25)

        args = [0.5, 1, 1, 1, 1, 0.5, 1]
        self.assertEqual(csm.fwhm_arcsec(args), 0.375)

        args = [0.5, 0, 2, 1, 1, 1, 0]
        self.assertEqual(csm.fwhm_arcsec(args), 0.25)

        args = [0.5, 0, 0, 1, 1, 1, 0]
        self.assertEqual(csm.fwhm_arcsec(args), 0.25)

        args = [0.5, 0, 0, 1, 1, 0, 0]
        with self.assertRaises(KeyError):
            csm.fwhm_arcsec(args)

        args = [0.5, -1, 2, 1, 1, 0, 0]
        self.assertAlmostEqual(csm.fwhm_arcsec(args), 3078.4476, 4)

        args = [0.5, -1, -2, 1, 1, 0, 0]
        self.assertAlmostEqual(csm.fwhm_arcsec(args), 3078.4476, 4)

        with self.assertRaises(TypeError):
            csm.fwhm_arcsec([])

        args = [0.5, 3, 1, -1, -1, 1, 0]
        self.assertEqual(csm.fwhm_arcsec(args), 0.25)

        args = [0.5, 3, 1, -1, 0, 1, 0]
        self.assertEqual(csm.fwhm_arcsec(args), 0.25)

        args = [0.5, 3, 1, 0, 0, 1, 0]
        self.assertEqual(csm.fwhm_arcsec(args), 0.25)

        args = [2000, 0.5, 1, 1, 1, 10000, 100]
        self.assertAlmostEqual(csm.fwhm_arcsec(args), 9235342.9410, 4)

        args = [0.5, 1, 0, 0, 1, 1, 1]
        self.assertEqual(csm.fwhm_arcsec(args), 0.5)

class TestHttpRequest(unittest.TestCase):
    def test_get_credentials(self):
        with capture_output() as (out, err):
            unm, pas, url = hrq.get_credentials()
            self.assertIsNone(unm)
            self.assertIsNone(pas)
            self.assertIsNone(url)
            output = out.getvalue().strip()
            self.assertTrue('could not load' in output)

        text = """
;
;  initial comments in file
;comment line with comment marker not in column 1 not allowed
;

[http-arch]
USER    =   maximal_user
PASSWD  =   maximal_passwd
URL     =   data.somewhere.net
"""
        f = open('temp.ini', 'w')
        f.write(text)
        f.close()
        unm, pas, url = hrq.get_credentials('temp.ini', 'http-arch')
        self.assertEqual(unm, 'maximal_user')
        self.assertEqual(pas, 'maximal_passwd')
        self.assertEqual(url, 'data.somewhere.net')
        os.unlink('temp.ini')


    def test_Request_init(self):
        auth = ('X', 'Y')
        req = hrq.Request(auth)
        self.assertEqual(auth, req.auth)

    @patch('despymisc.http_requests.urllib2.Request')
    def test_Request_POST(self, prq):
        data = 'hello=5'
        url = 'data.somewhere.net'
        with self.assertRaises(ValueError):
            req = hrq.Request([])
            req.POST(None, data)

        with self.assertRaises(ValueError):
            req = hrq.Request([])
            req.POST(None, {})

        with patch('despymisc.http_requests.urllib2.urlopen', side_effect=Exception("Bad call")) as ptch:
            req = hrq.Request([])
            req.POST(url, {})
            self.assertTrue(req.error_status[0])
            self.assertTrue('Bad call' in req.error_status[1])

        with patch('despymisc.http_requests.urllib2.urlopen', return_value='rval') as ptch:
            req = hrq.Request(['uname', 'pwd'])
            req.POST(url, {'request':'hello'})
            self.assertFalse(req.error_status[0])
            self.assertEqual(req.response, 'rval')

    @patch('despymisc.http_requests.urllib2.Request')
    def test_Request_get_read(self, prq):
        url = 'data.somewhere.net'

        with self.assertRaises(ValueError):
            req = hrq.Request(['uname', 'pwd'])
            req.get_read(None)

        with patch('despymisc.http_requests.urllib2.urlopen', side_effect=Exception("Bad call")) as ptch:
            req = hrq.Request(['uname', 'pwd'])
            req.get_read(url)
            self.assertTrue(req.error_status[0])

        with patch('despymisc.http_requests.urllib2.urlopen', side_effect=Exception("Bad call")) as ptch:
            req = hrq.Request([])
            req.get_read(url)
            self.assertTrue(req.error_status[0])

        with patch('despymisc.http_requests.urllib2.urlopen') as pop:
            req = hrq.Request(['uname', 'pwd'])
            resp = req.get_read(url)
            self.assertFalse(req.error_status[0])
            self.assertTrue('urlopen().read()' in str(resp))

    @patch('despymisc.http_requests.urllib2.Request')
    def test_Request_download_file(self, prq):
        output = 'test.output'
        with patch('despymisc.http_requests.urllib2.urlopen', mock_open(read_data='return data')) as mo:
            req = hrq.Request(['uname', 'pwd'])
            req.download_file('data.somewhere.net', output)
            f = open(output, 'r')
            self.assertTrue('return data' in f.readline())
            os.unlink(output)

    @patch('despymisc.http_requests.urllib2.Request')
    def test_Request_GET(self, prq):
        url = 'data.somewhere.net'
        with self.assertRaises(ValueError):
            req = hrq.Request(['uname', 'pwd'])
            req.GET(None)

        with patch('despymisc.http_requests.urllib2.urlopen', side_effect=Exception("Bad call")) as ptch:
            req = hrq.Request([])
            req.GET(url, {'item1':2, 'item2':'val2'})
            self.assertTrue(req.error_status[0])
            self.assertTrue('Bad call' in req.error_status[1])

        with patch('despymisc.http_requests.urllib2.urlopen', return_value='rval') as ptch:
            req = hrq.Request(['uname', 'pwd'])
            req.GET(url, {'request':'hello'})
            self.assertFalse(req.error_status[0])
            self.assertEqual(req.response, 'rval')

    @patch('despymisc.http_requests.urllib2.Request')
    def test_Request_download_file(self, prq):
        url = 'data.somewhere.net'
        text = """
;
;  initial comments in file
;comment line with comment marker not in column 1 not allowed
;

[http-arch]
USER    =   maximal_user
PASSWD  =   maximal_passwd
URL     =   data.somewhere.net
"""
        f = open('temp.ini', 'w')
        f.write(text)
        f.close()

        output = 'test.output'
        with patch('despymisc.http_requests.urllib2.urlopen', mock_open(read_data='return data')) as mo:
            hrq.download_file_des(url, output, 'temp.ini', 'http-arch')
            f = open(output, 'r')
            self.assertTrue('return data' in f.readline())
            os.unlink(output)
            os.unlink('temp.ini')

class TestMiscutils(unittest.TestCase):
    class TestError(OSError):
        def __init__(self, text=''):
            OSError.__init__(self, text)
            self.errno = errno.EEXIST

    class TestObj(object):
        def __init__(self, val):
            self.use_db = val

    def setUp(self):
        self.tch = 'TEST_CHECK_LVL'

    def test_fwdebug(self):
        msg = 'My message'
        pre = 'prfx'
        os.environ[self.tch] = '4'
        with capture_output() as (out, err):
            mut.fwdebug(5, self.tch, msg, pre)
            output = out.getvalue().strip()
            self.assertFalse(output)

        os.environ[self.tch] = '5'
        with capture_output() as (out, err):
            mut.fwdebug(5, self.tch, msg, pre)
            output = out.getvalue().strip()
            self.assertTrue(msg in output)
            self.assertTrue(pre in output)

    def test_fwdebug_check(self):
        os.environ[self.tch] = '0'
        self.assertFalse(mut.fwdebug_check(5, self.tch))
        self.assertFalse(mut.fwdebug_check(3, 'UNKNOWN'))
        self.assertFalse(mut.fwdebug_check(3, 'UNKNOWN_X'))
        os.environ['TEST_DEBUG'] = '8'
        del os.environ[self.tch]
        self.assertTrue(mut.fwdebug_check(5, self.tch))
        os.environ['self.tch'] = '5'
        self.assertTrue(mut.fwdebug_check(5, self.tch))
        os.environ['DESDM_DEBUG'] = '2'
        self.assertFalse(mut.fwdebug_check(5, self.tch))
        os.environ['DESDM_DEBUG'] = '10'
        self.assertTrue(mut.fwdebug_check(5, self.tch))

    def test_fwdebug_print(self):
        msg = 'My message'
        pre = 'prfx'
        with capture_output() as (out, err):
            mut.fwdebug_print(msg, pre)
            output = out.getvalue().strip()
            self.assertTrue(msg in output)
            self.assertTrue(pre in output)

    def test_fwdie(self):
        msg = 'My message'
        with capture_output() as (out, err):
            with self.assertRaises(SystemExit) as cm:
                mut.fwdie(msg, 95, 2)
            self.assertEqual(cm.exception.code, 95)
            output = out.getvalue().strip()
            self.assertTrue(msg in output)

    def test_fwsplit(self):
        inp = '[1, 3, 5, 7, 9]'
        self.assertEqual(mut.fwsplit(inp), ['1', '3', '5', '7', '9'])
        inp = '[a|b|c  |d]'
        self.assertEqual(mut.fwsplit(inp, '|'), ['a', 'b', 'c', 'd'])
        inp = '3:5'
        self.assertEqual(mut.fwsplit(inp), ['3', '4', '5'])
        inp = '1,3:5,9'
        self.assertEqual(mut.fwsplit(inp), ['1', '3', '4', '5', '9'])

    def test_coremakedirs(self):
        mut.coremakedirs(None)
        mut.coremakedirs('.')
        with patch('despymisc.miscutils.os.makedirs') as md:
            mut.coremakedirs('here')
        with patch('despymisc.miscutils.os.makedirs', side_effect=OSError()) as md:
            with self.assertRaises(OSError):
                mut.coremakedirs('here2')
        with patch('despymisc.miscutils.os.makedirs', side_effect=self.TestError()) as md:
            mut.coremakedirs('here2')

    def test_parse_fullname(self):
        name = 'file.ext'
        self.assertEqual(mut.parse_fullname(name), name)
        self.assertEqual(mut.parse_fullname('/the/path/' + name + '.fz'), name)
        hduname = name + '[0]'
        self.assertEqual(mut.parse_fullname(hduname), name)
        self.assertEqual(mut.parse_fullname(hduname, mut.CU_PARSE_HDU), '0')
        fname = name + '.fz'
        self.assertEqual(mut.parse_fullname(fname, mut.CU_PARSE_BASENAME), fname)
        self.assertEqual(mut.parse_fullname(name, mut.CU_PARSE_BASENAME), name)

        self.assertEqual(mut.parse_fullname(fname, mut.CU_PARSE_COMPRESSION), '.fz')
        self.assertIsNone(mut.parse_fullname(name + '.ff[0]', mut.CU_PARSE_COMPRESSION))
        self.assertIsNotNone(mut.parse_fullname(name + '.ff[0]', mut.CU_PARSE_FILENAME))
        ret = mut.parse_fullname(fname, mut.CU_PARSE_COMPRESSION | mut.CU_PARSE_FILENAME)
        self.assertEqual(len(ret), 2)
        self.assertEqual(ret[0], name)
        self.assertEqual(ret[1], '.fz')
        fname = name + '.tz'
        os.environ['MISCUTILS_DEBUG'] = '5'
        with capture_output() as (out, err):
            self.assertIsNone(mut.parse_fullname(fname, mut.CU_PARSE_COMPRESSION))
            output = out.getvalue().strip()
            self.assertTrue('Not valid' in output)
        fname = 'file.fz'
        with capture_output() as (out, err):
            self.assertIsNone(mut.parse_fullname(fname, mut.CU_PARSE_COMPRESSION))
            output = out.getvalue().strip()
            self.assertTrue('match pattern' in output)
        os.environ['MISCUTILS_DEBUG'] = '0'
        self.assertIsNone(mut.parse_fullname(name, mut.CU_PARSE_PATH))
        pth = '/the/file/path/'
        ret = mut.parse_fullname(pth + name + '.fz', mut.CU_PARSE_PATH | mut.CU_PARSE_COMPRESSION | mut.CU_PARSE_FILENAME)
        self.assertEqual(len(ret), 3)
        self.assertEqual(ret[0], pth[:-1])
        self.assertIsNone(mut.parse_fullname(name, 10000))

    def test_convertBool(self):
        self.assertTrue(mut.convertBool('True'))
        self.assertTrue(mut.convertBool(1))
        self.assertTrue(mut.convertBool('1'))
        self.assertTrue(mut.convertBool('y'))
        self.assertTrue(mut.convertBool(True))
        with self.assertRaises(Exception):
            mut.convertBool(2.5)

        self.assertFalse(mut.convertBool('False'))
        self.assertFalse(mut.convertBool(0))
        self.assertFalse(mut.convertBool('0'))
        self.assertFalse(mut.convertBool('n'))
        self.assertFalse(mut.convertBool(False))
        self.assertFalse(mut.convertBool(None))
        self.assertFalse(mut.convertBool('q'))

    def test_use_db(self):
        self.assertTrue(mut.use_db('y'))
        self.assertTrue(mut.use_db({'use_db': '1', 'other': False}))
        self.assertTrue(mut.use_db({'other': True}))
        self.assertTrue(mut.use_db(self.TestObj(True)))
        self.assertFalse(mut.use_db(self.TestObj(0)))
        self.assertTrue(mut.use_db(None))
        os.environ['DESDM_USE_DB'] = '1'
        self.assertTrue(mut.use_db(None))
        os.environ['DESDM_USE_DB'] = '0'
        self.assertFalse(mut.use_db(None))

    def test_checkTrue(self):
        self.assertTrue(mut.checkTrue('go', {'go': 1}))
        self.assertTrue(mut.checkTrue('go', {'gogo': 1}))
        self.assertFalse(mut.checkTrue('go', {'gogo': 1}, False))
        self.assertTrue(mut.checkTrue('use_db', self.TestObj(True)))
        self.assertTrue(mut.checkTrue('', True))
        self.assertFalse(mut.checkTrue('', 0))
        self.assertTrue(mut.checkTrue('', None))
        os.environ['DESDM_QQ'] = '1'
        self.assertTrue(mut.checkTrue('QQ', None))
        os.environ['DESDM_QQ'] = '0'
        self.assertFalse(mut.checkTrue('QQ', None))

    def test_pretty_print_dict(self):
        with self.assertRaises(AssertionError):
            mut.pretty_print_dict(None, out_file='my.out')
        with self.assertRaises(AssertionError):
            mut.pretty_print_dict([])
        data = {'item1': [1,3,5],
                'item2': 'abc',
                'item3': {'item4': True, 'item5': 7, 'item6': None}}
        with capture_output() as (out, err):
            mut.pretty_print_dict(data, indent=4)
            output = out.getvalue().strip()
            self.assertTrue(output.startswith('item'))
            self.assertTrue('item2 = abc' in output)
            self.assertTrue('    item4 = True' in output)

        with capture_output() as (out, err):
            mut.pretty_print_dict(data, sortit=True, indent=4)
            output = out.getvalue().strip()
            self.assertTrue(output.startswith('item1'))
            self.assertTrue('item2 = abc' in output)
            self.assertTrue('    item4 = True' in output)
        self.assertIsNone(mut._recurs_pretty_print_dict(None, None, False, '', ''))

    def test_get_config_vals(self):
        extra = {'item1': 5,
                 'item2': 'hello'}
        config = {'item3': True,
                  'item4': [1,2,3]}
        info = mut.get_config_vals(extra, config, {'item1': 'REQ', 'item5': 'OPT'})
        self.assertEqual(len(info.keys()), 1)
        self.assertTrue(info['item1'] == extra['item1'])
        with self.assertRaises(SystemExit):
            mut.get_config_vals(extra, config, {'item1': 'REQ', 'item5': 'REQ'})
        info = mut.get_config_vals(None, None, {'item1': 'OPT', 'item5': 'OPT'})
        self.assertEqual(len(info.keys()), 0)
        info = mut.get_config_vals(extra, config, {'item3': 'req', 'item9': 'opt'})
        self.assertEqual(len(info.keys()), 1)
        self.assertTrue(info['item3'])

    def test_dynamically_load_class(self):
        import psutil as put
        proc = mut.dynamically_load_class('psutil.Process')
        with self.assertRaises(put.NoSuchProcess):
            proc(0)

    def test_updateOrderedDict(self):
        od = OrderedDict()
        od['item1'] = 1234
        mut.updateOrderedDict(od, {'item2': 456, 'item3': 789})
        self.assertEqual(len(od.keys()), 3)

        od = OrderedDict()
        od1 = OrderedDict()
        od1['item2'] = 123
        od['item1'] = od1
        od2 = OrderedDict()
        od2['item1'] = 456

        with self.assertRaises(TypeError):
            mut.updateOrderedDict(od2, od)

        od = OrderedDict()
        od1 = OrderedDict()
        od1['item2'] = 123
        od['item1'] = od1
        od2 = OrderedDict()
        od2['item3'] = 456
        mut.updateOrderedDict(od2, od)
        self.assertEqual(len(od2), 2)
        self.assertTrue('item1' in od2.keys())

        od = OrderedDict()
        od1 = OrderedDict()
        od2 = OrderedDict()
        od3 = OrderedDict()
        od4 = OrderedDict()
        od4['item4'] = 111
        od3['item3'] = od4
        od2['item2'] = od3
        od2['item2.5'] = 555
        od1['item1'] = od2
        od1['item'] = 333

        od = copy.deepcopy(od1)

        del od['item']
        del od['item1']['item2.5']
        od['item1']['item2']['item3']['item4'] = 999

        self.assertNotEqual(od, od1)
        mut.updateOrderedDict(od, od1)
        self.assertEqual(od, od1)

    def test_get_list_directories(self):
        flist = ['/this/is/the/file/path.p', '/this/is/another/path.f']
        ret = mut.get_list_directories(flist)
        self.assertEqual(len(ret), 5)
        for item in ret:
            self.assertTrue(item.startswith('/this'))
        for i in range(1, len(ret)):
            self.assertTrue(ret[i].startswith('/this/is'))

    def test_elapsed_time(self):
        t1 = time.time()
        time.sleep(2)
        ret = mut.elapsed_time(t1)
        secs = float(ret.split()[1].replace('s',''))
        self.assertGreaterEqual(secs, 2.0)
        self.assertAlmostEqual(secs, 2.0, delta=0.05)
        with capture_output() as (out, err):
            ret = mut.elapsed_time(t1, True)
            output = out.getvalue().strip()
            self.assertTrue('Elapsed time' in output)

    def test_query2dict_of_lists(self):
        results = [(('file.1', 12345), ('file.2', 56789), ('file.3', 0))]
        descr = [[['filename', 0],
                  ['size', 0]]]
        myMock = MockDbi(data=results, descr=descr)
        ret = mut.query2dict_of_lists("select filename, size from desfile", myMock)
        self.assertEqual(len(ret), 2)
        self.assertEqual(len(ret['filename']), 3)
        self.assertEqual(ret['filename'][0], 'file.1')
        self.assertEqual(ret['size'][2], 0)

    def test_create_logger(self):
        import logging
        lm = mut.create_logger(logging.INFO, 'mylogger')
        self.assertTrue(isinstance(lm, logging.Logger))
        self.assertEqual(lm.name, 'mylogger')
        self.assertEqual(lm.level, logging.INFO)

class TestSplitAheadBin(unittest.TestCase):
    def test_parseArgs(self):
        temp = copy.deepcopy(sys.argv)
        sys.argv = ['split_ahead_by_ccd.py',
                    '--infile=myfile',
                    '--ccdlist=2,3,4,5']
        args = sabc.parseArgs()
        self.assertFalse(args.verbose)
        self.assertEqual(args.infile, 'myfile')
        sys.argv = temp

    def test_main(self):
        makeAheadFile()
        temp = copy.deepcopy(sys.argv)
        sys.argv = ['split_ahead_by_ccd.py',
                    '--infile=%s' % FILENAME,
                    '--ccdlist=3,4,5',
                    '--outfile=/tmp/junk']
        args = sabc.parseArgs()
        sys.argv = temp

        self.assertTrue(sabc.main(args))
        args.ccdlist = 'All'
        self.assertFalse(sabc.main(args)) # there are missing ccds

        args.outfile = None
        self.assertFalse(sabc.main(args))

        os.unlink(FILENAME)
        self.assertFalse(sabc.main(args))

        args.infile = None
        self.assertFalse(sabc.main(args))

        with capture_output() as (out, err):
            args.verbose = True
            self.assertFalse(sabc.main(args))
            output = out.getvalue().strip()
            self.assertTrue('infile' in output)

class TestRemoveDuplicatesFromList(unittest.TestCase):
    def test_main(self):
        fname = 'test.file'
        f = open(fname, 'w')
        f.write("""# start
a  1  5
b  2  5
c  3  6
d  4  7
e  5  6
# f  6  9
g  7  8
""")
        f.close()
        temp = copy.deepcopy(sys.argv)
        sys.argv = ['remove_duplicates_from_list.py',
                    fname,
                    '1']
        rdfl.main()
        self.assertEqual(len(rdfl.IDs), 6)
        sys.argv = ['remove_duplicates_from_list.py',
                    fname,
                    '2']
        rdfl.IDs = []
        rdfl.main()
        self.assertEqual(len(rdfl.IDs), 4)
        sys.argv = temp
        os.unlink(fname)

if __name__ == '__main__':
    unittest.main()
