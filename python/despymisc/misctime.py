# $Id: misctime.py 40842 2015-11-19 15:49:50Z felipe $
# $Rev:: 40842                            $:  # Revision of last commit.
# $LastChangedBy:: felipe                 $:  # Author of last commit.
# $LastChangedDate:: 2015-11-19 09:49:50 #$:  # Date of last commit.

""" Miscellaneous date/time utilities """

import datetime
from dateutil.parser import parse
from dateutil import tz

def convert_utc_str_to_nite(datestr):

    """
    Convert an UTC date string to a nite string, but without using pytz
    to avoid unnecessary dependancy. Translated from the old convert_utc_str_to_nite()
    F. Menanteau.
    """

    # e.g. datestr: 2014-08-15T17:31:02.416533+00:00
    nite = None

    # Hardcode time zone
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/Santiago')

    # convert date string to datetime object
    utc = parse(datestr)
    utc = utc.replace(tzinfo=from_zone)

    # Convert time zone to local on mountain
    local_dt = utc.astimezone(to_zone)
    noon_dt = local_dt.replace(hour=12, minute=0, second=0, microsecond=0)
    if local_dt < noon_dt:  # if before noon, observing nite has previous date
        obsdate = (local_dt - datetime.timedelta(days=1)).date()
    else:
        obsdate = local_dt.date()

    # Get the nite only -- not the time
    nite = obsdate.strftime('%Y%m%d')
    return nite
