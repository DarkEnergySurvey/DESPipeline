"""
    .. _despymisc-misctime:

    **misctime**
    ------------

    Miscellaneous date/time utility
"""

import datetime
from dateutil.parser import parse
from dateutil import tz

def convert_utc_str_to_nite(datestr):
    """ Convert an UTC date string to a nite string, it takes into account
        the time zone of observation, which is hard coded to 'America/Santiago'.
        If the observation was taken before 15 hours then it is considered to
        be from the previous night.

        Parameters
        ----------
        datestr : str
            The formatted timestamp of the observation time. Format is
            YYYY-MM-DDTHH:MM:SS.S+HH:MM

        Returns
        -------
        str
            The nite of the observation


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
