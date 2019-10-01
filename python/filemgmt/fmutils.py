"""
    .. _filemgmt-fmutils:

    **fmutils**
    -----------

    Miscellaneous FileMgmt utils
"""

class DataObject(object):
    """ Class to turn a dictionary into class elements

    """
    def __init__(self, **kw):
        for item, val in kw.iteritems():
            setattr(self, item, val)

    def get(self, attrib):
        """ Method to get the value of the given attribute

            Parameters
            ----------
            attrib : str
                The name of the attribute to get

            Returns
            -------
            The value of the attribute

        """
        return getattr(self, attrib, None)

    def set(self, attrib, value):
        """ Method to set the value of an attribute

            Parameters
            ----------
            attrib : str
                The name of the attribute to set

            value : vaires
                The value to set the attribute to

        """
        if not hasattr(self, attrib):
            raise Exception("%s is not a member of DataObject." % (attrib))
        setattr(self, attrib, value)
