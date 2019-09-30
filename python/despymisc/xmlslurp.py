#!/usr/bin/env python
"""
xmlslurper reads in an xml formatted file and converts it to a python dictionary
"""
import xml.parsers.expat

class Xmlslurper(object):
    """
    Class to read in an xml formatted file and convert the table contents to a python dictionary

    takes the filename and the expected table names as inputs
    """
    def __init__(self, filename, tablenames):
        self.data = {}


        ##################################################################
        def start_element(name, attrs, data=self.data):
            """ Overrides the xml parser start element routine, specifically looks
                for table, field, tr, and td tags, ingoring all others

                Parameters
                ----------
                name : str
                    The name of the tag
                attrs : dict
                    Any tag attributes
                data : dict
                    The current data structure

            """
            if name.upper() == 'TABLE':
                # skip if not one of the desired tables
                if not attrs['name'] in self.data['wanted_tables']:
                    return

                # initialize values for a new table
                data['curtable'] = attrs['name']
                data['fieldnames'] = []
                data['fieldtypes'] = []
                data['fieldarray'] = []
                data['tables'][data['curtable']] = []

            if name.upper() == 'FIELD' and self.data['curtable']:
                # save description information
                data['fieldnames'].append(attrs['name'].lower())
                data['fieldtypes'].append(attrs['datatype'])
                data['fieldarray'].append(attrs.get('arraysize', None))

            if name.upper() == 'TR':
                # new row, inialize row values
                data['col'] = 0             # current column
                data['prevcol'] = 0         # previous column
                data['prevtext'] = ''       # previous text parsed in case partial due to buffer
                data['currow'] = {}         # dictionary to store info from row

            if name.upper() == 'TD':
                # save state that are in a TD section
                data['in_TD'] = True


        ##################################################################
        def end_element(name, data=self.data):
            """ Overrides the xml parser end element routine, specifically looks
                for table, tr, and td tags, ingoring all others

                Parameters
                ----------
                name : str
                    The name of the tag
                data : dict
                    The current data structure
            """
            if name.upper() == 'TD':
                # if closed TD section, change TD state
                data['col'] += 1
                data['in_TD'] = False

            if name.upper() == 'TR' and data['curtable']:
                # save current row dictionary to current table
                data['tables'][data['curtable']].append(data['currow'])

            if name.upper() == 'TABLE' and data['curtable']:
                # empty table variables
                data['curtable'] = None
                del self.data['fieldnames']
                del self.data['fieldtypes']
                del self.data['fieldarray']


        ##################################################################
        def char_data(text, data=self.data):
            """ Overrides the xml parser char_data routine. It converts the
                contents of <td> tags into their expected data type and format

                Parameters
                ----------
                test : str
                    The contents of the current tag
                data : dict
                    The current data structure

            """
            prevtext = text

            if data['in_TD'] and self.data['curtable']:
                # if still same column, need to join with previous data
                if data['prevcol'] == data['col']:
                    text = data['prevtext'] + text

                curarrsize = data['fieldarray'][data['col']]
                curtype = data['fieldtypes'][data['col']]
                curname = data['fieldnames'][data['col']]

                if curarrsize != None and curtype != 'char':
                    # data is for an array field
                    # assumes array cannot be of strings

                    # so split into separate values
                    vals = text.strip().split()

                    # convert values to right type
                    if curtype == 'int':
                        for i, val in enumerate(vals):
                            vals[i] = int(val)
                    elif curtype == 'float':
                        for i, val in enumerate(vals):
                            vals[i] = float(val)

                    # save data array to current row data
                    data['currow'][curname] = vals
                else:
                    # single value, convert to right type
                    if curtype == 'int':
                        val = int(text)
                    elif curtype == 'float':
                        val = float(text)
                    else:
                        val = text

                    # save data array to current row data
                    data['currow'][curname] = val

                # save state for next char_data call
                data['prevtext'] = prevtext
                data['prevcol'] = data['col']


        ##################################################################
        # actual code for __init__

        # initialize values
        # which tables we want values from
        self.data['wanted_tables'] = tablenames

        # name of table currently parsing
        self.data['curtable'] = None
        self.data['params'] = {}

        # dictionary of tables
        #   tables are arrays of row dict
        self.data['tables'] = {}
        self.data['in_TD'] = False   # whether in TD section or not

        p = xml.parsers.expat.ParserCreate()
        #p.buffer_size=32768
        p.buffer_size = 2048

        # assign functions to handler
        p.StartElementHandler = start_element
        p.EndElementHandler = end_element
        p.CharacterDataHandler = char_data

        fl = open(filename, "r")
        p.ParseFile(fl)
        fl.close()

        #
        # clean out our bookkeeping
        #
        del self.data['curtable']
        del self.data['wanted_tables']
        del self.data['in_TD']


    ##################################################################
    def gettables(self):
        """ Method to get the full contents of the data table structure

            Returns
            -------
            Dict containing the current table data

        """
        return self.data['tables']

    #
    # we look like our data member...
    #
    def __getattr__(self, blah):
        return getattr(self.data['tables'], blah)




if __name__ == "__main__":  # pragma no coverage
    tablelist = ("Astrometric_Instruments",
                 "FGroups",
                 "Fields",
                 "Photometric_Instruments",
                 "PSF_Extensions",
                 "PSF_Fields",
                 "Warnings")
    import sys
    import glob
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    if len(sys.argv) > 1:
        pp.pprint(Xmlslurper(sys.argv[1], tablelist).gettables())
    else:
        for f in glob.glob('*.xml'):
            print "f: ", f
            pp.pprint(Xmlslurper(f, tablelist).gettables())
