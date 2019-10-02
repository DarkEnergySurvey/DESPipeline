"""
    .. _intgutils-queryutils:

    **queryutils**
    --------------

    Functions useful for query codes to be called by framework
"""

import json

from intgutils.wcl import WCL
import intgutils.intgdefs as intgdefs

###########################################################
def convert_single_files_to_lines(filelist, initcnt=1):
    """ Convert single files to dict of lines in prep for output """

    count = initcnt
    linedict = {'list': {}}

    if isinstance(filelist, dict) and len(filelist) > 1 and \
            'filename' not in filelist.keys():
        filelist = filelist.values()
    elif isinstance(filelist, dict):  # single file
        filelist = [filelist]

    linedict = {'list': {intgdefs.LISTENTRY: {}}}
    for onefile in filelist:
        fname = "file%05d" % (count)
        lname = "line%05d" % (count)
        linedict['list'][intgdefs.LISTENTRY][lname] = {'file': {fname: onefile}}
        count += 1
    return linedict

###########################################################
def output_lines(filename, dataset, outtype=intgdefs.DEFAULT_QUERY_OUTPUT_FORMAT):
    """ Writes dataset to file in specified output format """

    if outtype == 'xml':
        output_lines_xml(filename, dataset)
    elif outtype == 'wcl':
        output_lines_wcl(filename, dataset)
    elif outtype == 'json':
        output_lines_json(filename, dataset)
    else:
        raise Exception('Invalid outtype (%s).  Valid outtypes: xml, wcl, json' % outtype)


###########################################################
def output_lines_xml(filename, dataset):
    """Writes dataset to file in XML format"""

    with open(filename, 'w') as xmlfh:
        xmlfh.write("<list>\n")
        for datak, line in dataset.items():
            xmlfh.write("\t<line>\n")
            for name, filedict in line.items():
                xmlfh.write("\t\t<file nickname='%s'>\n" % name)
                for key, val in filedict.items():
                    if key.lower() == 'ccd':
                        val = "%02d" % (val)
                    xmlfh.write("\t\t\t<%s>%s</%s>" % (datak, val, datak))
                xmlfh.write("\t\t\t<fileid>%s</fileid>\n" % (filedict['id']))
                xmlfh.write("\t\t</file>\n")
            xmlfh.write("\t</line>\n")
        xmlfh.write("</list>\n")


###########################################################
def output_lines_wcl(filename, dataset):
    """ Writes dataset to file in WCL format """

    dswcl = WCL(dataset)
    with open(filename, "w") as wclfh:
        dswcl.write(wclfh, True, 4)  # print it sorted


###########################################################
def output_lines_json(filename, dataset):

    """ Writes dataset to file in json format """
    with open(filename, "w") as jsonfh:
        json.dump(dataset, jsonfh, indent=4, separators=(',', ': '))
