#!/usr/bin/env python

import glob
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("LICENSE.md", "r") as fh:
    long_license = fh.read()

bin_files = glob.glob("bin/*")

setuptools.setup(name='DESPipeline',
                 version='0.1',
                 descriptions='DES public pipeline',
                 author='Dark Energy Survey',
                 url='https://github.com/DarkEnergySurvey/DESPipeline',
                 long_description=long_description,
                 long_description_content_type="text/markdown",
                 license=long_license,
                 classifiers=['Programming Language :: Pythone :: 2',
                              'Operating System :: OS Independent'
                             ],
                 packages=['despyastro',
                           'despyfitsutils',
                           'despymisc',
                           'despyserviceaccess',
                           'deswebdav',
                           'filemgmt',
                           'intgutils',
                           'processingfw'],
                 package_dir={'': 'python'},
                 scripts=bin_files,
                 install_requires=['numpy==1.9.1',
                                   'scipy==0.14',
                                   'fitsio==0.9.8',
                                   'pyfits==3.3',
                                   'dateutil==1.5',
                                   'requests==2.10.0',
                                   'pycurl==7.43.0.2',
                                   'globusonline',
                                   'psutil==4.3.1',
                                   'pytz==2015.7'])
