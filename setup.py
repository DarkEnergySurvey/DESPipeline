import distutils
from distutils.core import setup

# The main call
setup(name='deswebdav',
      version ='1.2.0',
      license = "GPL",
      description = "DESDM's integration ut",
      author = "Doug Friedel",
      author_email = "friedel@astro.illinois.edu",
      packages = ['deswebdav'],
      package_dir = {'': 'python'},
      data_files=[('ups',['ups/deswebdav.table'])]
      )

