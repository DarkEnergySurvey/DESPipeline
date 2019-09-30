import distutils
from distutils.core import setup

# The main call
setup(name='despyastro',
      version ='0.3.0',
      license = "GPL",
      description = "A set of handy Python astro-related utility functions for DESDM",
      author = "Felipe Menanteau",
      author_email = "felipe@illinois.edu",
      packages = ['despyastro'],
      package_dir = {'': 'python'},
      )

