version_major = 3
version_minor = 3
version_micro = 2
version_extra = ''

# Format expected by setup.py and doc/source/conf.py: string of form "X.Y.Z"
__version__ = "{}.{}.{}{}".format(version_major,
                              version_minor,
                              version_micro,
                              version_extra)
CLASSIFIERS = ['Development Status :: 5 - Production/Stable',
               'Environment :: Console',
               'Environment :: X11 Applications',
               'Intended Audience :: Developers',
               'Intended Audience :: Science/Research',
               'Intended Audience :: Education',
               'Operating System :: OS Independent',
               "Programming Language :: Python :: 3.6",
               "Programming Language :: Python :: 3.7",
               "Programming Language :: Python :: 3.8",
               "Programming Language :: Python :: 3.9",
               "Programming Language :: Python :: 3.10",
               "Programming Language :: Python :: 3 :: Only",
               'Topic :: Scientific/Engineering',
               'Topic :: Utilities',
               'Topic :: Software Development :: Libraries',
               'Topic :: System :: Distributed Computing']


description = 'Soma-Workflow. A unified and simple interface to parallel computing resource'

long_description = """
=============
Soma-Workflow
=============

A unified and simple interface to parallel computing resource
"""

# versions for dependencies
SPHINX_MIN_VERSION = '1.0'

# Main setup parameters
NAME = 'soma-workflow'
ORGANISATION = "Populse"
MAINTAINER = "Populse team"
MAINTAINER_EMAIL = "support@brainvisa.info"
DESCRIPTION = description
LONG_DESCRIPTION = long_description
URL = "https://github.com/populse/soma-workflow"
DOWNLOAD_URL = "https://github.com/populse/soma-workflow"
LICENSE = "CeCILL-B"
CLASSIFIERS = CLASSIFIERS
AUTHOR = "Populse team"
AUTHOR_EMAIL = "support@brainvisa.info"
PLATFORMS = "OS Independent"
PROVIDES = ["soma-workflow"]
REQUIRES = [
    "six",
]
EXTRA_REQUIRES = {
    "plotting": ["matplotlib"],
    "client": ["zmq", "paramiko"],
    "doc": ["sphinx>=" + SPHINX_MIN_VERSION]}

#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

DB_VERSION = '3.1'
DB_PICKLE_PROTOCOL = 2  # python 2/3 compatible (should be, but is not)
