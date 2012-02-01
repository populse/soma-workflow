"""
Soma-workflow is a unified and simple interface to parallel computing resources. It aims at making easier the use of parallel resources by non expert users and software.
"""

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup
import os.path as op

commands = [op.join('bin','soma_workflow_gui')]

setup(
    name="soma-workflow",
    description=__doc__,
    long_description=file('README.txt', 'r').read(),
    license='CeCILL v2',
    classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          ' Environment :: X11 Applications ',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'Intended Audience :: Education',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2.5',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Topic :: Scientific/Engineering',
          'Topic :: Utilities',
          'Topic :: Software Development :: Libraries',
          'Topic :: System :: Distributed Computing',
      ],
    author='Soizic Laguitton',
    author_email='soizic.laguitton@gmail.com',
    version='2.1.0',
    url='http://www.brainvisa.info/soma-workflow',
    package_dir = {'': 'python'},
    packages=['soma', 'soma.workflow', 'soma.workflow.gui', 'soma.workflow.test'],
    include_package_data=True,
    package_data={'soma.workflow.gui': ['*ui', 'icon/*png']},
    scripts=commands,
    platforms=['Linux', 'MacOS X', 'Windows' ],
    #requires=['PyQt', ],
    extras_require={
       'plotting': ['matplotlib'],
       'client': ['Pyro', 'paramiko'],
    },
    )

