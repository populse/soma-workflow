"""
Soma-workflow is a unified and simple interface to parallel computing
resources. It aims at making easier the use of parallel resources by
non expert users and software.
"""
import ez_setup
ez_setup.use_setuptools()
from setuptools import setup
import os.path as op
commands = [op.join('bin', 'soma_delete_all_workflows'),
            op.join('bin', 'soma_stop_workflow'),
            op.join('bin', 'soma_workflow_gui'),
            op.join('bin', 'soma_restart_workflow'),
            op.join('bin', 'soma_submit_workflow'), ]
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
        'Programming Language :: Python :: 2.6',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities',
        'Topic :: Software Development :: Libraries',
        'Topic :: System :: Distributed Computing',
    ],
    author='Please check authors at http://www.brainvisa.info/soma-workflow',
    author_email='jinpeng.li@cea.fr',
    version='development version',
    url='http://www.brainvisa.info/soma-workflow',
    package_dir={'': 'python'},
    packages=['somadrmaa',
              'soma_workflow',
              'soma_workflow.gui',
              'soma_workflow.test',
              'soma_workflow.check_requirement'],
    include_package_data=True,
    package_data={'soma_workflow.gui': ['*ui', 'icon/*png']},
    scripts=commands,
    platforms=['Linux', 'MacOS X', 'Windows'],
    # requires=['PyQt', ],
    extras_require={
        'plotting': ['matplotlib'],
        'client': ['Pyro', 'paramiko'],
    },
)
