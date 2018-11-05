"""
Soma-workflow is a unified and simple interface to parallel computing
resources. It aims at making easier the use of parallel resources by
non expert users and software.
"""
import os
import ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages
import os.path as op
commands = [op.join('bin', 'soma_delete_all_workflows'),
            op.join('bin', 'soma_stop_workflow'),
            op.join('bin', 'soma_workflow_gui'),
            op.join('bin', 'soma_restart_workflow'),
            op.join('bin', 'soma_submit_workflow'), ]

python_dir = os.path.join(os.path.dirname(__file__), "python")
release_info = {}

with open(os.path.join(python_dir, "soma_workflow", "info.py")) as f:
    code = f.read()
    exec(code, release_info)

setup(
    name=release_info["NAME"],
    description=release_info["DESCRIPTION"],
    long_description=release_info["LONG_DESCRIPTION"],
    license=release_info["LICENSE"],
    classifiers=release_info["CLASSIFIERS"],
    author=release_info["AUTHOR"],
    author_email=release_info["AUTHOR_EMAIL"],
    version=release_info["__version__"],
    url=release_info["URL"],
    package_dir={'': 'python'},
    packages=find_packages(python_dir),
    package_data={'soma_workflow.gui': ['*ui', 'icon/*png'],
                  'soma_workflow.test.data.jobExamples':
                      ['complete/file*', 'complete/stdin*',
                       'complete/output_models/*', 'mpi/*',
                       'special_transfers/*'],
                  },
    platforms=release_info["PLATFORMS"],
    extras_require=release_info["EXTRA_REQUIRES"],
    install_requires=release_info["REQUIRES"],
    scripts=commands,
)
