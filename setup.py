
import ez_setup
ez_setup.use_setuptools()

import os.path as op
from setuptools import setup, find_packages, Extension, Distribution

commands = [op.join('bin','soma_workflow_gui')]

setup(
    name="soma-workflow", 
    author='Soizic Laguitton',
    author_email='soizic.laguitton@gmail.com',
    version='2.1',
    url='http://www.brainvisa.info/soma-workflow',
    package_dir = {'' : 'python'},
    packages=find_packages('python'),
    include_package_data=True,
    package_data={'soma.workflow.gui':['*ui','icon/*png']},
    scripts=commands,
    platforms=['linux'],
    )

