
import ez_setup
ez_setup.use_setuptools()

import os.path as op
from setuptools import setup, find_packages, Extension, Distribution

commands = [op.join('bin','soma_workflow_gui')]



setup(
    name="soma-workflow", author='Soizic Laguitton',
    author_email='soizic.laguitton@gmail.com',
    version='1.0',
    #setup_requires=['numpy>=1.0'],
    #install_requires=['Pyro==3.10'],
    # dependency_links = ["http://sourceforge.net/project/showfiles.php?" \
    #                         "group_id=126549&package_id=234596", 
    #                     "http://sourceforge.net/projects/pyro/files/",
    #                     'http://www.pytables.org/download/preliminary/'],
    package_dir = {'' : 'python'},
    packages=find_packages('python'),
    include_package_data=True,
    package_data={'soma.workflow.gui':['gui/*ui','icon/*png']},
    #ext_modules=cExtensions,
    #cmdclass={'build_ext':build_ext},
    scripts=commands,
    platforms=['linux'],
    #zip_safe=False,
    #configuration=configuration,
    )

