# -*- coding: utf-8 -*-

#  This software and supporting documentation are distributed by
#      Institut Federatif de Recherche 49
#      CEA/NeuroSpin, Batiment 145,
#      91191 Gif-sur-Yvette cedex
#      France
#
# This software is governed by the CeCILL-B license under
# French law and abiding by the rules of distribution of free software.
# You can  use, modify and/or redistribute the software under the
# terms of the CeCILL-B license as circulated by CEA, CNRS
# and INRIA at the following URL "http://www.cecill.info".
#
# As a counterpart to the access to the source code and  rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty  and the software's author,  the holder of the
# economic rights,  and the successive licensors  have only  limited
# liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading,  using,  modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean  that it is complicated to manipulate,  and  that  also
# therefore means  that it is reserved for developers  and  experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or
# data to be ensured and,  more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL-B license and that you accept its terms.

'''
* author: Nicolas Souedet
* organization: NeuroSpin
* license: `CeCILL B <http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html>`_

Import subprocess32 or subprocess API depending on python version and what is
available on the system.
'''

from __future__ import absolute_import, print_function
import sys

# from soma_workflow import swf_override_subprocess

# print('soma_workflow subprocess override:', swf_override_subprocess)
# if sys.version_info[:2] >= (3, 2) or not swf_override_subprocess:
if sys.version_info[:2] >= (3, 2):
    # in python >= 3.2, subprocess32 is not needed as it is the builtin
    # subprocess module
    #
    # if subprocess is already loaded we do not replace it because it
    # can lead to incompatibility issues (espacially in sphinx.ext.graphviz)
    from subprocess import *

else:
    # print('!!!! soma_workflow overriding subprocess !!!!')
    try:
        def __initialize_zmq():
            # It is necessary to first import zmq from the system if it is
            # installed otherwise the one embeded with subprocess32 is loaded
            # and it can lead to compatibility issue
            import zmq

        __initialize_zmq()

        del __initialize_zmq

    except ImportError:
        pass

    try:
        # It is necessary to replace subprocess by subprocess32 to fix issue
        # in subprocess start
        # Import in current module all that is defined in subprocess module
        from subprocess32 import *

        def __initialize_subprocess32():
            import subprocess32
            import subprocess as _subprocess
            if hasattr(_subprocess, '_args_from_interpreter_flags'):
                # get this private function which is used somewhere in
                # multiprocessing
                subprocess32._args_from_interpreter_flags \
                    = _subprocess._args_from_interpreter_flags
            del _subprocess
            import sys
            sys.modules['subprocess'] = sys.modules['subprocess32']

        __initialize_subprocess32()
        del __initialize_subprocess32

    except ImportError:
        from subprocess import *
        # disable this print becauses it causes commands that start the
        # servers to fail (they parse stdout and don't expect this message)
        # print('subprocess module will be used to start shell commands. Due to '
            #'issues in this module this can lead to problems during execution. '
            #'You should probably install subprocess32 module to avoid these '
            #'problems.')

        def __initialize_subprocess():
            import subprocess

        __initialize_subprocess()
        del __initialize_subprocess
