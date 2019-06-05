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

from __future__ import absolute_import
import sys

if sys.version_info[:2] >= (3, 2):
    # in python >= 3.2, subprocess32 is not needed as it is the builtin
    # subprocess module
    from subprocess import *
else:
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

        def __initialize_subprocess():
            import subprocess

        __initialize_subprocess()
        del __initialize_subprocess

        if sys.version_info[:2] <= (2, 6):
            # check_output did not exist in python 2.6
            def check_output(*popenargs, **kwargs):
                r"""Run command with arguments and return its output as a byte string.

                If the exit code was non-zero it raises a CalledProcessError.  The
                CalledProcessError object will have the return code in the returncode
                attribute and output in the output attribute.

                The arguments are the same as for the Popen constructor.  Example:

                >>> check_output(["ls", "-l", "/dev/null"])
                'crw-rw-rw- 1 root root 1, 3 Oct 18  2007 /dev/null\n'

                The stdout argument is not allowed as it is used internally.
                To capture standard error in the result, use stderr=STDOUT.

                >>> check_output(["/bin/sh", "-c",
                ...               "ls -l non_existent_file ; exit 0"],
                ...              stderr=STDOUT)
                'ls: non_existent_file: No such file or directory\n'
                """
                if 'stdout' in kwargs:
                    raise ValueError('stdout argument not allowed, it will be overridden.')
                process = Popen(stdout=PIPE, *popenargs, **kwargs)
                output, unused_err = process.communicate()
                retcode = process.poll()
                if retcode:
                    cmd = kwargs.get("args")
                    if cmd is None:
                        cmd = popenargs[0]
                    raise CalledProcessError(retcode, cmd, output=output)
                return output

