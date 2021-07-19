# -*- coding: utf-8 -*-

# calls job1 using params IO as json

# outputs: let's say filePathOut1 is given as input (the filename),
# and filePathOut2 filename is an output of this job

from __future__ import print_function

from __future__ import absolute_import
import os
import json
import subprocess
import sys

script = sys.argv[1]
filePathIn = sys.argv[2]
filePathOut1 = sys.argv[3]
timeToSleep = None
if len(sys.argv) > 4:
    timeToSleep = sys.argv[4]

my_dir = os.path.dirname(sys.argv[0])

filePathOut2 = os.path.join(os.path.dirname(filePathOut1), 'job5_filePathOut2')


cmd = [sys.executable, script, filePathIn, filePathOut1, filePathOut2]
if timeToSleep is not None:
    cmd.append(timeToSleep)

subprocess.check_call(cmd, stdin=sys.stdin)

# write output parameters
output_param_file = os.environ.get('SOMAWF_OUTPUT_PARAMS')

if output_param_file:
    out_params = {
        'filePathOut2': filePathOut2,
    }
    with open(output_param_file, 'w') as f:
        json.dump(out_params, f)
