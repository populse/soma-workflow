# -*- coding: utf-8 -*-

# calls job1 using params IO as json

# outputs: filePathOut filename is an output of this job

from __future__ import print_function

from __future__ import absolute_import
import os
import sys
import json
import subprocess

# get the input pams file location from env variable
param_file = os.environ.get('SOMAWF_INPUT_PARAMS')
# read it
with open(param_file) as f:
    params = json.load(f)
parameters = params['parameters']
# now get our specific parameter(s)
filePathIn1 = parameters['filePathIn1']
filePathIn2 = parameters['filePathIn2']
timeToSleep = parameters.get('timeToSleep')

my_dir = os.path.dirname(sys.argv[0])
script = parameters.get('script_job2', os.path.join(my_dir, 'job2.py'))

filePathOut = os.path.join(os.path.dirname(filePathIn1), 'job6_filePathOut')


cmd = [sys.executable, script, filePathIn1, filePathIn2, filePathOut]
if timeToSleep is not None:
    cmd.append(timeToSleep)

subprocess.check_call(cmd, stdin=sys.stdin)

# write output parameters
output_param_file = os.environ.get('SOMAWF_OUTPUT_PARAMS')

if output_param_file:
    out_params = {
        'filePathOut': filePathOut,
    }
    with open(output_param_file, 'w') as f:
        json.dump(out_params, f)
