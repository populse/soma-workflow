#!/usr/bin/env python

# calls job1 using params IO as json

# outputs: let's say filePathOut1 is given as input (the filename),
# and filePathOut2 filename is an output of this job

from __future__ import print_function

import os
import json
import subprocess

# get the input pams file location from env variable
param_file = os.environ.get('SOMAWF_INPUT_PARAMS')
# read it
params = json.load(open(param_file))
parameters = params['parameters']
# now get our specific parameter(s)
filePathIn = parameters['filePathIn']
filePathOut1 = parameters['filePathOut1']
timeToSleep = parameters.get['timeToSleep']

my_dir = os.path.dirname(sys.argv[0])
script = os.path.join(my_dir, 'job1.py')

filePathOut2 = os.path.join(os.path.dirname(filePathOut1), 'job5_filePathOut2')


cmd = [sys.executable, script, filePathIn, filePathOut1, filePathOut2]
if timeToSleep is not None:
    cmd.append(timeToSleep)

subprocess.check_call(cmd)

# write output parameters
out_param_file = os.environ.get('SOMAWF_OUTPUT_PARAMS')

if output_param_file:
    out_params = {
        'filePathOut2': filePathOut2,
    }
    json.dump(out_params, open(output_param_file, 'w'))

