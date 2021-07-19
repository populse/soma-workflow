# -*- coding: utf-8 -*-

# input: "input" param in json dict
# output: filename is an output of this job

from __future__ import print_function

from __future__ import absolute_import
import os
import sys
import json
import shutil

# get the input pams file location from env variable
param_file = os.environ.get('SOMAWF_INPUT_PARAMS')
# read it
with open(param_file) as f:
    param_json = json.load(f)
parameters = param_json['parameters']
# now get our specific parameter(s)
filePathIn = parameters['input']

in_dir = os.path.dirname(filePathIn)
out_dir = os.path.join(in_dir, 'job8_output')
try:
    os.mkdir(out_dir)
except OSError:
    pass  # already exists because of previous run or concurrent run

filePathOut = os.path.join(out_dir, 'job8_%s' % os.path.basename(filePathIn))

with open(filePathIn) as fin:
    with open(filePathOut, 'w') as fout:
        print('job8 output:', file=fout)
        print('------------', file=fout)
        fout.write(fin.read())

# write output parameters
output_param_file = os.environ.get('SOMAWF_OUTPUT_PARAMS')

if output_param_file:
    out_params = {
        'output': filePathOut,
    }
    with open(output_param_file, 'w') as f:
        json.dump(out_params, f)
