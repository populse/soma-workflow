# -*- coding: utf-8 -*-

# input: "inputs" param in json dict, list of files
# output: "output" filename is an intput of this job

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
filePathsIn = parameters['inputs']
filePathOut = parameters['output']

with open(filePathOut, 'w') as fout:
    for filein in filePathsIn:
        with open(filein) as fin:
            fout.write(fin.read())
