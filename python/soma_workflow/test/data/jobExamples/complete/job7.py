# -*- coding: utf-8 -*-

# inputs: as env var SOMAWF_INPUT_PARAMS
# expect "inputs" parameter: list of filenames
# outputs: copies of files in intermediate_results/ subdirectory

from __future__ import print_function

from __future__ import absolute_import
import os
import json
import sys
import shutil
from six.moves import zip

input_param_file = os.environ.get('SOMAWF_INPUT_PARAMS')
with open(input_param_file) as f:
    input_dict = json.load(f)
input_params = input_dict['parameters']

filePathsIn = input_params['inputs']
out_dir = input_params['output_dir']

if not os.path.exists(out_dir):
    os.mkdir(out_dir)

filePathsOut = [os.path.join(out_dir, os.path.basename(fp))
                for fp in filePathsIn]

for fin, fout in zip(filePathsIn, filePathsOut):
    shutil.copy2(fin, fout)

# write output parameters
output_param_file = os.environ.get('SOMAWF_OUTPUT_PARAMS')

if output_param_file:
    out_params = {
        'outputs': filePathsOut,
    }
    with open(output_param_file, 'w') as f:
        json.dump(out_params, f)
