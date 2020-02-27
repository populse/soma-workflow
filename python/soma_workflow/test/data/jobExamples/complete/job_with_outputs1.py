# -*- coding: utf-8 -*-
from __future__ import print_function

from __future__ import absolute_import
import sys
import time
import json
import os
from six.moves import range

if len(sys.argv) < 3 or len(sys.argv) > 4:
    sys.stdout.write("The program takes 2 or 3 arguments! \n")
    sys.exit()

sys.stdout.write(
    "JobWithOutputs1: What should we add to the end of the output file ?")
comment = sys.stdin.readline()
sys.stdout.write(
    "JobWithOutputs1: added to the end of the output file : " +
    comment +
    "\n")

filePathIn1 = sys.argv[1]
filePathIn2 = sys.argv[2]
# generate output file name
filePathOut = os.path.join(os.path.dirname(filePathIn1), 'job_output_file.txt')
fileParamsOut = sys.environ['SOMAWF_OUTPUT_PARAMS']

timeToSleep = 0
if len(sys.argv) == 5:
    timeToSleep = int(sys.argv[4])
for i in range(1, timeToSleep + 1):
    time.sleep(1)
    sys.stdout.write(repr(i) + " ")
    sys.stdout.flush()
sys.stdout.write("\n")

# sys.stdout.write("Input file 1 = " + filePathIn1 + "\n")
# sys.stdout.write("Input file 2 = " + filePathIn2 + "\n")
# sys.stdout.write("Output file = " + filePathOut + "\n")

fileIn1 = open(filePathIn1)
fileOut = open(filePathOut, "w")

print("2****************job2**************", file=fileOut)
line = fileIn1.readline()
while line:
    print("2 " + line, file=fileOut, end='')
    line = fileIn1.readline()


fileIn2 = open(filePathIn2)
nblines = len(fileIn2.readlines())
print("2 ", file=fileOut)
print("2  # lines:" + repr(nblines), file=fileOut, end=' ')

print("2 ", file=fileOut)
print("2 job2: stdin comment:", file=fileOut)
print("2 " + comment, file=fileOut, end='')
print("2******************************************************", file=fileOut)

del fileOut

output_params = {
    'file3_out': filePathOut,
    'num1_out': 26,
    'text1_out': 'This is an output of job JobWithOutputs1',
}

json.dump(output_params, open(fileParamsOut, 'w'))
