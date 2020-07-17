# -*- coding: utf-8 -*-
from __future__ import print_function

from __future__ import absolute_import
import sys
import time
from six.moves import range

if len(sys.argv) < 4 or len(sys.argv) > 5:
    sys.stdout.write("The program takes 3 or 4 arguments! \n")
    sys.exit()

sys.stdout.write("Job2: What should we add to the end of the output file ?")
comment = sys.stdin.readline()
sys.stdout.write(
    "Job2: added to the end of the output file : " +
    comment +
    "\n")


filePathIn1 = sys.argv[1]
filePathIn2 = sys.argv[2]
filePathOut = sys.argv[3]

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

with open(filePathOut, "w") as fileOut:
    with open(filePathIn1) as fileIn1:
        print("2****************job2**************", file=fileOut)
        line = fileIn1.readline()
        while line:
            print("2 " + line, file=fileOut, end='')
            line = fileIn1.readline()


    with open(filePathIn2) as fileIn2:
        nblines = len(fileIn2.readlines())
        print("2 ", file=fileOut)
        print("2  # lines:" + repr(nblines), file=fileOut, end=' ')

    print("2 ", file=fileOut)
    print("2 job2: stdin comment:", file=fileOut)
    print("2 " + comment, file=fileOut, end='')
    print("2******************************************************",
          file=fileOut)
