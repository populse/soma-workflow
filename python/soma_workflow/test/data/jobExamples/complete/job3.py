from __future__ import print_function

import sys
import time

if len(sys.argv) < 3 or len(sys.argv) > 4:
    sys.stdout.write("The program takes 2 or 3 arguments! \n")
    sys.exit()

sys.stdout.write("Job3: What should we add to the end of the output file ?")
comment = sys.stdin.readline()
sys.stdout.write(
    "Job3: added to the end of the output file : " +
    comment +
    "\n")


filePathIn = sys.argv[1]
filePathOut = sys.argv[2]

#sys.stdout.write("Input file = " + filePathIn + "\n")
#sys.stdout.write("Output file = " + filePathOut + "\n")

timeToSleep = 0
if len(sys.argv) == 4:
    timeToSleep = int(sys.argv[3])
for i in range(1, timeToSleep + 1):
    time.sleep(1)
    sys.stdout.write(repr(i) + " ")
    sys.stdout.flush()
sys.stdout.write("\n")


fileIn = open(filePathIn)
fileOut = open(filePathOut, "w")
print("3****************job3***********************", file=fileOut)
line = fileIn.readline()
while line:
    print("3 " + line, file=fileOut, end='')
    line = fileIn.readline()

print("3 ", file=fileOut)
print("3 job3: stdin comment:", file=fileOut)
print("3 " + comment, file=fileOut, end='')
print("3*************************************************************",
      file=fileOut)
