import sys
import time

## program arguments
#filepathout1 = sys.argv[0]
#filepathout2 = sys.argv[1]
#sys.stdout.write("outputs = " + filepathout1 + " and " + filepathout2 + "\n") 

## read twice form stdin
#param1 = sys.stdin.readline()
#param2 = sys.stdin.readline()
#sys.stdout.write("parametre 1 = " + param1)
#sys.stdout.write("parametre 2 = " + param2)

#sys.stdout.write("len(sys.args) = " + repr(len(sys.argv)) + "\n")

if len(sys.argv) < 4 or len(sys.argv) > 5:
  sys.stdout.write("The program takes 3 or 4 arguments! \n")
  sys.exit()

sys.stdout.write("Job1: What should we add to the end of the first file ?")
comment1 = sys.stdin.readline()
sys.stdout.write("Job1: added to the end of the first output file : " + comment1 + "\n")

sys.stdout.write("Job1: What should we add to the end of the second file ?")
comment2 = sys.stdin.readline()
sys.stdout.write("Job1: added to the end of the second output file : " + comment2 + "\n")

timeToSleep=0
if len(sys.argv) == 5:
  timeToSleep = int(sys.argv[4])
for i in range(1,timeToSleep+1):
  time.sleep(1)
  sys.stdout.write(repr(i)+" ")
  sys.stdout.flush()
sys.stdout.write("\n")

filePathIn = sys.argv[1] #argv[0]=="job1.py"
#sys.stdout.write("input file = " + filePathIn + "\n")
filePathOut1 = sys.argv[2]
#sys.stdout.write("output file 1 = " + filePathOut1 + "\n")
filePathOut2 = sys.argv[3]
#sys.stdout.write("output file 2 = " + filePathOut2 + "\n")

fileOut1 = open(filePathOut1, "w")
fileOut2 = open(filePathOut2, "w")

print >> fileOut1, "1****************job1**************"
print >> fileOut2, "1****************job1**************"

fileIn = open(filePathIn) 
line = fileIn.readline()
i = 0
while line:
  if i%2 == 0:
    print >> fileOut2, "1 " + repr(i) + " " + line,
  else:
    print >> fileOut1, "1 " + repr(i) + " " + line,
  line = fileIn.readline()
  i += 1
  

print >> fileOut1, "1 "
print >> fileOut1, "1 job1 out1: stdin comment:"
print >> fileOut1, "1 "+ comment1,
print >> fileOut1, "1******************************************************"

print >> fileOut2, "1 "
print >> fileOut2, "1 job1 out2: stdin comment:************"
print >> fileOut2, "1 "+ comment2,
print >> fileOut2, "1******************************************************"
