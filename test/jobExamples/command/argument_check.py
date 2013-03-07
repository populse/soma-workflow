import sys
cmpt = 0
print "nb of arguments: " + repr(len(sys.argv))
for arg in sys.argv:
  print repr(cmpt) + " => " + repr(arg)
  cmpt = cmpt + 1

