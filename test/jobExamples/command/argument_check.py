import sys
cmpt = 0
for arg in sys.argv:
  print repr(cmpt) + " => " + repr(arg)
  cmpt = cmpt + 1

