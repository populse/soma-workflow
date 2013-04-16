import sys
import time
from datetime import datetime

if len(sys.argv) == 2:
  period = int(sys.argv[1])
else:
  period = 10

beginning = datetime.now()
time.sleep(period)
sys.stdout.write(repr(datetime.now() - beginning) + " \n")
