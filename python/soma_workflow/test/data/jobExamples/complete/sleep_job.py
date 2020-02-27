# -*- coding: utf-8 -*-
from __future__ import absolute_import
import sys
import time

if len(sys.argv) == 2:
    period = int(sys.argv[1])
else:
    period = 10

time.sleep(period)
