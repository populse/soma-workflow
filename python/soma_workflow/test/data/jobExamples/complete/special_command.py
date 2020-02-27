# -*- coding: utf-8 -*-
from __future__ import print_function

from __future__ import absolute_import
import sys
import os

print("nb of arguments (without script filename): " + repr(len(sys.argv) - 1))
for cmpt, arg in enumerate(sys.argv[1:]):
    print(repr(cmpt) + " => " + arg)
