# -*- coding: utf-8 -*-
from __future__ import absolute_import
import sys

if __name__ == "__main__":

    if len(sys.argv) == 2:
        output_file = sys.argv[1]
        f = open(output_file, 'w')
        f.write("a very original message: Hello world!")
        f.close()
