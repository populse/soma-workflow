# -*- coding: utf-8 -*-
from __future__ import absolute_import
import sys
import os
import stat
import shutil

'''
Takes 2 parameters: path of the input directory
                    path of the output directory
The program lists the contents of the input directory and copy the content of
the input directory in the output directory.
'''


def contents(path_seq):
    result = []
    for path in path_seq:
        s = os.stat(path)
        if stat.S_ISDIR(s.st_mode) and os.path.basename(path) != ".svn":
            sys.stdout.write(
                "directory " + repr(os.path.basename(path)) + "\n")
            full_path_list = []
            for element in os.listdir(path):
                full_path_list.append(os.path.join(path, element))
            contents(full_path_list)
        else:
            if os.path.basename(path) != ".svn":
                sys.stdout.write(
                    "file " + repr(os.path.basename(path)) + "\n")
    return result


if len(sys.argv) != 3:
    sys.stdout.write("The program takes 2 arguments:\n")
    sys.stdout.write("      1. a directory input path.\n")
    sys.stdout.write("      2. a directory output path. \n")
    sys.exit()

dir_path_in = sys.argv[1]
dir_path_in = os.path.abspath(dir_path_in)
dir_path_out = sys.argv[2]
dir_path_out = os.path.abspath(dir_path_out)

full_path_list = []
for element in os.listdir(dir_path_in):
    full_path_list.append(os.path.join(dir_path_in, element))

contents(full_path_list)

shutil.copytree(dir_path_in, dir_path_out)
