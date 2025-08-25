import sys
import os
import stat
import shutil

'''
Takes 1 parameter: the path of an input file with the format .img
                   the path of an output file with the format .img
The program checks if the file .hdr exists for the input and copy the input file
at the location output path (both .img and .hdr files).
'''

if len(sys.argv) != 3:
    sys.stdout.write("The program takes 2 arguments\n")
    sys.stdout.write("      1. an input file path with format .img.\n")
    sys.stdout.write("         (the associated file .hdr must exist) ")
    sys.stdout.write("      2. an output file path with format .img. \n")
    sys.exit()


input_path = sys.argv[1]
input_path = os.path.abspath(input_path)
output_path = sys.argv[2]
output_path = os.path.abspath(output_path)

input_path_hdr = os.path.splitext(input_path)[0] + ".hdr"
if os.path.isfile(input_path_hdr):
    sys.stdout.write("Ok, the file .hdr exists. \n")
else:
    sys.stdout.write("in file path: " + repr(input_path) + "\n")
    sys.stdout.write("out file path: " + repr(output_path) + "\n")
    sys.stdout.write(" \n")
    sys.stdout.write("The file " + input_path_hdr + " doesn't exist!!\n")
    sys.exit()

output_path_hdr = os.path.splitext(output_path)[0] + ".hdr"
shutil.copyfile(input_path, output_path)
shutil.copyfile(input_path_hdr, output_path_hdr)
sys.stdout.write("Copying .img and .hdr to output_path\n")
