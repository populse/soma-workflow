import sys
import os
import stat

def contents(path_seq):
  result = []
  for path in path_seq:
    s = os.stat(path)
    if stat.S_ISDIR(s.st_mode) and os.path.basename(path) != ".svn":
      sys.stdout.write("directory " + repr(os.path.basename(path)) + " \n")
      full_path_list = []
      for element in os.listdir(path):
        full_path_list.append(os.path.join(path, element))
      contents(full_path_list)
    else:
      if os.path.basename(path) != ".svn":
        sys.stdout.write("file " + repr(os.path.basename(path))+ " \n")
  return result


if len(sys.argv) != 2:
  sys.stdout.write("The program take 1 argument: a directory path.")
  
dir_path = sys.argv[1]
dir_path = os.path.abspath(dir_path)

sys.stdout.write("directory path: " + repr(dir_path) + "\n")
sys.stdout.write(" \n")


full_path_list = []
for element in os.listdir(dir_path):
  full_path_list.append(os.path.join(dir_path, element))

contents(full_path_list)