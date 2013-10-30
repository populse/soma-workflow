# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 17:09:13 2013

@author: laure.hugo@cea.fr
"""


def identical_files(filepath1, filepath2):
    file1 = open(filepath1)
    file2 = open(filepath2)
    lineNb = 1
    line1 = file1.readline()
    line2 = file2.readline()
    identical = line1 == line2

    while identical and line1:
        line1 = file1.readline()
        line2 = file2.readline()
        lineNb = lineNb + 1
        identical = line1 == line2

    if identical:
        identical = line1 == line2
    if not identical:
        return (
            (False, "%s and %s are different. line %d: \n file1: %s file2:%s" %
             (filepath1, filepath2, lineNb, line1, line2))
        )
    else:
        return (True, None)


def check_files(files, files_models, tolerance=0):
    index = 0
    for file in files:
        t = tolerance
        (identical, msg) = identical_files(file, files_models[index])
        if not identical:
            if t <= 0:
                return (identical, msg)
            else:
                t = t - 1
                print "\n checkFiles: " + msg
        index = index + 1
    return (True, None)
