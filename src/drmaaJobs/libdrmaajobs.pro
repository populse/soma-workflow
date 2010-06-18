#!include ../../config-cpp-lib

TEMPLATE= lib
TARGET  = somadrmaajobs${BUILDMODEEXT}
INCBDIR = soma/pipeline

HEADERS = \
  drmaajobs.h \
  drmaa/drmaa.h

SOURCES = \
  drmaajobs.cc
