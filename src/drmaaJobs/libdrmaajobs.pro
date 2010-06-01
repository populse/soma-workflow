#!include ../../config-cpp-lib

TEMPLATE= lib
TARGET  = somadrmaajobs${BUILDMODEEXT}

HEADERS = \
  drmaajobs.h \
  drmaa/drmaa.h

SOURCES = \
  drmaajobs.cc
