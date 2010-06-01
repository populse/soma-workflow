TEMPLATE = sip
TARGET  = somadrmaajobssip

LIBBDIR = python/soma/pipeline/

#!include ../../config-sip

SIPS = jobs.sip
LIBS += ${SOMA_JOBS_CPP_LIBS}
