#! /usr/bin/env python

'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


from __future__ import with_statement
import ConfigParser
from soma.jobs.constants import *

jobs_cfg = ConfigParser.ConfigParser()


##########################
# NEUROSPIN TEST CLUSTER #
##########################

s = 'neurospin_test_cluster'

jobs_cfg.add_section(s)

# parallel job specific submission information
jobs_cfg.set(s, OCFG_PARALLEL_COMMAND, "-pe  {config_name} {max_node}") 
jobs_cfg.set(s, OCFG_PARALLEL_PC_MPI,        'mpi')
jobs_cfg.set(s, OCFG_PARALLEL_ENV_MPI_BIN,        '/volatile/laguitton/sge6-2u5/mpich/mpich-1.2.7/bin/')
jobs_cfg.set(s, OCFG_PARALLEL_ENV_NODE_FILE,      '$TMPDIR/machines')
#Job local process
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_FORMAT, '%(asctime)s => %(module)s line %(lineno)s : %(message)s      %(threadName)s')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_DIR,    '/neurospin/tmp/Soizic/jobFiles/')
jobs_cfg.set(s, CFG_SRC_LOCAL_PROCESS,           '/neurospin/tmp/Soizic/jobFiles/srcServers/localJobProcess.py')
#Job server
jobs_cfg.set(s, CFG_SUBMITTING_MACHINES,    "is143016")# is204723")
jobs_cfg.set(s, CFG_CLUSTER_ADDRESS,        "is143016")
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FORMAT, "%(asctime)s => line %(lineno)s: %(message)s")
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FILE,   '/volatile/laguitton/log_jobServer')
jobs_cfg.set(s, CFG_NAME_SERVER_HOST,       'is143016')
jobs_cfg.set(s, CFG_JOB_SERVER_NAME,        'JobServer')
jobs_cfg.set(s, CFG_TMP_FILE_DIR_PATH,      '/neurospin/tmp/Soizic/jobFiles/')
jobs_cfg.set(s, CFG_DATABASE_FILE,          '/volatile/laguitton/jobs.db')
#DRMS 
jobs_cfg.set(s, OCFG_DRMS,               'SGE') 
#Universal path translation file
jobs_cfg.set(s, OCFG_U_PATH_TRANSLATION_FILES, 'example{/neurospin/tmp/Soizic/jobFiles/translation_files/job_examples.sjtr} brainvisa{/home/sl225510/.brainvisa/soma-workflow.translation}')

##########################
# SOIZIC HOME CLUSTER #
##########################

s = 'soizic_home_cluster'

jobs_cfg.add_section(s)
#Job local process
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_FORMAT, '%(asctime)s => %(module)s line %(lineno)s: %(message)s          %(threadName)s')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_LEVEL, 'DEBUG')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_DIR,   '/home/soizic/soma-jobs-server/logs/')
jobs_cfg.set(s, CFG_SRC_LOCAL_PROCESS,  '/home/soizic/soma-jobs-test/python/soma/jobs/localJobProcess.py')
#Job server
jobs_cfg.set(s, CFG_SUBMITTING_MACHINES,    "soizic-vaio")
jobs_cfg.set(s, CFG_CLUSTER_ADDRESS,        "soizic-vaio")
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FORMAT, "%(asctime)s => line %(lineno)s: %(message)s")
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FILE,   '/home/soizic/soma-jobs-server/log_jobServer')
jobs_cfg.set(s, CFG_NAME_SERVER_HOST,       'None')
jobs_cfg.set(s, CFG_JOB_SERVER_NAME,        'JobServer')
jobs_cfg.set(s, CFG_TMP_FILE_DIR_PATH,      '/home/soizic/soma-jobs-server/jobFiles/')
jobs_cfg.set(s, CFG_DATABASE_FILE,          '/home/soizic/soma-jobs-server/jobs.db')
#DRMS
jobs_cfg.set(s, OCFG_DRMS,               'SGE')



###############
# DSV CLUSTER #
###############

s = 'DSV_cluster'

jobs_cfg.add_section(s)
# parallel job specific submission information
jobs_cfg.set(s, OCFG_PARALLEL_COMMAND,   "-l nodes={max_node}") 
jobs_cfg.set(s, OCFG_PARALLEL_PC_MPI,  'mpi')
jobs_cfg.set(s, OCFG_PARALLEL_ENV_MPI_BIN,   '/opt/mpich/gnu/bin/')
jobs_cfg.set(s, OCFG_PARALLEL_ENV_NODE_FILE, '$PBS_NODEFILE')
#Job local process
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_FORMAT, '%(asctime)s => %(module)s line %(lineno)s: %(message)s                 %(threadName)s')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_DIR,    '/home/sl225510/soma-jobs-server/logs/')
jobs_cfg.set(s, CFG_SRC_LOCAL_PROCESS,           '/home/sl225510/soma-jobs-server/localJobProcess.py')
#Job server
jobs_cfg.set(s, CFG_SUBMITTING_MACHINES,    'gabriel.intra.cea.fr')
jobs_cfg.set(s, CFG_CLUSTER_ADDRESS,        'gabriel.intra.cea.fr')
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FORMAT, "%(asctime)s => line %(lineno)s: %(message)s")
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FILE,   '/home/sl225510/soma-jobs-server/logs/log_jobServer')
jobs_cfg.set(s, CFG_NAME_SERVER_HOST,       'gabriel.intra.cea.fr')
jobs_cfg.set(s, CFG_JOB_SERVER_NAME,        'JobServer')
jobs_cfg.set(s, CFG_TMP_FILE_DIR_PATH,      '/home/sl225510/soma-jobs-server/jobFiles/')
jobs_cfg.set(s, CFG_DATABASE_FILE,          '/home/sl225510/soma-jobs-server/jobs.db')
#DRMS 
jobs_cfg.set(s, OCFG_DRMS,               'PBS') 




#################
# HIPIP CLUSTER #
#################

s = 'HiPiP_cluster'

jobs_cfg.add_section(s)
#Job local process
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_FORMAT, '%(asctime)s => %(module)s line %(lineno)s: %(message)s                 %(threadName)s')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_LOCAL_PROCESSES_LOG_DIR,    '/home/cea/soma-jobs-server/logs/')
jobs_cfg.set(s, CFG_SRC_LOCAL_PROCESS,           '/home/cea/brainvisa/python/soma/jobs/localJobProcess.py')
#Job server
jobs_cfg.set(s, CFG_SUBMITTING_MACHINES,    'hipip0')
jobs_cfg.set(s, CFG_CLUSTER_ADDRESS,        'hipipcluster')
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FORMAT, "%(asctime)s => line %(lineno)s: %(message)s")
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_JOB_SERVER_LOG_FILE,   '/home/cea/soma-jobs-server/logs/log_jobServer')
jobs_cfg.set(s, CFG_NAME_SERVER_HOST,       'hipip0')
jobs_cfg.set(s, CFG_JOB_SERVER_NAME,        'JobServer')
jobs_cfg.set(s, CFG_TMP_FILE_DIR_PATH,      '/home/cea/soma-jobs-server/jobFiles/')
jobs_cfg.set(s, CFG_DATABASE_FILE,          '/home/cea/soma-jobs-server/jobs.db')
#DRMS 
jobs_cfg.set(s, OCFG_DRMS,               'SGE') 
#Universal path translation file
jobs_cfg.set(s, OCFG_U_PATH_TRANSLATION_FILES, 'example{/home/cea/soma-jobs-server/translation_file_examples/job_examples.sjtr}  brainvisa{/home/cea/.brainvisa/soma-workflow.translation}')

########################
# client configuration #
########################

s = OCFG_SECTION_CLIENT

jobs_cfg.add_section(s)
# client log file
jobs_cfg.set(s, OCFG_CLIENT_LOG_FORMAT, '%(asctime)s => %(module)s line %(lineno)s: %(message)s')
jobs_cfg.set(s, OCFG_CLIENT_LOG_LEVEL,  'DEBUG')
jobs_cfg.set(s, OCFG_CLIENT_LOG_FILE,    '/home/sl225510/log_somajobsclient')


#####################################################"
with open('jobs.cfg', 'wb') as configfile:
    jobs_cfg.write(configfile)