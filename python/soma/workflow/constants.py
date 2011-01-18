'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

'''
Job status:
'''
NOT_SUBMITTED="not_submitted"
UNDETERMINED="undetermined"
QUEUED_ACTIVE="queued_active"
SYSTEM_ON_HOLD="system_on_hold"
USER_ON_HOLD="user_on_hold"
USER_SYSTEM_ON_HOLD="user_system_on_hold"
RUNNING="running"
SYSTEM_SUSPENDED="system_suspended"
USER_SUSPENDED="user_suspended"
USER_SYSTEM_SUSPENDED="user_system_suspended"
DONE="done"
FAILED="failed"
DELETE_PENDING="delete_pending"
KILL_PENDING="kill_pending"
SUBMISSION_PENDING="submission_pending"
WARNING="warning"
JOB_STATUS = [NOT_SUBMITTED,
              UNDETERMINED, 
              QUEUED_ACTIVE,
              SYSTEM_ON_HOLD,
              USER_ON_HOLD,
              USER_SYSTEM_ON_HOLD,
              RUNNING,
              SYSTEM_SUSPENDED,
              USER_SUSPENDED,
              USER_SYSTEM_SUSPENDED,
              DONE,
              FAILED,
              DELETE_PENDING,
              KILL_PENDING,
              SUBMISSION_PENDING,
              WARNING]

'''
Exit job status:
'''
EXIT_UNDETERMINED="exit_status_undetermined"
EXIT_ABORTED="aborted"
FINISHED_REGULARLY="finished_regularly"
FINISHED_TERM_SIG="finished_signal"
FINISHED_UNCLEAR_CONDITIONS="finished_unclear_condition"
USER_KILLED="killed_by_user"
JOB_EXIT_STATUS= [EXIT_UNDETERMINED,
                  EXIT_ABORTED,
                  FINISHED_REGULARLY,
                  FINISHED_TERM_SIG,
                  FINISHED_UNCLEAR_CONDITIONS,
                  USER_KILLED]


'''
File transfer status:
'''
FILES_DONT_EXIST = "don't exist"
FILES_ON_CLIENT = "on client side"
FILES_ON_CR = "on computing resource side"
FILES_ON_CLIENT_AND_CR = "on both sides"
TRANSFERING_FROM_CLIENT_TO_CR = "transfering client->cr"
TRANSFERING_FROM_CR_TO_CLIENT = "transfering cr->client"
FILES_UNDER_EDITION = "under edition"
FILE_TRANSFER_STATUS__ = [FILES_DONT_EXIST,
                          FILES_ON_CLIENT,
                          FILES_ON_CR,
                          FILES_ON_CLIENT_AND_CR,
                          TRANSFERING_FROM_CLIENT_TO_CR,
                          TRANSFERING_FROM_CR_TO_CLIENT,
                          FILES_UNDER_EDITION]


'''
Transfer type
'''
FILE_SENDING = "file_sending"
FILE_RETRIEVING = "file_retrieving"
DIR_SENDING = "directory_sending"
DIR_RETRIEVING = "directory_retrieving"
TRANSFER_TYPE = [FILE_SENDING, 
                 FILE_RETRIEVING, 
                 DIR_SENDING,
                 FILE_SENDING]

'''
Workflow status:
'''
WORKFLOW_NOT_STARTED = "worklflow_not_started"
WORKFLOW_IN_PROGRESS = "workflow_in_progress"
WORKFLOW_DONE = "workflow_done"
WORKFLOW_STATUS = [ WORKFLOW_NOT_STARTED, 
                    WORKFLOW_IN_PROGRESS, 
                    WORKFLOW_DONE,
                    DELETE_PENDING,
                    WARNING]


'''
Soma job configuration items
CFG => Mandatory items
OCFG => Optional
'''
CFG_CLUSTER_ADDRESS = 'cluster_address'
CFG_SUBMITTING_MACHINES = 'submitting_machines'
OCFG_DRMAA_IMPLEMENTATION = 'drmaa_implementation'

#queues
#OCFG_QUEUES is a list of queue name separated by white spaces.
#ex: "queue1 queue2"
#OCFG_MAX_JOB_IN_QUEUE allow to specify a maximum number of job N which can be
#in the queue for one user. The engine won't submit more than N job at once. The 
#also wait for the job to leave the queue before submitting new jobs.
#syntax: "{default_queue_max_nb_jobs} queue1{max_nb_jobs1} queue2{max_nb_job2}"
OCFG_QUEUES = 'queues'
OCFG_MAX_JOB_IN_QUEUE = 'max_jobs_in_queue' 

#database server

CFG_DATABASE_FILE = 'database_file'
CFG_TRANSFERED_FILES_DIR = 'transfered_files_dir_path'
CFG_SERVER_NAME = 'server_name'
CFG_NAME_SERVER_HOST ='name_server_host'

OCFG_SERVER_LOG_FILE = 'server_log_file'
OCFG_SERVER_LOG_LEVEL = 'server_logging_level'
OCFG_SERVER_LOG_FORMAT = 'server_logging_format'

#Engine

CFG_START_ENGINE_SCRIPT = 'start_engine_script'

OCFG_ENGINE_LOG_DIR = 'engine_log_dir_path'
OCFG_ENGINE_LOG_LEVEL = 'engine_logging_level'
OCFG_ENGINE_LOG_FORMAT = 'engine_logging_format'

#Shared resource path translation files 
#specify the translation files (if any) associated to a namespace
#eg. translation_files = brainvisa{/home/toto/.brainvisa/translation.sjtr} namespace2{path/translation1.sjtr} namespace2{path/translation2.sjtr}
OCFG_PATH_TRANSLATION_FILES = 'path_translation_files' 

# Parallel job configuration :
# DRMAA attributes used in parallel job submission (their value depends on the cluster and DRMS) 
OCFG_PARALLEL_COMMAND = "drmaa_native_specification"
OCFG_PARALLEL_JOB_CATEGORY = "drmaa_job_category"
PARALLEL_DRMAA_ATTRIBUTES = [OCFG_PARALLEL_COMMAND, OCFG_PARALLEL_JOB_CATEGORY]
# kinds of parallel jobs (items can be added by administrator)
OCFG_PARALLEL_PC_MPI="MPI"
OCFG_PARALLEL_PC_OPEN_MP="OpenMP"
PARALLEL_CONFIGURATIONS = [OCFG_PARALLEL_PC_MPI, OCFG_PARALLEL_PC_OPEN_MP]
# parallel job environment variables for the execution machine (items can be added by administrators) 
OCFG_PARALLEL_ENV_MPI_BIN = 'SOMA_JOB_MPI_BIN'
OCFG_PARALLEL_ENV_NODE_FILE = 'SOMA_JOB_NODE_FILE'
PARALLEL_JOB_ENV = [OCFG_PARALLEL_ENV_MPI_BIN, OCFG_PARALLEL_ENV_NODE_FILE]





