
'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


###########################
# Soma-workflow constants #
###########################

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
FILES_DO_NOT_EXIST = "do not exist"
FILES_ON_CLIENT = "on client side"
FILES_ON_CR = "on computing resource side"
FILES_ON_CLIENT_AND_CR = "on both sides"
TRANSFERING_FROM_CLIENT_TO_CR = "transfering client->cr"
TRANSFERING_FROM_CR_TO_CLIENT = "transfering cr->client"
FILES_UNDER_EDITION = "under edition"
FILE_TRANSFER_STATUS = [FILES_DO_NOT_EXIST,
                        FILES_ON_CLIENT,
                        FILES_ON_CR,
                        FILES_ON_CLIENT_AND_CR,
                        TRANSFERING_FROM_CLIENT_TO_CR,
                        TRANSFERING_FROM_CR_TO_CLIENT,
                        FILES_UNDER_EDITION]


'''
Transfer type
'''
TR_FILE_C_TO_CR = "file transfer form client to cr"
TR_DIR_C_TO_CR = "dir transfer from client to cr"
TR_MFF_C_TO_CR = "multi file format from client to cr"
TR_FILE_CR_TO_C = "file transfer form cr to client"
TR_DIR_CR_TO_C = "dir transfer from cr to client"
TR_MFF_CR_TO_C = "multi file format from cr to client"
TRANSFER_TYPES = [TR_FILE_C_TO_CR,
                  TR_DIR_C_TO_CR,
                  TR_MFF_C_TO_CR,
                  TR_FILE_CR_TO_C,
                  TR_DIR_CR_TO_C,
                  TR_MFF_CR_TO_C]

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




