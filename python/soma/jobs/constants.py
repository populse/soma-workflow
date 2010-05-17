

'''
Job status:
'''
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
JOB_STATUS = [UNDETERMINED, 
              QUEUED_ACTIVE,
              SYSTEM_ON_HOLD,
              USER_ON_HOLD,
              USER_SYSTEM_ON_HOLD,
              RUNNING,
              SYSTEM_SUSPENDED,
              USER_SUSPENDED,
              USER_SYSTEM_SUSPENDED,
              DONE,
              FAILED]

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
Parallel job configuration names:
'''
MPI="MPI"
OPEN_MP="OpenMP"
PARALLEL_CONFIGURATIONS = [MPI, OPEN_MP]