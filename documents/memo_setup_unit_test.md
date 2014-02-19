Unit Tests
----------

If a new feature has been added to soma_workflow, please run all the workflow unit tests. All unit tests have been put in:

```
soma-workflow/python/soma_workflow/test/workflow_tests
```

In order to run unit tests on a cluster, for example gabriel, you need to setup a configuration file on local PC and on remote cluster:

An configuration example (~/.soma-workflow.cfg) on local PC is shown as below:

```
[jl123456@gabriel]
cluster_address = gabriel
submitting_machines = gabriel
queues = Exception Global_short run32 LowPrio Cati_run4 Cati_run32 run4 Global_long run2 Cati_long default run16 Cati_run2 Cati_run8 Cati_LowPrio Cati_run16 run8 Cati_short test 
login = jl123456
```

where "jl123456" is ssh login and "gabriel" is the hostname of cluster. An similar configuration example (~/.soma-workflow.cfg) on cluster is shown as below:

```
[jl123456@gabriel]
database_file = /home/jl123456/.soma-workflow/soma_workflow.db
transfered_files_dir = /home/jl123456/.soma-workflow/transfered-files
name_server_host = gabriel
server_name = soma_workflow_database_jl123456
server_log_file = /home/jl123456/.soma-workflow/logs/log_server
server_log_format = %(asctime)s => line %(lineno)s: %(message)s
server_log_level = INFO
engine_log_dir = /home/jl123456/.soma-workflow/logs
engine_log_format = %(asctime)s => %(module)s line %(lineno)s: %(message)s                 %(threadName)s
engine_log_level = INFO
max_job_in_queue = {15} Exception{15} Global_short{15} Global_long{15}
PATH_TRANSLATION_FILES = example{/home/jl123456/swf_translation}
```

And then you can run a unit test for example:

```
$ cd soma-workflow/python/soma_workflow/test/workflow_tests
$ python test_njobs_with_dependencies.py
```
