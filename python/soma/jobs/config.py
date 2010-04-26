from __future__ import with_statement
import ConfigParser

jobs_cfg = ConfigParser.ConfigParser()


jobs_cfg.add_section('neurospin_test_cluster')
#Job local process
jobs_cfg.set('neurospin_test_cluster', 'job_processes_logging_format', '%(asctime)s => %(module)s line %(lineno)s: %(message)s')
jobs_cfg.set('neurospin_test_cluster', 'job_processes_logging_level',  'DEBUG')
jobs_cfg.set('neurospin_test_cluster', 'job_processes_log_dir_path',   '/neurospin/tmp/Soizic/jobFiles/')
jobs_cfg.set('neurospin_test_cluster', 'src_local_process',            '/neurospin/tmp/Soizic/jobFiles/srcServers/localJobProcess.py')
#Job server
jobs_cfg.set('neurospin_test_cluster', 'job_server_logging_format',    "%(asctime)s => line %(lineno)s: %(message)s")
jobs_cfg.set('neurospin_test_cluster', 'job_server_logging_level',     'DEBUG')
jobs_cfg.set('neurospin_test_cluster', 'job_server_log_file',          '/volatile/laguitton/log_jobServer')
jobs_cfg.set('neurospin_test_cluster', 'name_server_host',             'is143016')
jobs_cfg.set('neurospin_test_cluster', 'job_server_name',              'JobServer')
jobs_cfg.set('neurospin_test_cluster', 'tmp_file_dir_path',            '/neurospin/tmp/Soizic/jobFiles/')
jobs_cfg.set('neurospin_test_cluster', 'database_file',                '/volatile/laguitton/jobs.db')
#DRMS
jobs_cfg.set('neurospin_test_cluster', 'drms',                         'SGE')
jobs_cfg.set('neurospin_test_cluster', 'submitting_machines',          "['is143016', 'is204723']")




jobs_cfg.add_section('soizic_home_cluster')
#Job local process
jobs_cfg.set('soizic_home_cluster', 'job_processes_logging_format', '%(asctime)s => %(module)s line %(lineno)s: %(message)s')
jobs_cfg.set('soizic_home_cluster', 'job_processes_logging_level',  'DEBUG')
jobs_cfg.set('soizic_home_cluster', 'job_processes_log_dir_path',   '/home/soizic/jobFiles/')
jobs_cfg.set('soizic_home_cluster', 'src_local_process',            '/home/soizic/projets/jobsdev/python/soma/jobs/localJobProcess.py')
#Job server
jobs_cfg.set('soizic_home_cluster', 'job_server_logging_format',    "%(asctime)s => line %(lineno)s: %(message)s")
jobs_cfg.set('soizic_home_cluster', 'job_server_logging_level',     'DEBUG')
jobs_cfg.set('soizic_home_cluster', 'job_server_log_file',          '/home/soizic/log_jobServer')
jobs_cfg.set('soizic_home_cluster', 'name_server_host',             'None')
jobs_cfg.set('soizic_home_cluster', 'job_server_name',              'JobServer')
jobs_cfg.set('soizic_home_cluster', 'tmp_file_dir_path',            '/home/soizic/jobFiles/')
jobs_cfg.set('soizic_home_cluster', 'database_file',                '/home/soizic/jobs.db')
#DRMS
jobs_cfg.set('soizic_home_cluster', 'drms',                         'SGE')
jobs_cfg.set('soizic_home_cluster', 'submitting_machines',          "['soizic-vaio']")

with open('jobs.cfg', 'wb') as configfile:
    jobs_cfg.write(configfile)

job_client_cfg = ConfigParser.ConfigParser()

