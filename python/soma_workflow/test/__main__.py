
import sys

import soma_workflow.test.job_tests.test_workflow_api
res = soma_workflow.test.job_tests.test_workflow_api.test()

import soma_workflow.test.workflow_tests.test_exception1
res &= soma_workflow.test.workflow_tests.test_exception1.test()

import soma_workflow.test.workflow_tests.test_exception2
res &= soma_workflow.test.workflow_tests.test_exception2.test()

import soma_workflow.test.workflow_tests.test_fake_pipeline
import soma_workflow.test.workflow_tests.test_multiple
import soma_workflow.test.workflow_tests.test_native_spec
import soma_workflow.test.workflow_tests.test_njobs
import soma_workflow.test.workflow_tests.test_njobs_with_dependencies
import soma_workflow.test.workflow_tests.test_serial_jobs
import soma_workflow.test.workflow_tests.test_simple
import soma_workflow.test.workflow_tests.test_special_command
import soma_workflow.test.workflow_tests.test_special_transfer
import soma_workflow.test.workflow_tests.test_wrong_native_spec

if res:
    print('All tests OK')
    sys.exit(0)
else:
    print('Tests failed.')
    sys.exit(1)
