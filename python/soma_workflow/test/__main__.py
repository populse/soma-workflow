
import sys

import soma_workflow.test.test_serialization
res = soma_workflow.test.test_serialization.test()

import soma_workflow.test.job_tests.test_workflow_api
res &= soma_workflow.test.job_tests.test_workflow_api.test()

import soma_workflow.test.workflow_tests.test_exception1
res &= soma_workflow.test.workflow_tests.test_exception1.test()

import soma_workflow.test.workflow_tests.test_exception2
res &= soma_workflow.test.workflow_tests.test_exception2.test()

import soma_workflow.test.workflow_tests.test_fake_pipeline
res &= soma_workflow.test.workflow_tests.test_fake_pipeline.test()

import soma_workflow.test.workflow_tests.test_multiple
res &= soma_workflow.test.workflow_tests.test_multiple.test()

import soma_workflow.test.workflow_tests.test_native_spec
res &= soma_workflow.test.workflow_tests.test_native_spec.test()

import soma_workflow.test.workflow_tests.test_njobs
res &= soma_workflow.test.workflow_tests.test_njobs.test()

import soma_workflow.test.workflow_tests.test_njobs_with_dependencies
res &= soma_workflow.test.workflow_tests.test_njobs_with_dependencies.test()

import soma_workflow.test.workflow_tests.test_serial_jobs
res &= soma_workflow.test.workflow_tests.test_serial_jobs.test()

import soma_workflow.test.workflow_tests.test_simple
res &= soma_workflow.test.workflow_tests.test_simple.test()

import soma_workflow.test.workflow_tests.test_special_command
res &= soma_workflow.test.workflow_tests.test_special_command.test()

import soma_workflow.test.workflow_tests.test_special_transfer
res &= soma_workflow.test.workflow_tests.test_special_transfer.test()

import soma_workflow.test.workflow_tests.test_wrong_native_spec
res &= soma_workflow.test.workflow_tests.test_wrong_native_spec.test()

if res:
    print('All tests OK')
    sys.exit(0)
else:
    print('Tests failed.')
    sys.exit(1)
