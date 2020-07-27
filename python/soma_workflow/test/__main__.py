# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import

import sys
#import pdb
import signal
import code

def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    import traceback
    traceback.print_stack()

    #d={'_frame':frame}         # Allow access to frame object.
    #d.update(frame.f_globals)  # Unless shadowed by global
    #d.update(frame.f_locals)

    #i = code.InteractiveConsole(d)
    #message  = "Signal received : entering python shell.\nTraceback:\n"
    #message += ''.join(traceback.format_stack(frame))
    #i.interact(message)

    #pdb.set_trace()

if not sys.platform.startswith('win'):
    signal.signal(signal.SIGUSR1, debug)

res = True

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

import soma_workflow.test.workflow_tests.test_barrier_jobs
res &= soma_workflow.test.workflow_tests.test_barrier_jobs.test()

import soma_workflow.test.workflow_tests.test_workflow_env
res &= soma_workflow.test.workflow_tests.test_workflow_env.test()

# this test needs to be called *after* test_special_command because it triggers
# a warning and, for an unknown reason, later warnings do not fire any longer
# later on, even after calling warnings.resetwarnings()
# and warnings.simplefilter('always').
# It seems that another solution is to set warnings.simplefilter('always')
# first, then run test_special_command - then warnings will always fire
# whatever later settings for warnings. Something is broken there.
import soma_workflow.test.test_serialization
res &= soma_workflow.test.test_serialization.test()

if res:
    print('All tests OK')
    sys.exit(0)
else:
    print('Tests failed.')
    sys.exit(1)
