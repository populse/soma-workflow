# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 13:58:21 2013

@author: edouard.duchesnay@cea.fr
@author: benoit.da_mota@inria.fr
"""

from __future__ import absolute_import
from soma_workflow.test.workflow_tests.workflow_examples.workflow_examples import WorkflowExamples
from soma_workflow.test.workflow_tests.workflow_examples.workflow_local import WorkflowExamplesLocal
from soma_workflow.test.workflow_tests.workflow_examples.workflow_shared_transfer import WorkflowExamplesSharedTransfer
from soma_workflow.test.workflow_tests.workflow_examples.workflow_shared import WorkflowExamplesShared
from soma_workflow.test.workflow_tests.workflow_examples.workflow_transfer import WorkflowExamplesTransfer
from soma_workflow.test.workflow_tests.workflow_test import WorkflowTest

__all__ = ['WorkflowExamples',
           'WorkflowExamplesLocal',
           'WorkflowExamplesSharedTransfer',
           'WorkflowExamplesShared',
           'WorkflowExamplesTransfer',
           'WorkflowTest']
