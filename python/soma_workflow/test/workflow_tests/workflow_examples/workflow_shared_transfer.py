# -*- coding: utf-8 -*-
from __future__ import with_statement
from __future__ import absolute_import

# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 14:58:43 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
"""

import os

from soma_workflow.client import Job
from soma_workflow.client import SharedResourcePath
from soma_workflow.client import FileTransfer
from soma_workflow.test.workflow_tests import WorkflowExamples
from soma_workflow.test.workflow_tests.workflow_examples.workflow_local \
    import WorkflowExamplesLocal


class WorkflowExamplesSharedTransfer(WorkflowExamplesLocal):

    def transfer_function(self, dirname, filename, namespace, uuid,
                          disposal_timeout, is_input, client_paths=None):
        ''' use FileTransfer
        '''
        return FileTransfer(
            is_input,
            os.path.join(dirname, filename),
            disposal_timeout,
            uuid, client_paths)

    def shared_function(self, dirname, filename, namespace, uuid,
                        disposal_timeout, is_input, client_paths=None):
        ''' use SharedResourcePath
        '''
        return SharedResourcePath(filename, namespace, uuid, disposal_timeout)
