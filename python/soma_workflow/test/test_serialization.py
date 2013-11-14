'''
@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
import os

from soma_workflow.test import workflow_local
from soma_workflow.test import workflow_shared
from soma_workflow.test import workflow_transfer
from soma_workflow.client import Helper
import unittest


class SerializationTest(unittest.TestCase):

    def test_serialization(self):
        directory = "/tmp/"

        simple_wf_examples = workflow_local.WorkflowExamplesLocal()
        tr_wf_examples = workflow_transfer.WorkflowExamplesTransfer()
        srp_wf_examples = workflow_shared.WorkflowExamplesShared()
        workflows = []
        workflows.append(("multiple", simple_wf_examples.example_multiple()))
        workflows.append(("special_command",
                          simple_wf_examples.example_special_command()))

        workflows.append(("mutiple_transfer",
                          tr_wf_examples.example_multiple()))
        workflows.append(("special_command_transfer",
                          tr_wf_examples.example_special_command()))
        workflows.append(("special_transfer",
                          tr_wf_examples.example_special_transfer()))

        workflows.append(("mutiple_srp", srp_wf_examples.example_multiple()))
        workflows.append(("special_command_srp",
                          srp_wf_examples.example_special_command()))

        for workflow_name, workflow in workflows:
            print "Testing", workflow_name

            file_path = os.path.join(directory,
                                     "json_" + workflow_name + ".wf")
            Helper.serialize(file_path, workflow)

            new_workflow = Helper.unserialize(file_path)

            self.assertTrue(new_workflow.attributs_equal(workflow),
                            "Serialization failed for workflow %s" %
                            workflow_name)

            try:
                os.remove(file_path)
            except IOError:
                pass


if __name__ == '__main__':
    unittest.main()
