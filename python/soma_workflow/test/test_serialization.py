'''
author: laure.hugo@cea.fr
author: Soizic Laguitton
organization: IFR 49<http://www.ifr49.org
license: CeCILL B http://www.cecill.info/licences/Licence_CeCILL_V2-en.html
'''
from __future__ import print_function

import os
import tempfile

from soma_workflow.test.workflow_tests.workflow_examples import workflow_local
from soma_workflow.test.workflow_tests.workflow_examples import workflow_shared
from soma_workflow.test.workflow_tests.workflow_examples \
    import workflow_transfer
from soma_workflow.client import Helper
import unittest
import shutil


class SerializationTest(unittest.TestCase):

    def setUp(self):
        self.temporaries = []

    def tearDown(self):
        for t in self.temporaries:
            if os.path.isdir(t):
                try:
                    shutil.rmtree(t)
                except:
                    pass
            elif os.path.exists(t):
                try:
                    os.unkink(t)
                except:
                    pass

    def test_serialization(self):
        simple_wf_examples = workflow_local.WorkflowExamplesLocal()
        tr_wf_examples = workflow_transfer.WorkflowExamplesTransfer()
        srp_wf_examples = workflow_shared.WorkflowExamplesShared()
        self.temporaries += [simple_wf_examples.output_dir,
                             tr_wf_examples.output_dir,
                             srp_wf_examples.output_dir]
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
            print("Testing", workflow_name)

            file_path = tempfile.mkstemp(prefix="json_",
                                         suffix=workflow_name + ".wf")
            os.close(file_path[0])
            file_path = file_path[1]
            Helper.serialize(file_path, workflow)

            new_workflow = Helper.unserialize(file_path)

            self.assertTrue(new_workflow.attributs_equal(workflow),
                            "Serialization failed for workflow %s" %
                            workflow_name)

            try:
                os.remove(file_path)
            except IOError:
                pass


def test():
    suite = unittest.TestLoader().loadTestsFromTestCase(SerializationTest)
    runtime = unittest.TextTestRunner(verbosity=2).run(suite)
    return runtime.wasSuccessful()

if __name__ == '__main__':
    unittest.main()
