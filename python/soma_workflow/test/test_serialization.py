import os
import sys

from soma_workflow.test import workflow_local
from soma_workflow.test import workflow_shared
from soma_workflow.test import workflow_transfer
from soma_workflow.client import Helper

if __name__ == '__main__':

    directory = "/tmp/"

    simple_wf_examples = workflow_local.WorkflowExamplesLocal()
    tr_wf_examples = workflow_transfer.WorkflowExamplesTransfer()
    srp_wf_examples = workflow_shared.WorkflowExamplesShared()
    workflows = []
    workflows.append(("multiple", simple_wf_examples.example_multiple()))
    workflows.append(("special_command",
                      simple_wf_examples.example_special_command()))

    workflows.append(("mutiple_transfer", tr_wf_examples.example_multiple()))
    workflows.append(("special_command_transfer",
                      tr_wf_examples.example_special_command()))
    workflows.append(("special_transfer",
                      tr_wf_examples.example_special_transfer()))

    workflows.append(("mutiple_srp", srp_wf_examples.example_multiple()))
    workflows.append(("special_command_srp",
                      srp_wf_examples.example_special_command()))

    failed_workflows = []

    for workflow_name, workflow in workflows:
        print "--------------------------------------------------------------"
        print workflow_name

        file_path = os.path.join(directory, "json_" + workflow_name + ".wf")
        Helper.serialize(file_path, workflow)

        new_workflow = Helper.unserialize(file_path)

        if not new_workflow.attributs_equal(workflow):
            print "FAILED !!"
            failed_workflows.append(workflow_name)
        else:
            print "OK"

        try:
            os.remove(file_path)
        except IOError:
            pass

    if len(failed_workflows) == 0:
        print "\nWorkflow serialization successful"
    else:
        print "Failed tests :"
        for failed_workflow in failed_workflows:
            print "*", failed_workflow

    sys.exit(0)
