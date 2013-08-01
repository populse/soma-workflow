import os
import sys

from soma.workflow.test import test_workflow
from soma.workflow.client import Helper

if __name__ == '__main__':

  directory = "/tmp/"
  
  simple_wf_examples = test_workflow.WorkflowExamples(
                                             with_transfers=False,
                                             with_shared_resource_path=False)
  tr_wf_examples = test_workflow.WorkflowExamples(
                                             with_transfers=True,
                                             with_shared_resource_path=False)
  srp_wf_examples = test_workflow.WorkflowExamples(
                                             with_transfers=False,
                                             with_shared_resource_path=True)
  workflows = []
  workflows.append(("multiple", simple_wf_examples.multiple_simple_example()))
  workflows.append(("special_command", simple_wf_examples.special_command()))
  workflows.append(("mutiple_transfer", tr_wf_examples.multiple_simple_example()))
  workflows.append(("special_command_transfer", tr_wf_examples.special_command()))
  workflows.append(("special_transfer", tr_wf_examples.special_transfer()))
  workflows.append(("mutiple_crp", srp_wf_examples.multiple_simple_example()))
  workflows.append(("special_command_crp", srp_wf_examples.special_command()))
    
  ret_value = 0

  for workflow_name, workflow in workflows:
    print "--------------------------------------------------------------"
    print workflow_name

    file_path = os.path.join(directory, "json_" + workflow_name + ".wf")
    Helper.serialize(file_path, workflow)      

    new_workflow = Helper.unserialize(file_path)

    if not new_workflow.attributs_equal(workflow):
      print "FAILED !!"
      ret_value = 1
    else:
      print "OK"
    
    try:
      os.remove(file_path)
    except IOError:
      pass  

  if ret_value == 0:
    print "\nAll test ran with success."
  else:
    print "\nOne or several tests failed."

  sys.exit(ret_value)
