from PyQt4 import QtCore, QtGui
import sys
from soma.jobs.gui.workflowGui import WorkflowWidget, WorkflowItemModel
from WorkflowExample import *
from modeltest import ModelTest

    
if __name__=="__main__":
  
  app = QtGui.QApplication(sys.argv)
  
  workflowControler = TestWorkflow() 
  
  workflowWidget = WorkflowWidget(workflowControler)
  workflowWidget.show()
  

  #workflowWidget.setWorkflow(TestWorkflow.workflow, TestWorkflow.jobs)
  #TestWorkflow.workflowSubmission()
  #workflowWidget.setWorkflow(TestWorkflow.submitted_workflow, TestWorkflow.jobs)
  #TestWorkflow.transferInputFiles()
  #model = WorkflowItemModel(TestWorkflow.workflow, TestWorkflow.jobs, workflowWidget)
  #test = ModelTest(model, workflowWidget)
  
  app.exec_()
    