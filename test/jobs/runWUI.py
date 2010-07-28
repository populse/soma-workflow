from PyQt4 import QtCore, QtGui
import sys
from soma.jobs.gui.workflowGui import WorkflowWidget
from WorkflowExample import *


    
if __name__=="__main__":
  
  app = QtGui.QApplication(sys.argv)
  
  workflowWidget = WorkflowWidget()
  workflowWidget.show()
  
  TestWorkflow = TestWorkflow()
  #workflowWidget.setWorkflow(TestWorkflow.workflow)
  
  TestWorkflow.workflowSubmission()
  workflowWidget.setWorkflow(TestWorkflow.submitted_workflow, TestWorkflow.jobs)
  
  app.exec_()
    