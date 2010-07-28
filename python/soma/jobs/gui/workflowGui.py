from PyQt4 import QtGui, QtCore
from soma.jobs.jobClient import Workflow, Group, FileSending, FileRetrieving, FileTransfer, JobTemplate
from soma.jobs.constants import *
import time
import threading


class WorkflowWidget(QtGui.QWidget):
  
  def __init__(self, parent = None, flags = 0):
    super(WorkflowWidget, self).__init__(parent)
    
    self.setWindowTitle("Workflows !!")
   
    self.workflowTreeView = WorkflowTreeView(self)
    self.workflowGraphView = WorkflowGraphView(self)
    self.workflowElementInfo = WorkflowElementInfo(self)
  
  
    vlayout = QtGui.QVBoxLayout()
    vlayout.addWidget(self.workflowGraphView)
    vlayout.addWidget(self.workflowElementInfo)
    layout = QtGui.QHBoxLayout()
    layout.addWidget(self.workflowTreeView)
    layout.addLayout(vlayout)
    self.setLayout(layout)


    
    
  def setWorkflow(self, workflow, jobs):
    self.workflowItemModel = WorkflowItemModel(workflow, jobs, self)
    self.workflowTreeView.setModel(self.workflowItemModel)
    self.workflowTreeView.expandAll()
    
  
    
    
class WorkflowGraphView(QtGui.QGraphicsView):
  
  def __init__(self, parent = None):
    super(WorkflowGraphView, self).__init__(parent)
    self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    
      
class WorkflowItemModel(QtCore.QAbstractItemModel):
  
  def __init__(self, workflow, jobs = None, parent=None):
    super(WorkflowItemModel, self).__init__(parent)
    self.workflow = workflow 
    self.jobs = jobs
    
    def updateLoop(self, interval):
      while True:
        row = self.rowCount(QtCore.QModelIndex())
        self.dataChanged.emit(self.index(0,0,QtCore.QModelIndex()),
                              self.index(row,0, QtCore.QModelIndex()))
        time.sleep(interval)
    
    self.__update_loop = threading.Thread(name = "WorflowItemMode_update_loop",
                                         target = updateLoop,
                                         args = (self, 1))
    self.__update_loop.setDaemon(True)
    self.__update_loop.start()
    
    
    
  def index(self, row, column, parent=QtCore.QModelIndex()):
    #print " " 
    #print ">>> index " + repr(row) + " " + repr(column) 
    if row < 0 or not column == 0:
      #print "<<< index result QtCore.QModelIndex()"
      return QtCore.QModelIndex()
    
    if not parent.isValid():
      parentItem = self.workflow.mainGroup
    else:
      parentItem = parent.internalPointer()
    #print "    parent " + repr(parentItem.name)
   
    if parentItem and isinstance(parentItem, Group) and row < len(parentItem.elements):
      #print "<<< index result " + repr(parentItem.elements[row].name) + " " + repr(row) + " " + repr(column)
      return self.createIndex(row, column, parentItem.elements[row])
    else:
      #print "<<< index result QtCore.QModelIndex()"
      return QtCore.QModelIndex()

  def parent(self, index):
    #print " " 
    #print ">>> parent " 
    if not index.isValid():
      return QtCore.QModelIndex()

    item = index.internalPointer()
    #print item.name
    if item in self.workflow.mainGroup.elements:
      #print "<<< parent 1 return QtCore.QModelIndex()" 
      return QtCore.QModelIndex()
    
    parentItem = None
    #print "groups " + repr(len(self.workflow.groups))
    for group in self.workflow.groups:
      if item in group.elements:
        #print item.name + " is in " + group.name
        parentItem = group
      #else:
        #print item.name + " is not in " + group.name
    
    if not parentItem:
      #print "<<< parent 2 return QtCore.QModelIndex()" 
      return QtCore.QModelIndex()
        
    if parentItem in self.workflow.mainGroup.elements:
      #print "<<< parent return " + repr(self.workflow.mainGroup.elements.index(parentItem)) + " 0, mainGroup" 
      return self.createIndex(self.workflow.mainGroup.elements.index(parentItem), 0, parentItem)
    
    for group in self.workflow.groups:
      if parentItem in group.elements:
        #print "<<< parent return " + repr(group.elements.index(parentItem)) + " 0 " + group.name 
        return self.createIndex(group.elements.index(parentItem), 0, parentItem)


  def rowCount(self, parent):
    #print " " 
    #print ">>> rowCount"
    if not parent.isValid():
      item= self.workflow.mainGroup
    else:
      item = parent.internalPointer()
      
    #print "parent " + item.name
    
    if isinstance(item, Group):
      #print "<<< rowCount : " + repr(len(item.elements))
      return len(item.elements)
    else:
      #print "<<< rowCount : " + repr(0)
      return 0

  def columnCount(self, parent):
    #print " " 
    #print ">>> columnCount"
    if not parent.isValid():
      item= self.workflow.mainGroup
    else:
      item = parent.internalPointer()
    #print "parent " + item.name
    
    if isinstance(item, Group):
      #print "<<< columnCount : " + repr(1)
      return 1
    else:
      #print "<<< columnCount : " + repr(0)
      return 0

  def data(self, index, role):
    if not index.isValid():
      return QtCore.QVariant()
  
    item = index.internalPointer()
    #### Groups ####
    if isinstance(item, Group):
      if role == QtCore.Qt.DisplayRole:
        return item.name
      if role == QtCore.Qt.DecorationRole:
        return QtGui.QColor(255, 255, 255)
    
    #### JobTemplates ####
    if isinstance(item, JobTemplate):
      if item.job_id == -1:
        if role == QtCore.Qt.DisplayRole:
          return item.name
        if role == QtCore.Qt.DecorationRole:
          return QtGui.QColor(255, 255, 255)
        
      status = self.jobs.status(item.job_id)
      # not submitted
      if status == NOT_SUBMITTED:
        if role == QtCore.Qt.DisplayRole:
          return item.name
        if role == QtCore.Qt.DecorationRole:
          return QtGui.QColor(100, 100, 100)
      # Done or Failed
      if status == DONE or status == FAILED:
        exit_status, exit_value, term_signal, resource_usage = self.jobs.exitInformation(item.job_id)
        if role == QtCore.Qt.DisplayRole:
           return item.name + " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
        if role == QtCore.Qt.DecorationRole:
          if status == DONE:
             return QtGui.QColor(100, 100, 255)
          if status == FAILED:
            return QtGui.QColor(255, 50, 50)
          
      # Running
      if role == QtCore.Qt.DisplayRole:
        return item.name + " running..."
      if role == QtCore.Qt.DecorationRole:
        return QtGui.QColor(0, 250, 0)
    
    return QtCore.QVariant()


class WorkflowElementInfo(QtGui.QLabel):
  
  def __init__(self, parent = None):
    super(WorkflowElementInfo, self).__init__(parent)
    self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    
    
class WorkflowTreeView(QtGui.QTreeView):
  
  def __init__(self, parent = None):
    super(WorkflowTreeView, self).__init__(parent)
   # self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    