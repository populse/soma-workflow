from PyQt4 import QtGui, QtCore
from soma.jobs.jobClient import Workflow, Group, FileSending, FileRetrieving, FileTransfer, JobTemplate



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


    
  def setWorkflow(self, workflow):
    self.workflowItemModel = WorkflowItemModel(workflow, self)
    self.workflowTreeView.setModel(self.workflowItemModel)
    self.workflowTreeView.expandAll()
    
    
class WorkflowGraphView(QtGui.QGraphicsView):
  
  def __init__(self, parent = None):
    super(WorkflowGraphView, self).__init__(parent)
    self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    
      
class WorkflowItemModel(QtCore.QAbstractItemModel):
  
  def __init__(self, workflow, parent=None):
    super(WorkflowItemModel, self).__init__(parent)
    self.workflow = workflow   
    
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
    #print " " 
    #print ">>> data " 
    if not index.isValid():
      #print "<<< data " 
      return QtCore.QVariant()
    
    if not role == QtCore.Qt.DisplayRole:
      #print "<<< data " 
      return QtCore.QVariant()

    item = index.internalPointer()
    #print "<<< data " + item.name 
    return item.name
      



class WorkflowElementInfo(QtGui.QLabel):
  
  def __init__(self, parent = None):
    super(WorkflowElementInfo, self).__init__(parent)
    self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    
    
class WorkflowTreeView(QtGui.QTreeView):
  
  def __init__(self, parent = None):
    super(WorkflowTreeView, self).__init__(parent)
   # self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    