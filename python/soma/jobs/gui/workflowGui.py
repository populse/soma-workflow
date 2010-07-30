from PyQt4 import QtGui, QtCore
from soma.jobs.jobClient import Workflow, Group, FileSending, FileRetrieving, FileTransfer, JobTemplate
from soma.jobs.constants import *
import time
import threading

GRAY=QtGui.QColor(200, 200, 180)
BLUE=QtGui.QColor(0,200,255)
RED=QtGui.QColor(255,100,50)
GREEN=QtGui.QColor(155,255,50)
LIGHT_BLUE=QtGui.QColor(200,255,255)


class WorkflowWidget(QtGui.QWidget):
  
  def __init__(self, workflowControler, parent = None, flags = 0):
    super(WorkflowWidget, self).__init__(parent)
    
    self.workflowControler = workflowControler
    self.workflow = workflowControler.workflow
    
    self.setWindowTitle("Workflows !!")
   
    self.workflowTreeView = WorkflowTreeView(self)
    self.workflowItemModel = WorkflowItemModel(self.workflowControler.workflow, self.workflowControler.jobs, self)
    self.workflowTreeView.setModel(self.workflowItemModel)
    self.workflowTreeView.expandAll()
    
    self.workflowGraphView = WorkflowGraphView(self)
    self.workflowElementInfo = WorkflowElementInfo(self)
  
    self.submitWorkflowButton = QtGui.QPushButton("submit", self)
    self.transferInFilesButton = QtGui.QPushButton("transfer input files", self)
    self.transferOutFilesButton = QtGui.QPushButton("transfer output files", self)
  
    rvlayout = QtGui.QVBoxLayout()
    rvlayout.addWidget(self.workflowGraphView)
    rvlayout.addWidget(self.workflowElementInfo)
    
    lvlayout = QtGui.QVBoxLayout()
    lvlayout.addWidget(self.workflowTreeView)
    lvlayout.addWidget(self.submitWorkflowButton)
    lvlayout.addWidget(self.transferInFilesButton)
    lvlayout.addWidget(self.transferOutFilesButton)
    
    layout = QtGui.QHBoxLayout()
    layout.addLayout(lvlayout)
    layout.addLayout(rvlayout)
    self.setLayout(layout)
    
    self.submitWorkflowButton.clicked.connect(self.submitWorkflow)
    self.transferInFilesButton.clicked.connect(self.transferInputFiles)
    self.transferOutFilesButton.clicked.connect(self.transferOutputFiles)

  @QtCore.pyqtSlot()
  def submitWorkflow(self):
    self.workflowControler.workflowSubmission()
    self.workflowItemModel = WorkflowItemModel(self.workflowControler.submitted_workflow, self.workflowControler.jobs, self)
    self.workflowTreeView.setModel(self.workflowItemModel)
    self.workflowTreeView.expandAll()
    
  @QtCore.pyqtSlot()
  def transferInputFiles(self):
    self.workflowControler.transferInputFiles()
  
  @QtCore.pyqtSlot()
  def transferOutputFiles(self):
    self.workflowControler.transferOutputFiles()
  
    
class WorkflowGraphView(QtGui.QGraphicsView):
  
  def __init__(self, parent = None):
    super(WorkflowGraphView, self).__init__(parent)
    self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    
      
  
class WorkflowItem(object):
  
  GROUP = "group"
  JOB = "job"
  OUTPUT_FILE_T = "output_file_transfer"
  INPUT_FILE_T = "input_file_transfer"
  
  def __init__(self, it_id, 
               parent = -1, 
               row = -1,
               it_type = None,
               data = None,
               children_nb = 0):
    self.it_id = it_id
    self.parent = parent
    self.row = row
    self.it_type = it_type
    self.data = data
    self.children = [-1 for i in range(children_nb)]   
    
class WorkflowItemModel(QtCore.QAbstractItemModel):
  
  def __init__(self, workflow, jobs = None, parent=None):
    
    super(WorkflowItemModel, self).__init__(parent)
    self.workflow = workflow 
    self.jobs = jobs
    
    w_js = []
    w_fts = []
    for node in self.workflow.nodes:
      if isinstance(node, JobTemplate):
        w_js.append(node)
      elif isinstance(node, FileTransfer):
        w_fts.append(node)
     
    # ids => {workflow element: sequence of ids}
    self.ids = {}
    self.root_id = -1
    # items => {id : WorkflowItem}
    self.items = {}
    # unique id for the items
    id_cnt = 0
    
    # Jobs
    for job in w_js:
      item_id = id_cnt
      id_cnt = id_cnt + 1
      self.ids[job] = item_id
      self.items[item_id] = WorkflowItem(it_id = item_id, 
                                         parent = -1, 
                                         row = -1, 
                                         it_type = WorkflowItem.JOB, 
                                         data = job, 
                                         children_nb = len(job.referenced_input_files)+len(job.referenced_output_files))
      
    # Groups
    self.root_item = WorkflowItem(it_id = -1, 
                                  parent = -1, 
                                  row = -1, 
                                  it_type = WorkflowItem.GROUP, 
                                  data = self.workflow.mainGroup, 
                                  children_nb = len(self.workflow.mainGroup.elements))
                                       
    
    for group in self.workflow.groups:
      item_id = id_cnt
      id_cnt = id_cnt + 1
      self.ids[group] = item_id
      self.items[item_id] =  WorkflowItem(it_id = item_id, 
                                          parent = -1, 
                                          row = -1, 
                                          it_type = WorkflowItem.GROUP, 
                                          data = group, 
                                          children_nb = len(group.elements))
    
    # parent and children research for jobs and groups
    for item in self.items.values():
      if item.it_type == WorkflowItem.GROUP or item.it_type == WorkflowItem.JOB:
        if item.data in self.workflow.mainGroup.elements:
          item.parent = -1
          item.row = self.workflow.mainGroup.elements.index(item.data)
          self.root_item.children[item.row]=item.it_id
        for group in self.workflow.groups:
          if item.data in group.elements:
            item.parent = self.ids[group]
            item.row = group.elements.index(item.data)
            self.items[item.parent].children[item.row]=item.it_id
    
    # file transfers
    for ft in w_fts:
      self.ids[ft] = []
      for job in w_js:
        if ft in job.referenced_input_files or ft.local_file_path in job.referenced_input_files:
          item_id = id_cnt
          id_cnt = id_cnt + 1
          self.ids[ft].append(item_id)
          if ft in job.referenced_input_files:
            row = job.referenced_input_files.index(ft)
          else: 
            row = job.referenced_input_files.index(ft.local_file_path)
          self.items[item_id] = WorkflowItem( it_id = item_id, 
                                              parent=self.ids[job], 
                                              row = row, 
                                              it_type = WorkflowItem.INPUT_FILE_T, 
                                              data = ft)
          self.items[self.ids[job]].children[row]=item_id
        if ft in job.referenced_output_files or ft.local_file_path in job.referenced_output_files:
          item_id = id_cnt
          id_cnt = id_cnt + 1
          self.ids[ft].append(item_id)
          if ft in job.referenced_output_files:
            row = len(job.referenced_input_files)+job.referenced_output_files.index(ft)
          else:
            row = len(job.referenced_input_files)+job.referenced_output_files.index(ft.local_file_path)
          self.items[item_id] = WorkflowItem( it_id = item_id, 
                                              parent=self.ids[job], 
                                              row = row, 
                                              it_type = WorkflowItem.OUTPUT_FILE_T, 
                                              data = ft)
          self.items[self.ids[job]].children[row]=item_id
                                  
    ########## #print model ####################
    #for item in self.items.values():
      #print repr(item.it_id) + " " + repr(item.parent) + " " + repr(item.row) + " " + repr(item.it_type) + " " + repr(item.data.name) + " " + repr(item.children)    
    #raw_input()
    ###########################################
    
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
    ##print " " 
    ##print ">>> index " + repr(row) + " " + repr(column) 
    if row < 0 or not column == 0:
      ##print "<<< index result QtCore.QModelIndex()"
      return QtCore.QModelIndex()
    
    if not parent.isValid():
      if row < len(self.root_item.children):
        ##print " index result " + self.items[self.root_item.children[row]].data.name + "  row:" + repr(row)
        return self.createIndex(row, column, self.items[self.root_item.children[row]])
    else:
      parent_item = parent.internalPointer()
      #print " parent " + parent_item.data.name
      if row < len(parent_item.children):
        #print " index result " + self.items[parent_item.children[row]].data.name + " row:" + repr(row) 
        return self.createIndex(row, column, self.items[parent_item.children[row]])
      
    #print "<<< index result QtCore.QModelIndex()"
    return QtCore.QModelIndex()
    
    
    
  def parent(self, index):
    #print " " 
    #print ">>> parent " 
    
    if not index.isValid():
      #print "<<< parent QtCore.QModelIndex()"
      return QtCore.QModelIndex()
    
    item = index.internalPointer()
    #print "   " + item.data.name
    if not item.parent == -1:
      parent_item = self.items[item.parent]
      #print "<<< parent " + parent_item.data.name + " row: " + repr(parent_item.row)
      return self.createIndex(parent_item.row, 0, self.items[item.parent])
    
    #print "<<< parent QtCore.QModelIndex()"
    return QtCore.QModelIndex()
    

  def rowCount(self, parent):
    #print " " 
    #print ">>> rowCount"
    if not parent.isValid():
      #print "    parent root_item" 
      #print "<<< rowCount : " + repr(len(self.root_item.children))
      return len(self.root_item.children)
    else:
      parent_item = parent.internalPointer()
      #print "    parent " + parent_item.data.name
      #print "<<< rowCount : " + repr(len(parent_item.children))
      return len(parent_item.children)
    

  def columnCount(self, parent):
    #print " " 
    #print ">>> columnCount"
    
    children_nb = 0
    if not parent.isValid():
      children_nb = len(self.root_item.children)
      #print "   parent = root_item"
    else:
      children_nb = len(parent.internalPointer().children)
      #print "   parent = " + parent.internalPointer().data.name
        
    if children_nb == 0:
      #print "<<< columnCount : " + repr(0)
      return 0
    else:
      #print "<<< columnCount : " + repr(1)
      return 1
    


  def data(self, index, role):
    #print "  "
    #print ">>> data "
    if not index.isValid():
      return QtCore.QVariant()
  
    item = index.internalPointer()
    #print "  item " + item.data.name
    #### Groups ####
    if item.it_type == WorkflowItem.GROUP:
      if role == QtCore.Qt.DisplayRole:
        #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name
        return item.data.name
    
    #### JobTemplates ####
    if item.it_type == WorkflowItem.JOB:
      if item.data.job_id == -1:
        if role == QtCore.Qt.DisplayRole:
          #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name
          return item.data.name
      else:
        status = self.jobs.status(item.data.job_id)
        # not submitted
        if status == NOT_SUBMITTED:
          if role == QtCore.Qt.DisplayRole:
            #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name
            return item.data.name
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole GRAY"
            return GRAY
        # Done or Failed
        if status == DONE or status == FAILED:
          exit_status, exit_value, term_signal, resource_usage = self.jobs.exitInformation(item.data.job_id)
          if role == QtCore.Qt.DisplayRole:
            #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name + " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
            return item.data.name + " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
          if role == QtCore.Qt.DecorationRole:
            if status == DONE:
              #print "<<<< data QtCore.Qt.DecorationRole LIGHT_BLUE"
              return LIGHT_BLUE
            if status == FAILED:
              #print "<<<< data QtCore.Qt.DecorationRole RED"
              return RED
          
        # Running
        if role == QtCore.Qt.DisplayRole:
          #print "<<<< data QtCore.Qt.DisplayRole" + item.data.name + " running..."
          return item.data.name + " running..."
        if role == QtCore.Qt.DecorationRole:
          #print "<<<< data QtCore.Qt.DecorationRole GREEN"
          return GREEN
        
    #### FileTransfers ####
    if item.it_type == WorkflowItem.OUTPUT_FILE_T or item.it_type == WorkflowItem.INPUT_FILE_T :
      if item.it_type == WorkflowItem.INPUT_FILE_T:
        #if role == QtCore.Qt.BackgroundRole:
          ##print "<<<< data QtCore.Qt.BackgroundRole QtGui.QBrush(QtGui.QColor(200, 200, 255))"
          #return QtGui.QBrush(QtGui.QColor(255, 200, 200))
        if role == QtCore.Qt.ForegroundRole:
          #print "<<<< data QtCore.Qt.ForegroundRole QtGui.QBrush(RED)"
          return QtGui.QBrush(RED)
        display = "input: " + item.data.name
      if item.it_type == WorkflowItem.OUTPUT_FILE_T:
        #if role == QtCore.Qt.BackgroundRole:
          ##print "<<<< data QtCore.Qt.BackgroundRole QtGui.QBrush(QtGui.QColor(255, 200, 200))"
          #return QtGui.QBrush(QtGui.QColor(255, 200, 200))
        if role == QtCore.Qt.ForegroundRole:
          #print "<<<< data QtCore.Qt.ForegroundRole QtGui.QBrush(BLUE)"
          return QtGui.QBrush(BLUE)
        display = "output: " + item.data.name
        
      if item.data.local_file_path == " ":
        if role == QtCore.Qt.DisplayRole:
          #print "<<<< data QtCore.Qt.DisplayRole " + display
          return display
      else:
        status = self.jobs.transferStatus(item.data.local_file_path)
        if role == QtCore.Qt.DisplayRole:
          #print "<<<< data QtCore.Qt.DisplayRole " + display + " => " + status
          return display + " => " + status
        if status == TRANSFER_NOT_READY:
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole GRAY"
            return GRAY
        if status == READY_TO_TRANSFER:
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole BLUE"
            return BLUE
        if status == TRANSFERING:
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole GREEN"
            return GREEN
        if status == TRANSFERED:
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole LIGHT_BLUE"
            return LIGHT_BLUE
    
    #print "<<<< data "
    return QtCore.QVariant()


class WorkflowElementInfo(QtGui.QLabel):
  
  def __init__(self, parent = None):
    super(WorkflowElementInfo, self).__init__(parent)
    self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    
    
class WorkflowTreeView(QtGui.QTreeView):
  
  def __init__(self, parent = None):
    super(WorkflowTreeView, self).__init__(parent)
   # self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
    