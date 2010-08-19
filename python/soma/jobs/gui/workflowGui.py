from PyQt4 import QtGui, QtCore
from PyQt4 import uic
from soma.jobs.jobClient import Workflow, Group, FileSending, FileRetrieving, FileTransfer, JobTemplate
from soma.jobs.constants import *
import time
import threading
import os
from datetime import date
from datetime import datetime
from datetime import timedelta

GRAY=QtGui.QColor(200, 200, 180)
BLUE=QtGui.QColor(0,200,255)
RED=QtGui.QColor(255,100,50)
GREEN=QtGui.QColor(155,255,50)
LIGHT_BLUE=QtGui.QColor(200,255,255)

Ui_WorkflowMainWindow = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'WorkflowMainWindow.ui' ))[0]
Ui_JobInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'JobInfo.ui' ))[0]
Ui_TransferInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'TransferInfo.ui' ))[0]


class WorkflowWidget(QtGui.QMainWindow):
  
  def __init__(self, controler, parent = None, flags = 0):
    super(WorkflowWidget, self).__init__(parent)
    
    self.ui = Ui_WorkflowMainWindow()
    self.ui.setupUi(self)

    self.controler = controler
    self.controler.connect('neurospin_test_cluster')
    assert(self.controler.isConnected())
    
    self.setWindowTitle("Workflows !!")
   
    self.current_workflow = None
    self.expiration_date = None
    self.itemModel = None
   
    self.graphWidget = WorkflowGraphView(self.controler, self)
    graphWidgetLayout = QtGui.QVBoxLayout()
    graphWidgetLayout.addWidget(self.graphWidget)
    self.ui.graphViewScrollAreaWidgetContents.setLayout(graphWidgetLayout)
    
    self.itemInfoWidget = WorkflowElementInfo(self)
    itemInfoLayout = QtGui.QVBoxLayout()
    itemInfoLayout.addWidget(self.itemInfoWidget)
    self.ui.dockWidgetContents_intemInfo.setLayout(itemInfoLayout)

    self.ui.toolButton_submit.setDefaultAction(self.ui.action_submit)
    self.ui.toolButton_transfer_input.setDefaultAction(self.ui.action_transfer_infiles)
    self.ui.toolButton_transfer_output.setDefaultAction(self.ui.action_transfer_outfiles)
    self.ui.toolButton_button_delete_wf.setDefaultAction(self.ui.action_delete_workflow)
    self.ui.toolButton_change_exp_date.setDefaultAction(self.ui.action_change_expiration_date)
    
    self.ui.action_submit.triggered.connect(self.submitWorkflow)
    self.ui.action_transfer_infiles.triggered.connect(self.transferInputFiles)
    self.ui.action_transfer_outfiles.triggered.connect(self.transferOutputFiles)
    self.ui.action_open_wf.triggered.connect(self.openWorkflow)
    self.ui.action_create_wf_ex.triggered.connect(self.createWorkflowExample)
    self.ui.action_delete_workflow.triggered.connect(self.deleteWorkflow)
    self.ui.action_change_expiration_date.triggered.connect(self.changeExpirationDate)
    
    
    self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
    self.updateWorkflowList()
    self.currentWorkflowChanged()

  @QtCore.pyqtSlot()
  def openWorkflow(self):
    file_path = QtGui.QFileDialog.getOpenFileName(self, "Open a workflow");
    if file_path:
      self.currentWorkflowAboutToChange()
      self.current_workflow = self.controler.readWorkflowFromFile(file_path)
      self.currentWorkflowChanged()
      
    
  @QtCore.pyqtSlot()
  def createWorkflowExample(self):
    file_path = QtGui.QFileDialog.getSaveFileName(self, "Create a workflow example");
    if file_path:
      self.controler.generateWorkflowExample(file_path)

  @QtCore.pyqtSlot()
  def submitWorkflow(self):
    assert(self.current_workflow)
    
    name = unicode(self.ui.lineedit_wf_name.text())
    if name == "": name = None
    qtdt = self.ui.dateTimeEdit_expiration.dateTime()
    date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                    qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
    
    self.currentWorkflowAboutToChange()
    self.current_workflow = self.controler.submitWorkflow(self.current_workflow, name, date)
    self.expiration_date = date
    self.updateWorkflowList()
    self.currentWorkflowChanged()
    
    
  @QtCore.pyqtSlot()
  def transferInputFiles(self):
    self.controler.transferInputFiles(self.current_workflow)
  
  @QtCore.pyqtSlot()
  def transferOutputFiles(self):
    self.controler.transferOutputFiles(self.current_workflow)
    
  @QtCore.pyqtSlot(int)
  def workflowSelectionChanged(self, index):
    if index <0 or index >= self.ui.combo_submitted_wfs.count():
      return
    
    wf_id = self.ui.combo_submitted_wfs.itemData(index).toInt()[0]
    self.currentWorkflowAboutToChange()
    if wf_id != -1:
      (self.current_workflow, self.expiration_date) = self.controler.getWorkflow(wf_id)
    else:
      self.current_workflow = None
      self.expiration_date = datetime.now()
    self.currentWorkflowChanged()
    
  @QtCore.pyqtSlot()
  def deleteWorkflow(self):
    assert(self.current_workflow and self.current_workflow.wf_id != -1)
    
    if self.current_workflow.name:
      name = self.current_workflow.name
    else: 
      name = repr(self.current_workflow.wf_id)
    
    answer = QtGui.QMessageBox.question(self, "confirmation", "Do you want to delete the worflow " + name +"?", QtGui.QMessageBox.Ok, QtGui.QMessageBox.NoButton)
    if answer != QtGui.QMessageBox.Ok: return

    self.currentWorkflowAboutToChange()
    self.controler.deleteWorkflow(self.current_workflow.wf_id)
    self.current_workflow = None
    self.updateWorkflowList()
    self.currentWorkflowChanged()
    
  @QtCore.pyqtSlot()
  def changeExpirationDate(self):
    qtdt = self.ui.dateTimeEdit_expiration.dateTime()
    date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                    qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
    change_occured = self.controler.changeWorkflowExpirationDate(self.current_workflow.wf_id,date)
    if not change_occured:
      QtGui.QMessageBox.information(self, "information", "The workflow expiration date was not changed.")
      self.ui.dateTimeEdit_expiration.setDateTime(self.expiration_date)
    else:
      self.expiration_date = date
    
  
  def currentWorkflowAboutToChange(self):
    if self.itemModel:
        self.itemModel.emit(QtCore.SIGNAL("modelAboutToBeReset()"))
        self.itemModel.stopUpdateThread()
        self.itemModel
  
  def currentWorkflowChanged(self):
    if not self.current_workflow:
      # No workflow
      self.ui.treeView.setModel(None)
      
      self.graphWidget.clear()
      self.itemInfoWidget.clear()
      
      self.ui.lineedit_wf_name.clear()
      self.ui.lineedit_wf_name.setEnabled(False)
      
      self.ui.dateTimeEdit_expiration.setDateTime(datetime.now())
      self.ui.dateTimeEdit_expiration.setEnabled(False)
      
      self.ui.action_submit.setEnabled(False)
      self.ui.action_change_expiration_date.setEnabled(False)
      self.ui.action_delete_workflow.setEnabled(False)
      self.ui.action_transfer_infiles.setEnabled(False)
      self.ui.action_transfer_outfiles.setEnabled(False)
      
      self.ui.combo_submitted_wfs.currentIndexChanged.disconnect(self.workflowSelectionChanged)
      self.ui.combo_submitted_wfs.setCurrentIndex(0)
      self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
    else:
      self.itemModel = WorkflowItemModel(self.current_workflow, self.controler.jobs, self)
      self.ui.treeView.setModel(self.itemModel)
      self.itemModel.emit(QtCore.SIGNAL("modelReset()"))
      
      self.itemModel.dataChanged.connect(self.graphWidget.dataChanged)
      self.graphWidget.setWorflow(self.current_workflow)
      
      self.itemModel.dataChanged.connect(self.itemInfoWidget.dataChanged)
      self.itemInfoWidget.setSelectionModel(self.ui.treeView.selectionModel())
      
      if self.current_workflow.wf_id == -1:
        # Workflow not submitted
        if self.current_workflow.name:
          self.ui.lineedit_wf_name.setText(self.current_workflow.name)
        else:
          self.ui.lineedit_wf_name.clear()
        self.ui.lineedit_wf_name.setEnabled(True)
        
        self.ui.dateTimeEdit_expiration.setDateTime(datetime.now() + timedelta(days=5))
        self.ui.dateTimeEdit_expiration.setEnabled(True)
        
        self.ui.action_submit.setEnabled(True)
        self.ui.action_change_expiration_date.setEnabled(False)
        self.ui.action_delete_workflow.setEnabled(False)
        self.ui.action_transfer_infiles.setEnabled(False)
        self.ui.action_transfer_outfiles.setEnabled(False)
        
        self.ui.combo_submitted_wfs.currentIndexChanged.disconnect(self.workflowSelectionChanged)
        self.ui.combo_submitted_wfs.setCurrentIndex(0)
        self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
        
      else:
        # Submitted workflow
        if self.current_workflow.name:
          self.ui.lineedit_wf_name.setText(self.current_workflow.name)
        else: 
          self.ui.lineedit_wf_name.setText(repr(self.current_workflow.wf_id))
        self.ui.lineedit_wf_name.setEnabled(False)
        
        self.ui.dateTimeEdit_expiration.setDateTime(self.expiration_date)
        self.ui.dateTimeEdit_expiration.setEnabled(True)
        
        self.ui.action_submit.setEnabled(False)
        self.ui.action_change_expiration_date.setEnabled(True)
        self.ui.action_delete_workflow.setEnabled(True)
        self.ui.action_transfer_infiles.setEnabled(True)
        self.ui.action_transfer_outfiles.setEnabled(True)        
        
        index = self.ui.combo_submitted_wfs.findData(self.current_workflow.wf_id)
        self.ui.combo_submitted_wfs.currentIndexChanged.disconnect(self.workflowSelectionChanged)
        self.ui.combo_submitted_wfs.setCurrentIndex(index)
        self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
        
      #self.graphWidget.resize(self.graphWidget.pixmap.size()*2.0)

  
  def updateWorkflowList(self):
    self.ui.combo_submitted_wfs.currentIndexChanged.disconnect(self.workflowSelectionChanged)
    self.ui.combo_submitted_wfs.clear()
    for wf_info in self.controler.getSubmittedWorkflows():
      wf_id, expiration_date, workflow_name = wf_info
      if not workflow_name: workflow_name = repr(wf_id)
      self.ui.combo_submitted_wfs.addItem(workflow_name, wf_id)
    self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
    
          
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
    self.parent = parent # parent_id
    self.row = row
    if it_type:
      self.it_type = it_type
    else: 
      self.it_type = WorkflowItem.GROUP
    self.data = data
    self.children = [-1 for i in range(children_nb)]   
    
    self.state = {} 
    if self.it_type == WorkflowItem.JOB:
      self.state["status"] = " "
      self.state["exit_info"] = (" ", " ", " ", " ")
      self.state["stdout_path"] = " "
      self.state["stderr_path"] = " "
      self.state["stdout"] = " "
      self.state["stderr"] = " "
      
    elif self.it_type == WorkflowItem.OUTPUT_FILE_T or self.it_type == WorkflowItem.INPUT_FILE_T:
      self.state["transfer_status"] = " "
    
  def updateState(self, jobs):
    '''
    @type  jobs: soma.jobs.jobClient.job
    @param jobs: jobs interface (connection)
    @rtype: boolean
    @returns: did the state change?
    '''
    state_changed = False
    if self.it_type == WorkflowItem.JOB and self.data.job_id != -1 :
      status = jobs.status(self.data.job_id)
      state_changed = state_changed or status != self.state["status"]
      self.state["status"]=status
      #if state_changed and status == DONE or status == FAILED:
      exit_info =  jobs.exitInformation(self.data.job_id)
      state_changed = state_changed or exit_info != self.state["exit_info"]
      self.state["exit_info"] = jobs.exitInformation(self.data.job_id)
      
      if self.state["stdout"] == " " and (status == DONE or status == FAILED):
        line = jobs.stdoutReadLine(self.data.job_id)
        stdout = ""
        while line:
          stdout = stdout + line + "\n"
          line = jobs.stdoutReadLine(self.data.job_id)
        self.state["stdout"] = stdout
          
      if self.state["stderr"] == " " and (status == DONE or status == FAILED):
        line = jobs.stderrReadLine(self.data.job_id)
        stderr = ""
        while line:
          stderr = stderr + line + "\n"
          line = jobs.stderrReadLine(self.data.job_id)
        self.state["stderr"] = stderr
      
    elif (self.it_type == WorkflowItem.OUTPUT_FILE_T or self.it_type == WorkflowItem.INPUT_FILE_T) and self.data.local_path:
      transfer_status = jobs.transferStatus(self.data.local_path)
      state_changed = state_changed or transfer_status != self.state["transfer_status"]
      self.state["transfer_status"] = transfer_status
    return state_changed
    

    
class WorkflowItemModel(QtCore.QAbstractItemModel):
  
  def __init__(self, workflow, jobs = None, parent=None):
    
    super(WorkflowItemModel, self).__init__(parent)
    self.jobs = jobs
    
    w_js = set([])
    w_fts = set([])
    if not workflow.full_nodes:
      for node in workflow.nodes:
        if isinstance(node, JobTemplate):
          w_js.add(node)
        elif isinstance(node, FileTransfer):
          w_fts.add(node)
    else:
      for node in workflow.full_nodes:
        if isinstance(node, JobTemplate):
          w_js.add(node)
        elif isinstance(node, FileTransfer):
          w_fts.add(node)
    
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
      for ft in job.referenced_input_files:
        if isinstance(ft, FileTransfer): w_fts.add(ft)
      for ft in job.referenced_output_files:
        if isinstance(ft, FileTransfer): w_fts.add(ft)
      
      
    # Groups
    self.root_item = WorkflowItem(it_id = -1, 
                                  parent = -1, 
                                  row = -1, 
                                  it_type = WorkflowItem.GROUP, 
                                  data = workflow.mainGroup, 
                                  children_nb = len(workflow.mainGroup.elements))
                                       
    
    for group in workflow.groups:
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
        if item.data in workflow.mainGroup.elements:
          item.parent = -1
          item.row = workflow.mainGroup.elements.index(item.data)
          self.root_item.children[item.row]=item.it_id
        for group in workflow.groups:
          if item.data in group.elements:
            item.parent = self.ids[group]
            item.row = group.elements.index(item.data)
            self.items[item.parent].children[item.row]=item.it_id
    
    # file transfers
    def compFileTransfers(ft1, ft2): 
      if isinstance(ft1, FileTransfer):
        str1 = ft1.name
      else: str1 = ft1
      if isinstance(ft2, FileTransfer):
        str2 = ft2.name
      else: str2 = ft2
      return cmp(str1, str2)
    for ft in w_fts:
      self.ids[ft] = []
      for job in w_js:
        ref_in = list(job.referenced_input_files)
        ref_in.sort(compFileTransfers)
        ref_out = list(job.referenced_output_files)
        ref_out.sort(compFileTransfers)
        if ft in ref_in or ft.local_path in ref_in:
          item_id = id_cnt
          id_cnt = id_cnt + 1
          self.ids[ft].append(item_id)
          if ft in ref_in:
            row = ref_in.index(ft)
          else: 
            row = ref_in.index(ft.local_path)
          self.items[item_id] = WorkflowItem( it_id = item_id, 
                                              parent=self.ids[job], 
                                              row = row, 
                                              it_type = WorkflowItem.INPUT_FILE_T, 
                                              data = ft)
          self.items[self.ids[job]].children[row]=item_id
        if ft in ref_out or ft.local_path in ref_out:
          item_id = id_cnt
          id_cnt = id_cnt + 1
          self.ids[ft].append(item_id)
          if ft in ref_out:
            row = len(ref_in)+ref_out.index(ft)
          else:
            row = len(ref_in)+ref_out.index(ft.local_path)
          self.items[item_id] = WorkflowItem( it_id = item_id, 
                                              parent=self.ids[job], 
                                              row = row, 
                                              it_type = WorkflowItem.OUTPUT_FILE_T, 
                                              data = ft)
          self.items[self.ids[job]].children[row]=item_id
                                  
    ########## #print model ####################
    #print "dependencies : " + repr(len(workflow.dependencies))
    #if workflow.full_dependencies: 
      #print "full_dependencies : " + repr(len(workflow.full_dependencies)) 
    #for dep in workflow.dependencies:
      #print dep[0].name + " -> " + dep[1].name
    #for item in self.items.values():
      #print repr(item.it_id) + " " + repr(item.parent) + " " + repr(item.row) + " " + repr(item.it_type) + " " + repr(item.data.name) + " " + repr(item.children)   
    #raw_input()
    ###########################################
    self.__update_state = True
    self.__update_interval = 3
    def updateLoop(self, interval):
      while self.__update_state:
        self.checkChanges()
        time.sleep(interval)
    
    self.__update_loop = threading.Thread(name = "WorflowItemModel_update_loop",
                                         target = updateLoop,
                                         args = (self, 3))
    
    self.__update_loop.setDaemon(True)
    self.__update_loop.start()
    
    
  def checkChanges(self):
    data_changed = False
    for item in self.items.values():
      #print "update state " + repr(self.__update_state) + " " + repr(self.name) + " " + repr(self.wf_id)
      if not self.__update_state: break 
      item_changed = item.updateState(self.jobs)
      data_changed = data_changed or item_changed
      #self.dataChanged.emit(self.createIndex(item.row, 0, item), self.createIndex(item.row, 0, item))
    
    if data_changed and self.__update_state:
      row = self.rowCount(QtCore.QModelIndex())
      self.dataChanged.emit(self.index(0,0,QtCore.QModelIndex()), self.index(row,0, QtCore.QModelIndex()))
        
  def stopUpdateThread(self):
    self.__update_state = False
    
    
    
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
        status = item.state["status"]
        #status = self.jobs.status(item.data.job_id)
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
          #exit_status, exit_value, term_signal, resource_usage = self.jobs.exitInformation(item.data.job_id)
          exit_status, exit_value, term_signal, resource_usage = item.state["exit_info"]
          if role == QtCore.Qt.DisplayRole:
            #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name + " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
            return item.data.name #+ " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
          if role == QtCore.Qt.DecorationRole:
            if status == DONE and exit_status == FINISHED_REGULARLY:
              #print "<<<< data QtCore.Qt.DecorationRole LIGHT_BLUE"
              return LIGHT_BLUE
            else:
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
        
      if not item.data.local_path:
        if role == QtCore.Qt.DisplayRole:
          #print "<<<< data QtCore.Qt.DisplayRole " + display
          return display
      else:
        #status = self.jobs.transferStatus(item.data.local_path)
        status = item.state["transfer_status"]
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


class WorkflowElementInfo(QtGui.QWidget):
  
  def __init__(self, parent = None):
    super(WorkflowElementInfo, self).__init__(parent)
    #self.setFrameStyle(QtGui.QFrame.Box| QtGui.QFrame.Plain)
   
    self.selectionModel = None
    self.infoWidget = None
    
    self.vLayout = QtGui.QVBoxLayout(self)
    
  def setSelectionModel(self, selectionModel):
    if self.selectionModel:
      self.selectionModel.currentChanged.disconnect(self.currentChanged)
    self.selectionModel = selectionModel
    self.selectionModel.currentChanged.connect(self.currentChanged)
    
    if self.infoWidget:
      self.infoWidget.hide()
      self.vLayout.removeWidget(self.infoWidget)
      self.infoWidget = None
      
  def clear(self):
    if self.infoWidget:
      self.infoWidget.hide()
      self.vLayout.removeWidget(self.infoWidget)
    self.infoWidget = None
    self.dataChanged()
    
    
  @QtCore.pyqtSlot(QtCore.QModelIndex, QtCore.QModelIndex)
  def currentChanged(self, current, previous):
    
    if self.infoWidget:
      self.infoWidget.hide()
      self.vLayout.removeWidget(self.infoWidget)
  
    item = current.internalPointer()
    if item.it_type == WorkflowItem.JOB:
      self.infoWidget = JobInfoWidget(item, self)
    elif item.it_type == WorkflowItem.OUTPUT_FILE_T or item.it_type == WorkflowItem.INPUT_FILE_T:
      self.infoWidget = TransferInfoWidget(item, self)
    else:
      self.infoWidget = None
      
    if self.infoWidget:
      self.vLayout.addWidget(self.infoWidget)
      
    self.update()
    
  @QtCore.pyqtSlot()
  def dataChanged(self):
    if self.infoWidget:
      self.infoWidget.dataChanged()
      
    
    
class JobInfoWidget(QtGui.QTabWidget):
  
  def __init__(self, jobItem, parent = None):
    super(JobInfoWidget, self).__init__(parent)
    
    self.ui = Ui_JobInfo()
    self.ui.setupUi(self)
    
    self.jobItem = jobItem
    
    self.dataChanged()
    
  def dataChanged(self):
 
    self.ui.job_name.setText(self.jobItem.data.name)
    self.ui.job_status.setText(self.jobItem.state["status"])
    exit_status, exit_value, term_signal, resource_usage = self.jobItem.state["exit_info"]
    if exit_status: 
      self.ui.exit_status.setText(exit_status)
    else: 
      self.ui.exit_status.setText(" ")
    if exit_value: 
      self.ui.exit_value.setText(exit_value)
    else: 
      self.ui.exit_value.setText(" ")
    if term_signal: 
      self.ui.term_signal.setText(term_signal)
    else: 
      self.ui.term_signal.setText(" ")
    if resource_usage: 
      self.ui.resource_usage.insertItems(0, resource_usage.split())
    else: 
      self.ui.resource_usage.clear()
    
    self.ui.stdout_path.setText(self.jobItem.state["stdout_path"])
    self.ui.stderr_path.setText(self.jobItem.state["stderr_path"])
    self.ui.stdout_file_contents.setText(self.jobItem.state["stdout"])
    self.ui.stderr_file_contents.setText(self.jobItem.state["stderr"])
    
    
class TransferInfoWidget(QtGui.QTabWidget):
  
  def __init__(self, transferItem, parent = None):
    super(TransferInfoWidget, self).__init__(parent)
    self.ui = Ui_TransferInfo()
    self.ui.setupUi(self)
    
    self.transferItem = transferItem
    self.dataChanged()
    
  def dataChanged(self):
    
    self.ui.transfer_name.setText(self.transferItem.data.name)
    self.ui.transfer_status.setText(self.transferItem.state["transfer_status"])
    
    self.ui.remote_path.setText(self.transferItem.data.remote_path)
    if self.transferItem.data.remote_paths:
      self.ui.remote_paths.insertItems(0, self.transferItem.data.remote_paths)
    else:
      self.ui.remote_paths.clear()
    
    if self.transferItem.data.local_path:
      self.ui.local_path.setText(self.transferItem.data.local_path)
    else: 
      self.ui.local_path.setText(" ")
    
    
class WorkflowGraphView(QtGui.QLabel):
  
  def __init__(self, controler, parent = None):
    super(WorkflowGraphView, self).__init__(parent)
    
    self.controler = controler
    self.workflow = None
   
    #self.setBackgroundRole(QtGui.QPalette.Base)
    #self.setSizePolicy(QtGui.QSizePolicy.Ignored, QtGui.QSizePolicy.Ignored)
    #self.setScaledContents(True)
    
    #self.resize(200,300)

                                        
  def setWorflow(self, workflow):
    self.workflow = workflow
    self.dataChanged()
    
  def clear(self):
    self.workflow = None
    self.dataChanged()
    
  @QtCore.pyqtSlot()
  def dataChanged(self):
    if self.workflow:
      image_file_path = self.controler.printWorkflow(self.workflow)
      self.image = QtGui.QImage(image_file_path)
      self.pixmap = QtGui.QPixmap.fromImage(self.image)
      self.setPixmap(self.pixmap)
    else:
      self.setPixmap(QtGui.QPixmap())
    