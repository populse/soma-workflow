from __future__ import with_statement
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
Ui_GraphWidget = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'graphWidget.ui' ))[0]
Ui_TransferInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'TransferInfo.ui' ))[0]
#Ui_TransferProgressionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'TransferProgressionDlg.ui' ))[0]
Ui_ConnectionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'connectionDlg.ui' ))[0]
Ui_FirstConnectionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'firstConnectionDlg.ui' ))[0]


class WorkflowWidget(QtGui.QMainWindow):
  
  def __init__(self, controler, client_model, parent = None, flags = 0):
    super(WorkflowWidget, self).__init__(parent)
    
    self.ui = Ui_WorkflowMainWindow()
    self.ui.setupUi(self)

    self.controler = controler
    self.model = client_model
    
    
    self.connect(self.model, QtCore.SIGNAL('current_connection_changed()'), self.currentConnectionChanged)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_about_to_change()'), self.currentWorkflowAboutToChange)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_changed()'),  self.currentWorkflowChanged)
    
    self.resource_list = self.controler.getRessourceIds()
    self.ui.combo_resources.addItems(self.resource_list)
    
    self.setWindowTitle("Workflows")
   
    self.itemModel = None # model for the TreeView
   
    self.graphWidget = WorkflowGraphView(self.controler, self)
    graphWidgetLayout = QtGui.QVBoxLayout()
    graphWidgetLayout.addWidget(self.graphWidget)
    self.ui.dockWidgetContents_graph_view.setLayout(graphWidgetLayout)
    
    self.itemInfoWidget = WorkflowElementInfo(self.model, self)
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
    self.ui.combo_resources.currentIndexChanged.connect(self.resourceSelectionChanged)
    
    self.showMaximized()
    
    # first connection:
    self.firstConnection_dlg = QtGui.QDialog(self)
    self.ui_firstConnection_dlg = Ui_FirstConnectionDlg()
    self.ui_firstConnection_dlg.setupUi(self.firstConnection_dlg)
    self.ui_firstConnection_dlg.combo_resources.addItems(self.resource_list)
    self.firstConnection_dlg.accepted.connect(self.firstConnection)
    self.firstConnection_dlg.rejected.connect(self.close)
    self.firstConnection_dlg.show()
    
    if not self.model.current_connection:
      self.close()
      
      
  @QtCore.pyqtSlot()
  def firstConnection(self):
    resource_id = unicode(self.ui_firstConnection_dlg.combo_resources.currentText())
    if self.ui_firstConnection_dlg.lineEdit_login.text(): 
      login = unicode(self.ui_firstConnection_dlg.lineEdit_login.text()).encode('utf-8')
    else: 
      login = None
    if self.ui_firstConnection_dlg.lineEdit_password.text():
      password = unicode(self.ui_firstConnection_dlg.lineEdit_password.text()).encode('utf-8')
    else:
      password = None
    test_no = 1
    (connection, msg) = self.controler.getConnection(resource_id, login, password, test_no)
    if connection:
      self.model.addConnection(resource_id, connection)
      self.firstConnection_dlg.hide()
    else:
      QtGui.QMessageBox.information(self, "Connection failed", msg)
      self.ui_firstConnection_dlg.lineEdit_password.clear()
      self.firstConnection_dlg.show()
    
  @QtCore.pyqtSlot()
  def openWorkflow(self):
    file_path = QtGui.QFileDialog.getOpenFileName(self, "Open a workflow");
    if file_path:
      workflow = self.controler.readWorkflowFromFile(file_path)
      self.model.addWorkflow(workflow, datetime.now() + timedelta(days=5))
      self.updateWorkflowList()
      
    
  @QtCore.pyqtSlot()
  def createWorkflowExample(self):
    file_path = QtGui.QFileDialog.getSaveFileName(self, "Create a workflow example");
    if file_path:
      self.controler.generateWorkflowExample(file_path)

  @QtCore.pyqtSlot()
  def submitWorkflow(self):
    assert(self.model.current_workflow)
    
    name = unicode(self.ui.lineedit_wf_name.text())
    if name == "": name = None
    qtdt = self.ui.dateTimeEdit_expiration.dateTime()
    date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                    qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
    
    workflow = self.controler.submitWorkflow(self.model.current_workflow.server_workflow, name, date, self.model.current_connection)
    self.model.addWorkflow(workflow, date) 
    self.updateWorkflowList()
    
  @QtCore.pyqtSlot()
  def transferInputFiles(self):
    self.controler.transferInputFiles(self.model.current_workflow.server_workflow, self.model.current_connection)
  
  @QtCore.pyqtSlot()
  def transferOutputFiles(self):
    self.controler.transferOutputFiles(self.model.current_workflow.server_worflow, self.model.current_connection)
    
  @QtCore.pyqtSlot(int)
  def workflowSelectionChanged(self, index):
    if index <0 or index >= self.ui.combo_submitted_wfs.count():
      return
    wf_id = self.ui.combo_submitted_wfs.itemData(index).toInt()[0]
    if wf_id != -1:
      if self.model.isLoadedWorkflow(wf_id):
        self.model.setCurrentWorkflow(wf_id)
      else:
        (workflow, expiration_date) = self.controler.getWorkflow(wf_id, self.model.current_connection)
        self.model.addWorkflow(workflow, expiration_date)
    else:
      self.clearCurrentWorkflow()
    
  @QtCore.pyqtSlot(int)
  def resourceSelectionChanged(self, index):
    if index <0 or index >= self.ui.combo_resources.count():
      index = self.ui.combo_resources.findText(self.model.current_resource_id)
      self.ui.combo_resources.setCurrentIndex(index)
      return
    
    resource_id = unicode(self.ui.combo_resources.itemText(index)).encode('utf-8')
    if resource_id == " ":
      index = self.ui.combo_resources.findText(self.model.current_resource_id)
      self.ui.combo_resources.setCurrentIndex(index)
      return
    
    if resource_id in self.model.connections.keys():
      self.model.setCurrentConnection(resource_id)
      
      return
    else:
      connection_invalid = True
      try_again = True
      while connection_invalid or try_again: 
        connection_dlg = QtGui.QDialog()
        ui = Ui_ConnectionDlg()
        ui.setupUi(connection_dlg)
        ui.resource_label.setText(resource_id)
        if connection_dlg.exec_() != QtGui.QDialog.Accepted: 
          try_again = False
          index = self.ui.combo_resources.findText(self.model.current_resource_id)
          self.ui.combo_resources.setCurrentIndex(index)
          break
        if ui.lineEdit_login.text(): 
          login = unicode(ui.lineEdit_login.text()).encode('utf-8')
        else: login = None
        if ui.lineEdit_password.text():
          password = unicode(ui.lineEdit_password.text()).encode('utf-8')
        else: password = None
        test_no = 1
        (connection, msg) = self.controler.getConnection(resource_id, login, password, test_no)
        if connection:
          connection_invalid = False
          self.model.addConnection(resource_id, connection)
          return 
        else:
          QtGui.QMessageBox.information(self, "Connection failed", msg)
    
  @QtCore.pyqtSlot()
  def deleteWorkflow(self):
    assert(self.model.current_workflow and self.model.current_wf_id != -1)
    
    if self.model.current_workflow.name:
      name = self.model.current_workflow.name
    else: 
      name = repr(self.model.current_wf_id)
    
    answer = QtGui.QMessageBox.question(self, "confirmation", "Do you want to delete the workflow " + name +"?", QtGui.QMessageBox.Ok, QtGui.QMessageBox.NoButton)
    if answer != QtGui.QMessageBox.Ok: return

    self.controler.deleteWorkflow(self.model.current_workflow.wf_id, self.model.current_connection)
    self.model.deleteWorkflow()
    self.updateWorkflowList()
    
  @QtCore.pyqtSlot()
  def changeExpirationDate(self):
    qtdt = self.ui.dateTimeEdit_expiration.dateTime()
    date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                    qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
    change_occured = self.controler.changeWorkflowExpirationDate(self.model.current_wf_id, date,  self.model.current_connection)
    if not change_occured:
      QtGui.QMessageBox.information(self, "information", "The workflow expiration date was not changed.")
      self.ui.dateTimeEdit_expiration.setDateTime(self.expiration_date)
    else:
      self.model.changeExpirationDate(date)
      
  
  @QtCore.pyqtSlot()
  def currentConnectionChanged(self):
    self.setWindowTitle("Workflows - " + self.model.current_resource_id)
    self.updateWorkflowList()
    self.model.clearCurrentWorkflow()
    index = self.ui.combo_resources.findText(self.model.current_resource_id)
    self.ui.combo_resources.setCurrentIndex(index)
      
  @QtCore.pyqtSlot()
  def currentWorkflowAboutToChange(self):
    if self.itemModel:
        self.itemModel.emit(QtCore.SIGNAL("modelAboutToBeReset()"))
        self.itemModel = None
  
  @QtCore.pyqtSlot()
  def currentWorkflowChanged(self):
    if not self.model.current_workflow:
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
      self.itemModel = WorkflowItemModel(self.model.current_workflow, self)
      self.ui.treeView.setModel(self.itemModel)
      self.itemModel.emit(QtCore.SIGNAL("modelReset()"))
      
      self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.graphWidget.dataChanged)
      
      #=> TEMPORARY : the graph view has to be built from the clientModel
      self.graphWidget.setWorkflow(self.model.current_workflow.server_workflow, self.model.current_connection)
      
      self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.itemInfoWidget.dataChanged)
      self.itemInfoWidget.setSelectionModel(self.ui.treeView.selectionModel())
      
      if self.model.current_wf_id == -1:
        # Workflow not submitted
        if self.model.current_workflow.name:
          self.ui.lineedit_wf_name.setText(self.model.current_workflow.name)
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
        if self.model.current_workflow.name:
          self.ui.lineedit_wf_name.setText(self.model.current_workflow.name)
        else: 
          self.ui.lineedit_wf_name.setText(repr(self.model.current_wf_id))
        self.ui.lineedit_wf_name.setEnabled(False)
        
        self.ui.dateTimeEdit_expiration.setDateTime(self.model.expiration_date)
        self.ui.dateTimeEdit_expiration.setEnabled(True)
        
        self.ui.action_submit.setEnabled(False)
        self.ui.action_change_expiration_date.setEnabled(True)
        self.ui.action_delete_workflow.setEnabled(True)
        self.ui.action_transfer_infiles.setEnabled(True)
        self.ui.action_transfer_outfiles.setEnabled(True)        
        
        index = self.ui.combo_submitted_wfs.findData(self.model.current_wf_id)
        self.ui.combo_submitted_wfs.currentIndexChanged.disconnect(self.workflowSelectionChanged)
        self.ui.combo_submitted_wfs.setCurrentIndex(index)
        self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
        
  def updateWorkflowList(self):
    self.ui.combo_submitted_wfs.currentIndexChanged.disconnect(self.workflowSelectionChanged)
    self.ui.combo_submitted_wfs.clear()
    for wf_info in self.controler.getSubmittedWorkflows(self.model.current_connection):
      wf_id, expiration_date, workflow_name = wf_info
      if not workflow_name: workflow_name = repr(wf_id)
      self.ui.combo_submitted_wfs.addItem(workflow_name, wf_id)
    self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
    
class WorkflowElementInfo(QtGui.QWidget):
  
  def __init__(self, client_model, parent = None):
    super(WorkflowElementInfo, self).__init__(parent)
    
    self.selectionModel = None
    self.infoWidget = None
    self.model = client_model # used to update stderr and stdout only
    
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
    if isinstance(item, ClientJob):
      self.infoWidget = JobInfoWidget(item, self.model.current_connection, self)
    elif isinstance(item, ClientFileTransfer):
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
  
  def __init__(self, job_item, connection, parent = None):
    super(JobInfoWidget, self).__init__(parent)
    
    self.ui = Ui_JobInfo()
    self.ui.setupUi(self)
    
    self.job_item = job_item
    self.connection = connection
    
    self.dataChanged()
    
    self.currentChanged.connect(self.currentTabChanged)
    self.ui.stderr_refresh_button.clicked.connect(self.refreshStderr)
    self.ui.stdout_refresh_button.clicked.connect(self.refreshStdout)
    
  def dataChanged(self):
 
    self.ui.job_name.setText(self.job_item.data.name)
    self.ui.job_status.setText(self.job_item.status)
    exit_status, exit_value, term_signal, resource_usage = self.job_item.exit_info
    if exit_status: 
      self.ui.exit_status.setText(exit_status)
    else: 
      self.ui.exit_status.setText(" ")
    if exit_value: 
      self.ui.exit_value.setText(repr(exit_value))
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
    
    self.ui.command.setText(self.job_item.command)
    self.ui.stdout_file_contents.setText(self.job_item.stdout)
    self.ui.stderr_file_contents.setText(self.job_item.stderr)
    
  
  @QtCore.pyqtSlot(int)
  def currentTabChanged(self, index):
    if index == 1 and self.job_item.stdout == "":
      self.job_item.updateStdout(self.connection)
      self.dataChanged()
    if index == 2 and self.job_item.stderr == "":
      self.job_item.updateStderr(self.connection)
      self.dataChanged()
      
  @QtCore.pyqtSlot()
  def refreshStdout(self):
    self.job_item.updateStdout(self.connection)
    self.dataChanged()
    
  @QtCore.pyqtSlot()
  def refreshStderr(self):
    self.job_item.updateStderr(self.connection)
    self.dataChanged()
    
  
    
class TransferInfoWidget(QtGui.QTabWidget):
  
  def __init__(self, transferItem, parent = None):
    super(TransferInfoWidget, self).__init__(parent)
    self.ui = Ui_TransferInfo()
    self.ui.setupUi(self)
    
    self.transferItem = transferItem
    self.dataChanged()
    
  def dataChanged(self):
    
    self.ui.transfer_name.setText(self.transferItem.data.name)
    self.ui.transfer_status.setText(self.transferItem.transfer_status)
    
    self.ui.remote_path.setText(self.transferItem.data.remote_path)
    if self.transferItem.data.remote_paths:
      self.ui.remote_paths.insertItems(0, self.transferItem.data.remote_paths)
    else:
      self.ui.remote_paths.clear()
    
    if self.transferItem.data.local_path:
      self.ui.local_path.setText(self.transferItem.data.local_path)
    else: 
      self.ui.local_path.setText(" ")
    
class WorkflowGraphView(QtGui.QWidget):
  
  def __init__(self, controler, connection, parent = None):
    super(WorkflowGraphView, self).__init__(parent)
    self.ui = Ui_GraphWidget()
    self.ui.setupUi(self)
    
    self.controler = controler
    
    self.workflow = None
    self.connection = None
    
    self.image_label = QtGui.QLabel(self)
    self.image_label.setBackgroundRole(QtGui.QPalette.Base)
    self.image_label.setSizePolicy(QtGui.QSizePolicy.Ignored, QtGui.QSizePolicy.Ignored)
    self.image_label.setScaledContents(True)
    
    self.ui.scrollArea.setBackgroundRole(QtGui.QPalette.Dark)
    #self.ui.scrollArea.setWidget(self.image_label)
    self.ui.scrollArea.setWidgetResizable(False)
    
    self.ui.zoom_slider.setRange(10, 200)
    self.ui.zoom_slider.sliderMoved.connect(self.zoomChanged)
    self.ui.zoom_slider.setValue(100)
    self.scale_factor = 1.0
    
    self.ui.adjust_size_checkBox.stateChanged.connect(self.adjustSizeChanged)
                                        
  def setWorkflow(self, workflow, connection):
    self.workflow = workflow
    self.connection = connection
    self.dataChanged()
    
  def clear(self):
    self.workflow = None
    self.dataChanged()
    
  @QtCore.pyqtSlot(int)
  def zoomChanged(self, percentage):
    self.scale_factor = percentage / 100.0
    if self.workflow:
      self.image_label.resize(self.image_label.pixmap().size()*self.scale_factor)
    
  @QtCore.pyqtSlot(int)
  def adjustSizeChanged(self, state):
    if self.ui.adjust_size_checkBox.isChecked():
      pass
      # TBI
    
  @QtCore.pyqtSlot()
  def dataChanged(self):
    if False: #self.workflow:
      image_file_path = self.controler.printWorkflow(self.workflow, self.connection)
      image = QtGui.QImage(image_file_path)
      pixmap = QtGui.QPixmap.fromImage(image)
      self.image_label.setPixmap(pixmap)
      self.ui.scrollArea.setWidget(self.image_label)
      self.image_label.resize(self.image_label.pixmap().size()*self.scale_factor)
    else:
      self.ui.scrollArea.takeWidget()
    
class WorkflowItemModel(QtCore.QAbstractItemModel):
  
  def __init__(self, client_workflow, parent=None):
    '''
    @type client_workflow: L{ClientWorkflow}
    '''
    super(WorkflowItemModel, self).__init__(parent)
    self.workflow = client_workflow 
    
    self.standby_icon=GRAY
    self.transfer_ready_icon=BLUE
    self.failed_icon=RED
    self.running_icon=GREEN
    self.done_icon=LIGHT_BLUE
    
    #self.standby_icon = QtGui.QIcon('/volatile/laguitton/build-dir/python/soma/jobs/gui/icons/hourglass.png')
    #self.failed_icon=QtGui.QIcon('/volatile/laguitton/build-dir/python/soma/jobs/gui/icons/abort.png')
    #self.running_icon=QtGui.QIcon('/volatile/laguitton/build-dir/python/soma/jobs/gui/icons/forward.png')
    #self.done_icon=QtGui.QIcon('/volatile/laguitton/build-dir/python/soma/jobs/gui/icons/ok.png')
    
  def index(self, row, column, parent=QtCore.QModelIndex()):
    ##print " " 
    ##print ">>> index " + repr(row) + " " + repr(column) 
    if row < 0 or not column == 0:
      ##print "<<< index result QtCore.QModelIndex()"
      return QtCore.QModelIndex()
    
    if not parent.isValid():
      if row < len(self.workflow.root_item.children):
        ##print " index result " + self.workflow.items[self.workflow.root_item.children[row]].data.name + "  row:" + repr(row)
        return self.createIndex(row, column, self.workflow.items[self.workflow.root_item.children[row]])
    else:
      parent_item = parent.internalPointer()
      #print " parent " + parent_item.data.name
      if row < len(parent_item.children):
        #print " index result " + self.workflow.items[parent_item.children[row]].data.name + " row:" + repr(row) 
        return self.createIndex(row, column, self.workflow.items[parent_item.children[row]])
      
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
      parent_item = self.workflow.items[item.parent]
      #print "<<< parent " + parent_item.data.name + " row: " + repr(parent_item.row)
      return self.createIndex(parent_item.row, 0, self.workflow.items[item.parent])
    
    #print "<<< parent QtCore.QModelIndex()"
    return QtCore.QModelIndex()

  def rowCount(self, parent):
    #print " " 
    #print ">>> rowCount"
    if not parent.isValid():
      #print "    parent root_item" 
      #print "<<< rowCount : " + repr(len(self.workflow.root_item.children))
      return len(self.workflow.root_item.children)
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
      children_nb = len(self.workflow.root_item.children)
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
    
    #if not item.initiated:
      # WIP
      
    #### Groups ####
    if isinstance(item,ClientGroup):
      if role == QtCore.Qt.FontRole:
        font = QtGui.QFont()
        font.setBold(True)
        return font
      if role == QtCore.Qt.DisplayRole:
        #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name
        return item.data.name
      else:
        #if item.state == ClientGroup.GP_NOT_SUBMITTED:
          #if role == QtCore.Qt.DecorationRole:
            ##print "<<<< data QtCore.Qt.DecorationRole GRAY"
            #return self.standby_icon
            ##return GRAY
        if item.state == ClientGroup.GP_DONE:
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole LIGHT_BLUE"
            return self.done_icon
            #return LIGHT_BLUE
        if item.state == ClientGroup.GP_FAILED:
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole RED"
            return self.failed_icon
            #return RED
        if item.state == ClientGroup.GP_RUNNING:
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole GREEN"
            return self.running_icon
            #return GREEN
    
    #### JobTemplates ####
    if isinstance(item, ClientJob): 
      if item.data.job_id == -1:
        if role == QtCore.Qt.DisplayRole:
          #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name
          return item.data.name
      else:
        status = item.status
        #status = self.jobs.status(item.data.job_id)
        # not submitted
        if status == NOT_SUBMITTED:
          if role == QtCore.Qt.DisplayRole:
            #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name
            return item.data.name
          if role == QtCore.Qt.DecorationRole:
            #print "<<<< data QtCore.Qt.DecorationRole GRAY"
            return QtGui.QIcon() #self.standby_icon
            #return GRAY
        # Done or Failed
        if status == DONE or status == FAILED:
          #exit_status, exit_value, term_signal, resource_usage = self.jobs.exitInformation(item.data.job_id)
          exit_status, exit_value, term_signal, resource_usage = item.exit_info
          if role == QtCore.Qt.DisplayRole:
            #print "<<<< data QtCore.Qt.DisplayRole " + item.data.name + " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
            return item.data.name #+ " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
          if role == QtCore.Qt.DecorationRole:
            if status == DONE and exit_status == FINISHED_REGULARLY and exit_value == 0:
              #print "<<<< data QtCore.Qt.DecorationRole LIGHT_BLUE"
              return self.done_icon
              #return LIGHT_BLUE
            else:
              #print "<<<< data QtCore.Qt.DecorationRole RED"
              return self.failed_icon
              #return RED
          
        # Running
        if role == QtCore.Qt.DisplayRole:
          #print "<<<< data QtCore.Qt.DisplayRole" + item.data.name + " running..."
          return item.data.name + " " + status #" running..."
        if role == QtCore.Qt.DecorationRole:
          #print "<<<< data QtCore.Qt.DecorationRole GREEN"
          return self.running_icon
          #return GREEN
        
    #### FileTransfers ####
    if isinstance(item, ClientFileTransfer):
      if isinstance(item, ClientInputFileTransfer):
        #if role == QtCore.Qt.BackgroundRole:
          ##print "<<<< data QtCore.Qt.BackgroundRole QtGui.QBrush(QtGui.QColor(200, 200, 255))"
          #return QtGui.QBrush(QtGui.QColor(255, 200, 200))
        if role == QtCore.Qt.ForegroundRole:
          #print "<<<< data QtCore.Qt.ForegroundRole QtGui.QBrush(RED)"
          return QtGui.QBrush(RED)
        display = "input: " + item.data.name
      if isinstance(item, ClientOutputFileTransfer):
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
        status = item.transfer_status
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
    
########################################################
#######################   MODEL   ######################
########################################################

class ClientModel(QtCore.QObject):
  '''
  Model for the client side of soma.jobs. This model was created to provide faster
  GUI minimizing communications with the server.
  The instances of this class hold the connections and the ClientWorkflow instances 
  created in the current session of the GUI.
  The current workflow is periodically updated and the signal data_changed is
  emitted if necessary.
  '''
  
  def __init__(self, parent = None):
    
    super(ClientModel, self).__init__(parent)

    
    self.connections = {} # ressource_id => connection
    self.workflows = {} # client_workflows # resource_id => workflow_id => ClientWorkflow
    self.expiration_dates = {} # resource_id => workflow_ids => expiration_dates
        
    self.current_connection = None
    self.current_resource_id = None
    self.current_workflow = None
    self.current_wf_id = -1
    self.expiration_date = None
    
    self.update_interval = 3 # update period in seconds
    self.auto_update = True
    self.hold = False
    
    def updateLoop(self):
      # update only the current workflows
      while True:
        if not self.hold and self.auto_update and self.current_workflow and self.current_workflow.updateState(): 
          self.emit(QtCore.SIGNAL('workflow_state_changed()'))
        time.sleep(self.update_interval)
    
    self.__update_thread = threading.Thread(name = "ClientModelUpdateLoop",
                                           target = updateLoop,
                                           args = ([self]))
    self.__update_thread.setDaemon(True)
    self.__update_thread.start()
   
   
  def addConnection(self, resource_id, connection):
    '''
    Adds a connection and use it as the current connection
    '''
    self.connections[resource_id] = connection
    self.workflows[resource_id] = {}
    self.expiration_dates[resource_id] = {}
    self.current_resource_id = resource_id
    self.current_connection = connection
    self.emit(QtCore.SIGNAL('current_connection_changed()'))
    
  def setCurrentConnection(self, resource_id):
    if resource_id != self.current_resource_id:
      assert(resource_id in self.connections.keys())
      self.current_resource = resource_id
      self.current_connection = self.connections[resource_id]
      self.emit(QtCore.SIGNAL('current_connection_changed()'))
    
  def addWorkflow(self, workflow, expiration_date):
    '''
    Build a ClientWorkflow from a soma.jobs.jobClient.Worklfow and 
    use it as the current workflow. 
    @type worklfow: soma.jobs.jobClient.Workflow
    '''
    self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
    self.hold = True 
    self.current_workflow = ClientWorkflow(workflow, self.current_connection)
    self.current_wf_id = self.current_workflow.wf_id
    self.expiration_date = expiration_date
    if self.current_wf_id != -1:
      self.workflows[self.current_resource_id][self.current_workflow.wf_id] = self.current_workflow
      self.expiration_dates[self.current_resource_id][self.current_workflow.wf_id] = self.expiration_date
    self.current_workflow.updateState()
    self.emit(QtCore.SIGNAL('current_workflow_changed()'))
    self.hold = False
    
  def deleteWorkflow(self):
    self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
    if self.current_workflow and self.current_workflow.wf_id in self.workflows.keys():
      del self.workflows[wf_id]
      del self.expiration_dates[wf_id]
    self.current_workflow = None
    self.current_wf_id = -1
    self.expiration_date = datetime.now()
    
    self.emit(QtCore.SIGNAL('current_workflow_changed()'))
    
  def clearCurrentWorkflow(self):
    if self.current_workflow != None or  self.current_wf_id != -1:
      self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
      self.current_workflow = None
      self.current_wf_id = -1
      self.expiration_date = datetime.now()
      self.emit(QtCore.SIGNAL('current_workflow_changed()'))
    
  def setCurrentWorkflow(self, wf_id):
    if wf_id != self.current_wf_id:
      assert(wf_id in self.workflows[self.current_resource_id].keys())
      self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
      self.current_wf_id = wf_id
      self.current_workflow = self.workflows[self.current_resource_id][self.current_wf_id]
      self.expiration_date = self.expiration_dates[self.current_resource_id][self.current_wf_id]
      self.emit(QtCore.SIGNAL('current_workflow_changed()'))
      
  def changeExpirationDate(self, date):
     self.expiration_date = date 
     self.expiration_dates[self.current_resource_id][self.current_workflow.wf_id] = self.expiration_date

  def isLoadedWorkflow(self, wf_id):
    return wf_id in self.workflows[self.current_resource_id].keys()
  
  

    
class ClientWorkflow(object):
  
  def __init__(self, workflow, connection = None):
    '''
    Creates a ClientWorkflow from a soma.job.jobClient.Workflow.
    '''
    
    self.connection = connection
    
    self.name = workflow.name 
    self.wf_id = workflow.wf_id
    
    self.ids = {} # ids => {workflow element: sequence of ids}
    self.root_id = -1 # id of the root node
    self.items = {} # items => {id : WorkflowItem}
    self.root_item = None
   
    id_cnt = 0  # unique id for the items
    
    self.server_workflow = workflow 
    self.server_jobs = {} # server job id => client job id
    self.server_file_transfers = {} # server file path => client transfer id
    
    #print " ==> building the workflow "
    #begining = datetime.now()
    # retrieving the set of job and the set of file transfers
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
    
    # Processing the Jobs to create the corresponding ClientJob instances
    for job in w_js:
      item_id = id_cnt
      id_cnt = id_cnt + 1
      self.server_jobs[job.job_id] = item_id
      self.ids[job] = item_id
      self.items[item_id] = ClientJob(it_id = item_id, 
                                      parent = -1, 
                                      row = -1, 
                                      data = job, 
                                      children_nb = len(job.referenced_input_files)+len(job.referenced_output_files))
      for ft in job.referenced_input_files:
        if isinstance(ft, FileTransfer): w_fts.add(ft)
      for ft in job.referenced_output_files:
        if isinstance(ft, FileTransfer): w_fts.add(ft)
      
      
    # Create the ClientGroup instances
    self.root_item = ClientGroup( self, 
                                  it_id = -1, 
                                  parent = -1, 
                                  row = -1, 
                                  data = workflow.mainGroup, 
                                  children_nb = len(workflow.mainGroup.elements))
                                       
    
    for group in workflow.groups:
      item_id = id_cnt
      id_cnt = id_cnt + 1
      self.ids[group] = item_id
      self.items[item_id] =  ClientGroup( self,
                                          it_id = item_id, 
                                          parent = -1, 
                                          row = -1, 
                                          data = group, 
                                          children_nb = len(group.elements))
    
    # parent and children research for jobs and groups
    for item in self.items.values():
      if isinstance(item, ClientGroup) or isinstance(item, ClientJob):
        if item.data in workflow.mainGroup.elements:
          item.parent = -1
          item.row = workflow.mainGroup.elements.index(item.data)
          self.root_item.children[item.row]=item.it_id
        for group in workflow.groups:
          if item.data in group.elements:
            item.parent = self.ids[group]
            item.row = group.elements.index(item.data)
            self.items[item.parent].children[item.row]=item.it_id
    
    # processing the file transfers
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
          self.server_file_transfers[ft.local_path] = item_id
          self.ids[ft].append(item_id)
          if ft in ref_in:
            row = ref_in.index(ft)
          else: 
            row = ref_in.index(ft.local_path)
          self.items[item_id] = ClientInputFileTransfer(  it_id = item_id, 
                                                          parent=self.ids[job], 
                                                          row = row, 
                                                          data = ft)
          self.items[self.ids[job]].children[row]=item_id
        if ft in ref_out or ft.local_path in ref_out:
          item_id = id_cnt
          id_cnt = id_cnt + 1
          self.server_file_transfers[ft.local_path] = item_id
          self.ids[ft].append(item_id)
          if ft in ref_out:
            row = len(ref_in)+ref_out.index(ft)
          else:
            row = len(ref_in)+ref_out.index(ft.local_path)
          self.items[item_id] = ClientOutputFileTransfer( it_id = item_id, 
                                                          parent=self.ids[job], 
                                                          row = row, 
                                                          data = ft)
          self.items[self.ids[job]].children[row]=item_id
          
    #end = datetime.now() - begining
    #print " <== end building worflow " + repr(end.seconds)
                                  
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
    
  def updateState(self):
    if self.wf_id == -1: 
      return False
    data_changed = False
    
    #print " ==> communication with the server " + repr(self.server_workflow.wf_id)
    #begining = datetime.now()
    
    wf_status = self.connection.workflowStatus(self.server_workflow.wf_id)
    
    #end = datetime.now() - begining
    #print " <== end communication" + repr(self.server_workflow.wf_id) + " : " + repr(end.seconds)
    
    #print " ==> updating jobs " + repr(self.server_workflow.wf_id)
    #begining = datetime.now()
    
    #updating jobs:
    for job_info in wf_status[0]:
      job_id, status, exit_info = job_info
      item = self.items[self.server_jobs[job_id]]
      data_changed = item.updateState(status, exit_info) or data_changed

    #end = datetime.now() - begining
    #print " <== end updating jobs" + repr(self.server_workflow.wf_id) + " : " + repr(end.seconds)
    
    #print " ==> updating transfers " + repr(self.server_workflow.wf_id)
    #begining = datetime.now()
    
    #updating file transfer
    for transfer_info in wf_status[1]:
      local_file_path, status = transfer_info 
      item = self.items[self.server_file_transfers[local_file_path]]
      data_changed = item.updateState(status) or data_changed
    
    #end = datetime.now() - begining
    #print " <== end updating transfers" + repr(self.server_workflow.wf_id) + " : " + repr(end.seconds) + " " + repr(data_changed)
    
    
    #updateing groups 
    self.root_item.updateState()
    
    return data_changed
        

class ClientWorkflowItem(object):
  '''
  Abstract class for workflow items.
  '''
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0):
    '''
    @type  connection: soma.jobs.jobClient.job
    @param connection: jobs interface 
    '''
    
    self.it_id = it_id
    self.parent = parent # parent_id
    self.row = row
    self.data = data
    self.children = [-1 for i in range(children_nb)]   
    
    self.initiated = False

class ClientGroup(ClientWorkflowItem):
  
  GP_NOT_SUBMITTED = "not_submitted"
  GP_DONE = "done"
  GP_FAILED = "failed"
  GP_RUNNING = "running"
  
  def __init__(self,
               client_workflow,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0):
    super(ClientGroup, self).__init__(it_id, parent, row, data, children_nb)  
    
    self.client_workflow = client_workflow
    
    self.achievement = 0 # % of achievement
    self.state = ClientGroup.GP_NOT_SUBMITTED
    self.total_children_nb = 0 # will be computed at the first update
    
    # TO DO => state % of achievement
    
  def updateState(self):
    self.initiated = True
    state_changed = False
    
    self.total_children_nb = len(self.children)
    self.not_sub_nb = 0
    self.done_nb = 0
    self.failed_nb = 0
    self.running_nb = 0
    
    for child in self.children:
      item = self.client_workflow.items[child]
      if isinstance(item, ClientJob):
        if item.status == NOT_SUBMITTED:
          self.not_sub_nb = self.not_sub_nb +1
        elif item.status == DONE or item.status == FAILED:
          exit_status, exit_value, term_signal, resource_usage = item.exit_info
          if item.status == DONE and exit_status == FINISHED_REGULARLY and exit_value == 0:
            self.done_nb = self.done_nb +1
          else:
            self.failed_nb = self.failed_nb +1
        else:
          self.running_nb = self.running_nb +1
      if isinstance(item, ClientGroup):
        item.updateState()
        self.total_children_nb = self.total_children_nb + item.total_children_nb
        if item.state == ClientGroup.GP_NOT_SUBMITTED:
          self.not_sub_nb = self.not_sub_nb + 1
        if item.state == ClientGroup.GP_DONE:
          self.done_nb = self.done_nb +1
        if item.state == ClientGroup.GP_FAILED:
          self.failed_nb = self.failed_nb +1
        if item.state == ClientGroup.GP_RUNNING:
          self.running_nb = self.running_nb +1
          
    #print 'group ' + self.data.name
    #print ' failed_nb ' + repr(self.failed_nb)
    #print ' done_nb ' + repr(self.done_nb)
    #print ' not_sub_nb ' + repr(self.not_sub_nb)
    #print ' running_nb ' + repr(self.running_nb)
    #print ' '
          
    if self.failed_nb != 0:
      new_state = ClientGroup.GP_FAILED
    elif self.done_nb == len(self.children):
      new_state = ClientGroup.GP_DONE
    elif self.not_sub_nb == len(self.children):
      new_state = ClientGroup.GP_NOT_SUBMITTED
    else:
      new_state = ClientGroup.GP_RUNNING
        
    state_changed = self.state != new_state
    self.state = new_state
    return state_changed

class ClientJob(ClientWorkflowItem):
  
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               it_type = None,
               data = None,
               children_nb = 0 ):
    super(ClientJob, self).__init__(it_id, parent, row, data, children_nb)
    
    self.status = "not submitted"
    self.exit_info = ("", "", "", "")
    self.stdout = ""
    self.stderr = ""
    
    cmd_seq = []
    for command_el in data.command:
      if isinstance(command_el, FileTransfer):
        cmd_seq.append(command_el.remote_path)
      else:
        cmd_seq.append(repr(command_el))
    separator = " " 
    self.command = separator.join(cmd_seq)
    
  def updateState(self, status, exit_info):
    self.initiated = True
    state_changed = False
    state_changed = self.status != status or state_changed
    self.status = status
    state_changed = self.exit_info != exit_info or state_changed
    self.exit_info = exit_info
    
    return state_changed
    
  def updateStdout(self, connection):
    if self.data:
      connection.resertStdReading()
      line = connection.stdoutReadLine(self.data.job_id)
      stdout = ""
      while line:
        stdout = stdout + line + "\n"
        line = connection.stdoutReadLine(self.data.job_id)
      self.stdout = stdout
      
  def updateStderr(self, connection):
    if self.data:
      connection.resertStdReading()
      line = connection.stderrReadLine(self.data.job_id)
      stderr = ""
      while line:
        stderr = stderr + line + "\n"
        line = connection.stderrReadLine(self.data.job_id)
      self.stderr = stderr
      
class ClientFileTransfer(ClientWorkflowItem):
  
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0 ):
    super(ClientFileTransfer, self).__init__(it_id, parent, row, data, children_nb)
    self.transfer_status = " "
    
  def updateState(self, transfer_status):
    
    self.initiated = True
    state_changed = False
    
    state_changed = state_changed or transfer_status != self.transfer_status
    self.transfer_status = transfer_status

    return state_changed
    
class ClientOutputFileTransfer(ClientFileTransfer):
  
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0 ):
    super(ClientOutputFileTransfer, self).__init__(it_id, parent, row, data, children_nb)
    

class ClientInputFileTransfer(ClientFileTransfer):
    
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0 ):
    super(ClientInputFileTransfer, self).__init__(it_id, parent, row, data, children_nb)
    


    
    
    
    
    