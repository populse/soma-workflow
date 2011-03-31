from __future__ import with_statement
'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import time
import threading
import os
from datetime import date
from datetime import datetime
from datetime import timedelta

from PyQt4 import QtGui, QtCore
from PyQt4 import uic
import matplotlib
matplotlib.use('Qt4Agg')
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure 

from soma.workflow.client import Workflow, Group, FileTransfer, SharedResourcePath, Job, WorkflowController, Helper
from soma.workflow.engine_types import EngineWorkflow, EngineJob, EngineTransfer
from soma.workflow.constants import *
from soma.workflow.configuration import Configuration
from soma.workflow.test.test_workflow import WorkflowExamples
from soma.workflow.errors import UnknownObjectError, ConfigurationError, SerializationError


class PyroError(Exception):     pass
class ProtocolError(PyroError): pass
class ConnectionClosedError(ProtocolError): pass


#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------



GRAY=QtGui.QColor(200, 200, 180)
BLUE=QtGui.QColor(0,200,255)
RED=QtGui.QColor(255,100,50)
GREEN=QtGui.QColor(155,255,50)
LIGHT_BLUE=QtGui.QColor(200,255,255)

Ui_WorkflowMainWindow = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                 'WorkflowMainWindow.ui' ))[0]

Ui_JobInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                            'JobInfo.ui' ))[0]

Ui_GraphWidget = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                        'graphWidget.ui' ))[0]

Ui_PlotWidget = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                         'PlotWidget.ui' ))[0]

Ui_TransferInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                       'TransferInfo.ui' ))[0]

Ui_GroupInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                          'GroupInfo.ui' ))[0]

Ui_ConnectionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                      'connectionDlg.ui' ))[0]

Ui_FirstConnectionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 
                                                 'firstConnectionDlg.ui' ))[0]

Ui_WorkflowExampleDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ),  
                                                 'workflowExampleDlg.ui' ))[0]

Ui_SubmissionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ),  
                                                      'submissionDlg.ui' ))[0]


#-----------------------------------------------------------------------------
# Local utilities
#-----------------------------------------------------------------------------


def setLabelFromString(label, value):
  '''
  @type value: string
  '''
  if value:
    label.setText(unicode(value.encode('utf-8')))
  else:
    label.setText("")
    
def setLabelFromInt(label, value):
  '''
  @type value: int
  '''
  if not value == None:
    label.setText(repr(value))
  else:
    label.setText("")

def setLabelFromTimeDelta(label, value):
  '''
  @type value: datetime.timedelta
  '''
  if not value == None:
    hours = value.seconds//3600
    mins = (value.seconds%3600)//60
    seconds = (value.seconds%3600)%60
    hours = hours + value.days * 24
    label.setText("%s:%s:%s" %(repr(hours), repr(mins), repr(seconds)))
  else:
    label.setText("")


def setLabelFromDateTime(label, value):
  '''
  @type value: datetime.datetime
  '''
  if not value == None:
    datetime = QtCore.QDateTime(value)
    label.setText(datetime.toString("dd/MM/yy HH:mm:ss"))
  else:
    label.setText("")
  

#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------


class Controller(object):
  @staticmethod
  def delete_workflow(wf_id, 
                      wf_ctrl):
    return wf_ctrl.delete_workflow(wf_id)
  
  @staticmethod
  def change_workflow_expiration_date(wf_id, 
                                      date, 
                                      wf_ctrl):
    return wf_ctrl.change_workflow_expiration_date(wf_id, date)

  @staticmethod
  def get_submitted_workflows(wf_ctrl):
    return wf_ctrl.workflows()

  @staticmethod
  def submit_workflow(workflow, 
                      name, 
                      expiration_date,
                      queue,
                      wf_ctrl):
    wf_id = wf_ctrl.submit_workflow(workflow=workflow,
                                    expiration_date=expiration_date,
                                    name=name,
                                    queue=queue) 
    wf = wf_ctrl.workflow(wf_id)
    return wf

  @staticmethod
  def restart_workflow(workflow_id, wf_ctrl):
    return wf_ctrl.restart_workflow(workflow_id)

  @staticmethod
  def get_connection(resource_id, 
                    login, 
                    password,
                    rsa_key_pass):
    wf_ctrl = WorkflowController(resource_id=resource_id, 
                                 login=login, 
                                 password=password,
                                 rsa_key_pass=rsa_key_pass)
    return wf_ctrl

  @staticmethod
  def serialize_workflow(file_path, workflow):
    Helper.serialize(file_path, workflow)

  @staticmethod
  def unserialize_workflow(file_path):
    workflow = Helper.unserialize(file_path)
    return workflow

  @staticmethod
  def get_queues(wf_ctrl):
    return wf_ctrl.config.get_queues()

  @staticmethod
  def submit_workflow(worklfow, expiration_date, name, queue, wf_ctrl):
    wf_id = wf_ctrl.submit_workflow( 
                          workflow=worklfow,
                          expiration_date=expiration_date,
                          name=name,
                          queue=queue) 
    workflow = wf_ctrl.workflow(wf_id)
    return workflow

  @staticmethod
  def transfer_input_files(workflow, wf_ctrl, buffer_size=256**2):
    Helper.transfer_input_files(workflow, wf_ctrl, buffer_size)

  @staticmethod
  def transfer_output_files(workflow, wf_ctrl, buffer_size= 256**2):
    Helper.transfer_output_files(workflow, wf_ctrl, buffer_size)


class WorkflowWidget(QtGui.QMainWindow):
  
  def __init__(self, model, parent=None, flags=0):
    super(WorkflowWidget, self).__init__(parent)
    
    self.ui = Ui_WorkflowMainWindow()
    self.ui.setupUi(self)
    
    self.setCorner(QtCore.Qt.BottomLeftCorner, QtCore.Qt.LeftDockWidgetArea)
    self.setCorner(QtCore.Qt.BottomRightCorner, QtCore.Qt.RightDockWidgetArea)
    
    self.tabifyDockWidget(self.ui.dock_graph, self.ui.dock_plot)
    
    self.model = model
    
    self.connect(self.model, QtCore.SIGNAL('current_connection_changed()'), self.currentConnectionChanged)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_changed()'),  self.currentWorkflowChanged)
    self.connect(self.model, QtCore.SIGNAL('connection_closed_error()'), self.reconnectAfterConnectionClosed)
    self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'),
    self.updateCurrentWorkflowStatus)

    try:
      self.config_file_path = Configuration.search_config_path()
      self.resource_list = Configuration.get_configured_resources(self.config_file_path)
    except ConfigurationError, e:
      QtGui.QMessageBox.critical(self, "Configuration problem", "%s" %(e))
      self.close()

    self.ui.combo_resources.addItems(self.resource_list)
    
    self.setWindowTitle("soma-workflow")
   
    self.treeWidget = WorkflowTree(self.model, self)
    treeWidgetLayout = QtGui.QVBoxLayout()
    treeWidgetLayout.setContentsMargins(2,2,2,2)
    treeWidgetLayout.addWidget(self.treeWidget)
    self.ui.centralwidget.setLayout(treeWidgetLayout)

    self.connect(self.treeWidget, QtCore.SIGNAL('selection_model_changed()'), self.selection_model_changed)
   
    self.itemInfoWidget = WorkflowElementInfo(self.model, self)
    itemInfoLayout = QtGui.QVBoxLayout()
    itemInfoLayout.setContentsMargins(2,2,2,2)
    itemInfoLayout.addWidget(self.itemInfoWidget)
    self.ui.dockWidgetContents_intemInfo.setLayout(itemInfoLayout)

    self.connect(self.itemInfoWidget, QtCore.SIGNAL('connection_closed_error()'), self.reconnectAfterConnectionClosed)

    self.workflowInfoWidget = WorkflowInfo(self.model, self)
    wfInfoLayout = QtGui.QVBoxLayout()
    wfInfoLayout.setContentsMargins(2,2,2,2)
    wfInfoLayout.addWidget(self.workflowInfoWidget)
    self.ui.widget_wf_info.setLayout(wfInfoLayout)
    
    #self.graphWidget = WorkflowGraphView(self)
    #graphWidgetLayout = QtGui.QVBoxLayout()
    #graphWidgetLayout.setContentsMargins(2,2,2,2)
    #graphWidgetLayout.addWidget(self.graphWidget)
    #self.ui.dockWidgetContents_graph.setLayout(graphWidgetLayout)
    
    self.workflowPlotWidget = WorkflowPlot(self.model, self)
    plotLayout = QtGui.QVBoxLayout()
    plotLayout.setContentsMargins(2,2,2,2)
    plotLayout.addWidget(self.workflowPlotWidget)
    self.ui.dockWidgetContents_plot.setLayout(plotLayout)
    
    self.ui.widget_wf_status_date.hide()
    self.ui.widget_wf_info.hide()
    
    self.ui.toolButton_button_delete_wf.setDefaultAction(self.ui.action_delete_workflow)
    self.ui.toolButton_change_exp_date.setDefaultAction(self.ui.action_change_expiration_date)
    
    self.ui.action_submit.triggered.connect(self.submit_workflow)
    self.ui.action_transfer_infiles.triggered.connect(self.transferInputFiles)
    self.ui.action_transfer_outfiles.triggered.connect(self.transferOutputFiles)
    self.ui.action_open_wf.triggered.connect(self.openWorkflow)
    self.ui.action_create_wf_ex.triggered.connect(self.createWorkflowExample)
    self.ui.action_delete_workflow.triggered.connect(self.delete_workflow)
    self.ui.action_change_expiration_date.triggered.connect(self.changeExpirationDate)
    self.ui.action_save.triggered.connect(self.saveWorkflow)
    self.ui.action_restart.triggered.connect(self.restart_workflow)
    
    self.ui.list_widget_submitted_wfs.itemSelectionChanged.connect(self.workflowSelectionChanged)
    self.ui.combo_resources.currentIndexChanged.connect(self.resourceSelectionChanged)

    self.ui.wf_list_refresh_button.clicked.connect(self.refreshWorkflowList)
    
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
    if self.ui_firstConnection_dlg.lineEdit_rsa_password.text():
      rsa_key_pass = unicode(self.ui_firstConnection_dlg.lineEdit_rsa_password.text()).encode('utf-8')
    else:
      rsa_key_pass = None

    wf_ctrl = None
    try:
      wf_ctrl = Controller.get_connection(resource_id, 
                                          login, 
                                          password,
                                          rsa_key_pass)
    except ConfigurationError, e:
      QtGui.QMessageBox.critical(self, "Configuration problem", "%s" %(e))
      self.ui_firstConnection_dlg.lineEdit_password.clear()
      self.firstConnection_dlg.show()
    except Exception, e:
      QtGui.QMessageBox.critical(self, "Connection failed", "%s" %(e))
      self.ui_firstConnection_dlg.lineEdit_password.clear()
      self.firstConnection_dlg.show()
    else:
      self.model.addConnection(resource_id, wf_ctrl)
      self.firstConnection_dlg.hide()
    
  @QtCore.pyqtSlot()
  def openWorkflow(self):
    file_path = QtGui.QFileDialog.getOpenFileName(self, "Open a workflow");
    if file_path:
      try:
        workflow = Controller.unserialize_workflow(file_path)
      except SerializationError, e: 
        QtGui.QMessageBox.warning(self, "Error opening the workflow", "%s" %(e))
      else:
        self.updateWorkflowList()
        self.model.addWorkflow(workflow, datetime.now() + timedelta(days=5))
      
  @QtCore.pyqtSlot()
  def saveWorkflow(self):
    file_path = QtGui.QFileDialog.getSaveFileName(self, "Save the current workflow")
    if file_path:
      try:
        Controller.serialize_workflow(file_path, self.model.current_workflow)
      except SerializationError, e:
        QtGui.QMessageBox.warning(self, "Error", "%s: %s" %(type(e),e))
        
    
  @QtCore.pyqtSlot()
  def createWorkflowExample(self):
    worflowExample_dlg = QtGui.QDialog(self)
    ui = Ui_WorkflowExampleDlg()
    ui.setupUi(worflowExample_dlg)
    ui.comboBox_example_type.addItems(WorkflowExamples.get_workflow_example_list())
    if worflowExample_dlg.exec_() == QtGui.QDialog.Accepted:
      with_file_transfer = ui.checkBox_file_transfers.checkState() == QtCore.Qt.Checked
      with_shared_resource_path = ui.checkBox_shared_resource_path.checkState() == QtCore.Qt.Checked
      example_type = ui.comboBox_example_type.currentIndex()
      file_path = QtGui.QFileDialog.getSaveFileName(self, 
                                                    "Create a workflow example")
      if file_path:
        try:
          wf_examples = WorkflowExamples(with_file_transfer,
                                  with_shared_resource_path)
        except ConfigurationError, e:
          QtGui.QMessageBox.warning(self, "Error", "%s" %(e))
        else:
          workflow = wf_examples.get_workflow_example(example_type)
          try:
            Controller.serialize_workflow(file_path, workflow)
          except SerializationError, e:
            QtGui.QMessageBox.warning(self, "Error", "%s" %(e))
        
  @QtCore.pyqtSlot()
  def submit_workflow(self):
    assert(self.model.current_workflow)
    
    submission_dlg = QtGui.QDialog(self)
    ui = Ui_SubmissionDlg()
    ui.setupUi(submission_dlg)
    setLabelFromString(ui.resource_label, self.model.current_resource_id)
    ui.resource_label.setText(self.model.current_resource_id)
    ui.lineedit_wf_name.setText("")
    ui.dateTimeEdit_expiration.setDateTime(datetime.now() + timedelta(days=5))
    
    queues = ["default queue"]
    queues.extend(Controller.get_queues(self.model.current_connection))
    ui.combo_queue.addItems(queues)

    if submission_dlg.exec_() == QtGui.QDialog.Accepted:
      name = unicode(ui.lineedit_wf_name.text())
      if name == "": name = None
      qtdt = ui.dateTimeEdit_expiration.dateTime()
      date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                      qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
      queue =  unicode(ui.combo_queue.currentText()).encode('utf-8')
      if queue == "default queue": queue = None
      while True:
        try:
          workflow = Controller.submit_workflow(
                            self.model.current_workflow.server_workflow, 
                            date, 
                            name, 
                            queue, 
                            self.model.current_connection)
        except ConnectionClosedError, e:
          if not self.reconnectAfterConnectionClosed():
            return
        else:
          break
      self.updateWorkflowList()
      self.model.addWorkflow(workflow, date) 

  @QtCore.pyqtSlot()
  def restart_workflow(self):
    try:
      done = Controller.restart_workflow(self.model.current_workflow.wf_id,
                                         self.model.current_connection)
    except ConnectionClosedError, e:
      pass
    except SystemExit, e:
      pass
    if not done:
      QtGui.QMessageBox.warning(self, 
                                "Restart workflow", 
                                "The workflow is already running.")
    else:
      self.model.restartCurrentWorkflow()
  
  @QtCore.pyqtSlot()
  def transferInputFiles(self):
    def transfer(self):
      try:
        self.ui.action_transfer_infiles.setEnabled(False)
        Controller.transfer_input_files(
                self.model.current_workflow.server_workflow, 
                self.model.current_connection, 
                buffer_size=256**2)
      except ConnectionClosedError, e:
        self.ui.action_transfer_infiles.setEnabled(True)
        pass
      except SystemExit, e:
        pass
      self.ui.action_transfer_infiles.setEnabled(True)
    thread = threading.Thread(name = "TransferInputFiles",
                              target = transfer,
                              args =([self]))
    thread.setDaemon(True)
    thread.start()

  @QtCore.pyqtSlot()
  def transferOutputFiles(self):
    def transfer(self):
      try:
        self.ui.action_transfer_outfiles.setEnabled(False)
        Controller.transfer_output_files(
                self.model.current_workflow.server_workflow, 
                self.model.current_connection, 
                buffer_size=256**2)
      except ConnectionClosedError, e:
        self.ui.action_transfer_outfiles.setEnabled(True)
      except SystemExit, e:
        pass
      self.ui.action_transfer_outfiles.setEnabled(True)
    thread = threading.Thread(name = "TransferOuputFiles",
                              target = transfer,
                              args =([self]))
    thread.setDaemon(True)
    thread.start()
    
  @QtCore.pyqtSlot()
  def workflowSelectionChanged(self):
    selected_items = self.ui.list_widget_submitted_wfs.selectedItems()
    if not selected_items:
      return
    wf_id = selected_items[0].data(QtCore.Qt.UserRole).toInt()[0]
    if wf_id != -1:
      if self.model.isLoadedWorkflow(wf_id):
        self.model.setCurrentWorkflow(wf_id)
      else:
        workflow = self.model.current_connection.workflow(wf_id)
        if workflow:
          expiration_date = self.model.current_connection.workflows([wf_id])[wf_id][1]
        else:
          expiration_date = None
        if workflow != None:
          self.model.addWorkflow(workflow, expiration_date)
        else:
          self.updateWorkflowList()
          self.model.clearCurrentWorkflow()
    else:
      self.model.clearCurrentWorkflow()
    
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
      new_connection = self.createConnection(resource_id)
      if new_connection:
        self.model.addConnection(resource_id, new_connection)

  def createConnection(self, resource_id):
    connection_invalid = True
    try_again = True
    while connection_invalid or try_again: 
      connection_dlg = QtGui.QDialog()
      connection_dlg.setModal(True)
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
      if ui.lineEdit_rsa_password.text():
        rsa_key_pass = unicode(ui.lineEdit_rsa_password.text()).encode('utf-8')
      else:
        rsa_key_pass = None
      try:
        wf_ctrl = Controller.get_connection(resource_id, 
                                            login, 
                                            password,
                                            rsa_key_pass)
      except ConfigurationError, e:
        QtGui.QMessageBox.information(self, "Configuration error", "%s" %(e))
        return None
      except Exception, e:
        QtGui.QMessageBox.information(self, 
                                      "Connection failed", 
                                      "%s: %s" %(type(e),e))
      else:
        return wf_ctrl
    return None

  @QtCore.pyqtSlot()
  def delete_workflow(self):
    assert(self.model.current_workflow and self.model.current_wf_id != -1)
    
    if self.model.current_workflow.name:
      name = self.model.current_workflow.name
    else: 
      name = repr(self.model.current_wf_id)
    
    answer = QtGui.QMessageBox.question(self, "confirmation", "Do you want to delete the workflow " + name +"?", QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
    if answer != QtGui.QMessageBox.Yes: return
    while True:
      try:
        Controller.delete_workflow(self.model.current_workflow.wf_id,
                                   self.model.current_connection)
      except ConnectionClosedError, e:
        if not self.reconnectAfterConnectionClosed():
          return
      else:
        break
    self.updateWorkflowList()
    self.model.delete_workflow()
    
  @QtCore.pyqtSlot()
  def changeExpirationDate(self):
    qtdt = self.ui.dateTimeEdit_expiration.dateTime()
    date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                    qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
    
    while True:
      try:
        change_occured = Controller.change_workflow_expiration_date(
                          self.model.current_wf_id, 
                          date,  
                          self.model.current_connection)
      except ConnectionClosedError, e:
        if not self.reconnectAfterConnectionClosed():
          return
      else:
        break
    if not change_occured:
      QtGui.QMessageBox.information(self, "information", "The workflow expiration date was not changed.")
      self.ui.dateTimeEdit_expiration.setDateTime(self.expiration_date)
    else:
      self.model.changeExpirationDate(date)
      
  
  @QtCore.pyqtSlot()
  def currentConnectionChanged(self):
    self.setWindowTitle("soma-workflow - " + self.model.current_resource_id)
    self.updateWorkflowList()
    self.model.clearCurrentWorkflow()
    index = self.ui.combo_resources.findText(self.model.current_resource_id)
    self.ui.combo_resources.setCurrentIndex(index)
  
  @QtCore.pyqtSlot()
  def selection_model_changed(self):
    self.itemInfoWidget.setSelectionModel(self.treeWidget.tree_view.selectionModel())
      
  @QtCore.pyqtSlot()
  def currentWorkflowChanged(self):
    if not self.model.current_workflow:
      # No workflow
      
      #self.graphWidget.clear()
      self.itemInfoWidget.clear()
      
      self.ui.wf_name.clear()
      self.ui.wf_status.clear()
     
      self.ui.dateTimeEdit_expiration.setDateTime(datetime.now())
      self.ui.dateTimeEdit_expiration.setEnabled(False)
      
      self.ui.action_submit.setEnabled(False)
      self.ui.action_change_expiration_date.setEnabled(False)
      self.ui.action_delete_workflow.setEnabled(False)
      self.ui.action_transfer_infiles.setEnabled(False)
      self.ui.action_transfer_outfiles.setEnabled(False)
      self.ui.action_save.setEnabled(False)
      
      self.ui.widget_wf_status_date.hide()
      self.ui.widget_wf_info.hide()
      
      
    else:
  
      #self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.graphWidget.dataChanged)
      
      #=> TEMPORARY : the graph view has to be built from the guiModel
      #self.graphWidget.setWorkflow(self.model.current_workflow.server_workflow, self.model.current_connection)
      
      
      if self.model.current_wf_id == -1:
        # Workflow not submitted
        if self.model.current_workflow.name:
          self.ui.wf_name.setText(self.model.current_workflow.name)
        else:
          self.ui.wf_name.clear()

        self.ui.wf_status.setText("not submitted")
         
        self.ui.dateTimeEdit_expiration.setDateTime(datetime.now() + timedelta(days=5))
        self.ui.dateTimeEdit_expiration.setEnabled(True)
        
        self.ui.action_submit.setEnabled(True)
        self.ui.action_change_expiration_date.setEnabled(False)
        self.ui.action_delete_workflow.setEnabled(False)
        self.ui.action_transfer_infiles.setEnabled(False)
        self.ui.action_transfer_outfiles.setEnabled(False)
        
        self.ui.widget_wf_status_date.hide()
        self.ui.widget_wf_info.hide()
        
        self.ui.list_widget_submitted_wfs.clearSelection()
        
      else:
        # Submitted workflow
        if self.model.current_workflow.name:
          self.ui.wf_name.setText(self.model.current_workflow.name)
        else: 
          self.ui.wf_name.setText(repr(self.model.current_wf_id))
        
        self.ui.wf_status.setText(self.model.current_workflow.wf_status)

        self.ui.dateTimeEdit_expiration.setDateTime(self.model.expiration_date)
        self.ui.dateTimeEdit_expiration.setEnabled(True)
        
        self.ui.action_submit.setEnabled(False)
        self.ui.action_change_expiration_date.setEnabled(True)
        self.ui.action_delete_workflow.setEnabled(True)
        self.ui.action_transfer_infiles.setEnabled(True)
        self.ui.action_transfer_outfiles.setEnabled(True)    
        self.ui.action_save.setEnabled(True)    
        
        self.ui.widget_wf_status_date.show()
        self.ui.widget_wf_info.show()
        
        index = None
        for i in range(0, self.ui.list_widget_submitted_wfs.count()):
          if self.model.current_wf_id == self.ui.list_widget_submitted_wfs.item(i).data(QtCore.Qt.UserRole).toInt()[0]:
            self.ui.list_widget_submitted_wfs.setCurrentRow(i)
            break

  @QtCore.pyqtSlot()  
  def updateCurrentWorkflowStatus(self):
    self.ui.wf_status.setText(self.model.current_workflow.wf_status)
       
          
  @QtCore.pyqtSlot()  
  def refreshWorkflowList(self):
    self.updateWorkflowList()
    self.model.clearCurrentWorkflow()


  def updateWorkflowList(self):
    
    while True:
      try:
        submittedWorflows = Controller.get_submitted_workflows(self.model.current_connection)
      except ConnectionClosedError, e:
        if not self.reconnectAfterConnectionClosed():
          return
      else:
        break
    
    self.ui.list_widget_submitted_wfs.itemSelectionChanged.disconnect(self.workflowSelectionChanged)
    self.ui.list_widget_submitted_wfs.clear()
    for wf_id, wf_info in submittedWorflows.iteritems():
      workflow_name, expiration_date = wf_info
      if not workflow_name: workflow_name = repr(wf_id)
      item = QtGui.QListWidgetItem(workflow_name, self.ui.list_widget_submitted_wfs)
      item.setData(QtCore.Qt.UserRole, wf_id)
      self.ui.list_widget_submitted_wfs.addItem(item)
    self.ui.list_widget_submitted_wfs.itemSelectionChanged.connect(self.workflowSelectionChanged)
    
    
  @QtCore.pyqtSlot()
  def reconnectAfterConnectionClosed(self):
    answer = QtGui.QMessageBox.question(None, 
                                        "Connection closed",
                                        "The connection to  "+ self.model.current_resource_id +" closed.\n  Do you want to try a reconnection?", 
                                        QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
    if answer == QtGui.QMessageBox.Yes:
      new_connection = self.createConnection(self.model.current_resource_id)
      if new_connection:
        self.model.reinitCurrentConnection(new_connection)
        return True
      else:
        self.model.deleteCurrentConnection()
        return False
    

class WorkflowTree(QtGui.QWidget):

  def __init__(self, model, parent = None):
    super(WorkflowTree, self).__init__(parent)

    self.tree_view = QtGui.QTreeView(self)
    self.tree_view.setHeaderHidden(True)
    self.vLayout = QtGui.QVBoxLayout(self)
    self.vLayout.setContentsMargins(0,0,0,0)
    self.vLayout.addWidget(self.tree_view)

    self.model = model
    self.item_model = None
    
    self.connect(self.model, QtCore.SIGNAL('current_connection_changed()'), self.clear)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_about_to_change()'), self.currentWorkflowAboutToChange)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_changed()'),  self.currentWorkflowChanged)
    self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.dataChanged)
    
  @QtCore.pyqtSlot()
  def clear(self):
    if self.item_model:
      self.item_model.emit(QtCore.SIGNAL("modelAboutToBeReset()"))
      self.item_model = None
      self.tree_view.setModel(None)
      #self.item_model.emit(QtCore.SIGNAL("modelReset()"))

  @QtCore.pyqtSlot()
  def currentWorkflowAboutToChange(self):
    if self.item_model:
      self.item_model.emit(QtCore.SIGNAL("modelAboutToBeReset()"))
      self.item_model = None
      self.tree_view.setModel(None)
    
  @QtCore.pyqtSlot()
  def currentWorkflowChanged(self):
    if self.model.current_workflow:
      self.item_model = WorkflowItemModel(self.model.current_workflow, self)
      self.tree_view.setModel(self.item_model)
      self.item_model.emit(QtCore.SIGNAL("modelReset()"))
      self.emit(QtCore.SIGNAL("selection_model_changed()"))
    
  @QtCore.pyqtSlot()
  def dataChanged(self):
    if self.item_model:
      row = self.item_model.rowCount(QtCore.QModelIndex())
      self.item_model.dataChanged.emit(self.item_model.index(0,0,QtCore.QModelIndex()),
                                       self.item_model.index(row,0,QtCore.QModelIndex()))      
    
      


class WorkflowInfo(QtGui.QWidget):
  
  def __init__(self, model, parent = None):
    super(WorkflowInfo, self).__init__(parent)
    
    self.infoWidget = None
    self.vLayout = QtGui.QVBoxLayout(self)
    self.model = model
    
    self.connect(self.model, QtCore.SIGNAL('current_connection_changed()'), self.clear)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_about_to_change()'), self.clear)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_changed()'),  self.workflowChanged)
    self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.dataChanged)
  
  @QtCore.pyqtSlot()
  def clear(self):
    if self.infoWidget:
      self.infoWidget.hide()
      self.vLayout.removeWidget(self.infoWidget)
    self.infoWidget = None
    self.dataChanged()
    
  @QtCore.pyqtSlot()
  def workflowChanged(self):
    if self.infoWidget:
      self.infoWidget.hide()
      self.vLayout.removeWidget(self.infoWidget)
    if self.model.current_workflow:
      self.infoWidget = GroupInfoWidget(self.model.current_workflow.root_item, self)
    if self.infoWidget:
      self.vLayout.addWidget(self.infoWidget)
    self.update()
    
  @QtCore.pyqtSlot()
  def dataChanged(self):
    if self.infoWidget:
      self.infoWidget.dataChanged()
      
class WorkflowPlot(QtGui.QWidget):
  
  def __init__(self, model, parent = None):
    super(WorkflowPlot, self).__init__(parent)
    
    self.plotWidget = None
    self.vLayout = QtGui.QVBoxLayout(self)
    self.model = model
    
    self.connect(self.model, QtCore.SIGNAL('current_connection_changed()'), self.clear)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_about_to_change()'), self.clear)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_changed()'),  self.workflowChanged)
    self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.dataChanged)
  
  @QtCore.pyqtSlot()
  def clear(self):
    if self.plotWidget:
      self.plotWidget.hide()
      self.vLayout.removeWidget(self.plotWidget)
    self.plotWidget = None
    self.dataChanged()
    
  @QtCore.pyqtSlot()
  def workflowChanged(self):
    if self.plotWidget:
      self.plotWidget.hide()
      self.vLayout.removeWidget(self.plotWidget)
    if self.model.current_workflow:
      self.plotWidget = PlotView(self.model.current_workflow.root_item, self)
    if self.plotWidget:
      self.vLayout.addWidget(self.plotWidget)
    self.update()
    
  @QtCore.pyqtSlot()
  def dataChanged(self):
    if self.plotWidget:
      self.plotWidget.dataChanged()
    
class WorkflowElementInfo(QtGui.QWidget):
  
  def __init__(self, model, parent = None):
    super(WorkflowElementInfo, self).__init__(parent)
    
    self.selectionModel = None
    self.infoWidget = None
    self.model = model # used to update stderr and stdout only
    
    self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.dataChanged) 
    self.connect(self.model, QtCore.SIGNAL('current_connection_changed()'), self.clear) 
    
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
      
  @QtCore.pyqtSlot()
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
    if isinstance(item, GuiJob):
      self.infoWidget = JobInfoWidget(item, self.model.current_connection, self)
    elif isinstance(item, GuiTransfer):
      self.infoWidget = TransferInfoWidget(item, self)
    elif isinstance(item, GuiGroup):
      self.infoWidget = GroupInfoWidget(item, self)
    else:
      self.infoWidget = None
    if self.infoWidget:
      self.vLayout.addWidget(self.infoWidget)
    self.update()
    
  @QtCore.pyqtSlot()
  def dataChanged(self):
    if self.infoWidget:
      self.infoWidget.dataChanged()
 

########################################################
####################   VIEWS   #########################
########################################################
 
class JobInfoWidget(QtGui.QTabWidget):
  
  def __init__(self, job_item, connection, parent = None):
    super(JobInfoWidget, self).__init__(parent)
    
    self.ui = Ui_JobInfo()
    self.ui.setupUi(self)
    
    self.job_item = job_item
    self.connection = connection
    
    self.dataChanged()
    
    self.currentChanged.connect(self.currentTabChanged)
    self.ui.stderr_refresh_button.clicked.connect(self.refreshStdErrOut)
    self.ui.stdout_refresh_button.clicked.connect(self.refreshStdErrOut)
    
  def dataChanged(self):
 
    setLabelFromString(self.ui.job_name, self.job_item.name)
    setLabelFromString(self.ui.job_status,self.job_item.status)
    exit_status, exit_value, term_signal, resource_usage = self.job_item.exit_info
    setLabelFromString(self.ui.exit_status, exit_status)
    setLabelFromInt(self.ui.exit_value, exit_value)
    setLabelFromString(self.ui.term_signal, term_signal)
    setLabelFromString(self.ui.command,self.job_item.command)
    
    if resource_usage: 
      self.ui.resource_usage.insertItems(0, resource_usage.split())
    else: 
      self.ui.resource_usage.clear()
    
    setLabelFromDateTime(self.ui.submission_date, self.job_item.submission_date)
    setLabelFromDateTime(self.ui.execution_date, self.job_item.execution_date)
    setLabelFromDateTime(self.ui.ending_date, self.job_item.ending_date)
    if self.job_item.submission_date:
      if self.job_item.execution_date:
        time_in_queue = self.job_item.execution_date - self.job_item.submission_date
        setLabelFromTimeDelta(self.ui.time_in_queue, time_in_queue)
        if self.job_item.ending_date:
          execution_time = self.job_item.ending_date - self.job_item.execution_date
          setLabelFromTimeDelta(self.ui.execution_time, execution_time)
    
    self.ui.stdout_file_contents.setText(self.job_item.stdout)
    self.ui.stderr_file_contents.setText(self.job_item.stderr)
    
  
  @QtCore.pyqtSlot(int)
  def currentTabChanged(self, index):
    if (index == 1 or index == 2) and self.job_item.stdout == "":
      try:
        self.job_item.updateStdOutErr(self.connection)
      except ConnectionClosedError, e:
        self.parent.emit(QtCore.SIGNAL("connection_closed_error()"))
      else:
        self.dataChanged() 
      
  @QtCore.pyqtSlot()
  def refreshStdErrOut(self):
    try:
      self.job_item.updateStdOutErr(self.connection)
    except ConnectionClosedError, e:
        self.parent().emit(QtCore.SIGNAL("connection_closed_error()"))
    self.dataChanged()
    
    
class TransferInfoWidget(QtGui.QTabWidget):
  
  def __init__(self, transfer_item, parent = None):
    super(TransferInfoWidget, self).__init__(parent)
    self.ui = Ui_TransferInfo()
    self.ui.setupUi(self)
    
    self.transfer_item = transfer_item
    self.dataChanged()
    
  def dataChanged(self):
    
    setLabelFromString(self.ui.transfer_name, self.transfer_item.name)
    setLabelFromString(self.ui.transfer_status, self.transfer_item.transfer_status)
    setLabelFromString(self.ui.client_path, self.transfer_item.data.client_path)
    setLabelFromString(self.ui.cr_path, self.transfer_item.engine_path)
    
    if self.transfer_item.data.client_paths:
      self.ui.client_paths.insertItems(0, self.transfer_item.data.client_paths)
    else:
      self.ui.client_paths.clear()
    

class GroupInfoWidget(QtGui.QWidget):
  
  def __init__(self, group_item, parent = None):
    super(GroupInfoWidget, self).__init__(parent)
    self.ui = Ui_GroupInfo()
    self.ui.setupUi(self)
    
    self.group_item = group_item
    self.dataChanged()
    
  def dataChanged(self):
    
    job_nb = len(self.group_item.not_sub) + len(self.group_item.done) + len(self.group_item.failed) + len(self.group_item.running)
    
    ended_job_nb = len(self.group_item.done) + len(self.group_item.failed)
    
    total_time = None
    if self.group_item.first_sub_date and self.group_item.last_end_date:
      total_time = self.group_item.last_end_date - self.group_item.first_sub_date
    
    #input_file_nb = len(self.group_item.input_to_transfer) + len(self.group_item.input_transfer_ended)
    #ended_input_transfer_nb = len(self.group_item.input_transfer_ended)
    
    setLabelFromString(self.ui.status, self.group_item.status)
    setLabelFromInt(self.ui.job_nb, job_nb)
    setLabelFromInt(self.ui.ended_job_nb, ended_job_nb)
    setLabelFromTimeDelta(self.ui.total_time, total_time)
    setLabelFromTimeDelta(self.ui.theoretical_serial_time, self.group_item.theoretical_serial_time)
    #setLabelFromInt(self.ui.input_file_nb, input_file_nb)
    #setLabelFromInt(self.ui.ended_input_transfer_nb, ended_input_transfer_nb)
    
    #if self.group_item.input_to_transfer:
      #self.ui.comboBox_input_to_transfer.insertItems(0,  self.group_item.input_to_transfer)
    #else:
      #self.ui.comboBox_input_to_transfer.clear()
    
    #if self.group_item.output_ready:
      #self.ui.comboBox_output_to_transfer.insertItems(0,  self.group_item.output_ready)
    #else:
      #self.ui.comboBox_output_to_transfer.clear()
    

class PlotView(QtGui.QWidget):
  
  def __init__(self, group_item, parent = None):
    super(PlotView, self).__init__(parent)
    
    self.ui = Ui_PlotWidget()
    self.ui.setupUi(self)
    self.vlayout = QtGui.QVBoxLayout()
    self.ui.frame_plot.setLayout(self.vlayout)
    
    self.ui.combo_plot_type.addItems(["jobs fct time", "nb proc fct time"])
    self.ui.combo_plot_type.setCurrentIndex(0)
    self.plot_type = 0
    
    self.canvas = None
    self.group_item = group_item
    self.jobs = self.group_item.done
    self.jobs.extend(self.group_item.failed)
    self.jobs.extend(self.group_item.running)
    self.jobs.extend(self.group_item.not_sub)
    self.jobs = sorted(self.jobs, key=self.sortkey)
    
    self.updatePlot()
    
    self.ui.combo_plot_type.currentIndexChanged.connect(self.plotTypeChanged)
    self.ui.button_refresh.clicked.connect(self.refresh)
    
  def sortkey(self, j):
    if j.execution_date:
      return j.execution_date
    else:
      return datetime.max
    
  @QtCore.pyqtSlot(int)
  def plotTypeChanged(self, index):
    self.plot_type = index
    self.updatePlot()
     
  QtCore.pyqtSlot()
  def refresh(self):
    self.updatePlot()
     
  def dataChanged(self):
    if self.ui.checkbox_auto_update.isChecked():
      self.updatePlot()
  
  def updatePlot(self):
    if self.plot_type == 0:
      self.jobsFctTime()
    if self.plot_type == 1:
      self.nbProcFctTime()

    
  def jobsFctTime(self):
    
    if self.canvas:
      self.canvas.hide()
      self.vlayout.removeWidget(self.canvas)
      self.canvas = None
      
    self.figure = Figure()
    self.axes = self.figure.add_subplot(111)
    self.axes.hold(True)
    self.canvas = FigureCanvas(self.figure)
    self.canvas.setParent(self)
    self.canvas.updateGeometry()
    self.vlayout.addWidget(self.canvas)
    
    def key(j):
      if j.execution_date:
        return j.execution_date
      else:
        return datetime.max
    
    self.jobs = sorted(self.jobs, key=self.sortkey)
  
    nb_jobs = 0 
    x_min = datetime.max
    x_max = datetime.min
    for j in self.jobs:
      if j.execution_date:
        nb_jobs = nb_jobs + 1
        if j.execution_date < x_min:
          x_min = j.execution_date
        if j.ending_date:
          self.axes.plot([j.execution_date, j.ending_date], [nb_jobs, nb_jobs])
          if j.ending_date > x_max:
            x_max = j.ending_date
        else:
          self.axes.plot([j.execution_date, datetime.now()], [nb_jobs, nb_jobs])
          
  
    if nb_jobs:
      self.axes.set_ylim(0, nb_jobs+1)
    
    self.axes.set_xlabel("Time")
    self.axes.set_ylabel("Jobs")
    self.figure.autofmt_xdate(rotation=80)

    self.canvas.draw()
    
    self.update()
  
    
  def nbProcFctTime(self):
    
    if self.canvas:
      self.canvas.hide()
      self.vlayout.removeWidget(self.canvas)
      self.canvas = None
      
    self.figure = Figure()
    self.axes = self.figure.add_subplot(111)
    self.axes.hold(True)
    self.canvas = FigureCanvas(self.figure)
    self.canvas.setParent(self)
    self.canvas.updateGeometry()
    self.vlayout.addWidget(self.canvas)
    
    
    dates = []
    nb_process_running = []
    infos = [] # sequence of tuple (job_item, start, date) 
               # start is a bolean
               # if start then date is the execution date
               # else date is the ending date
    
    for job_item in self.jobs:
      if job_item.execution_date:
        infos.append((job_item, True, job_item.execution_date))
      if job_item.ending_date:
        infos.append((job_item, False, job_item.ending_date))
      else:
        infos.append((job_item, False, datetime.now()))
        
    infos = sorted(infos, key=lambda info_elem: info_elem[2])
    
    nb_process = 0
    previous = None
    for info_elem in infos:
      if previous and info_elem[2] == previous[2]:
        if info_elem[1]:
          nb_process = nb_process + 1
        else:
          nb_process = nb_process - 1
        nb_process_running[len(nb_process_running)-1] = nb_process
      else:
        dates.append(info_elem[2])
        if info_elem[1]:
          nb_process = nb_process + 1
        else:
          nb_process = nb_process - 1
        nb_process_running.append(nb_process)
      previous = info_elem
     
    nb_proc_max = 0
    for i in range( 1, len(nb_process_running)):
      self.axes.fill_between([dates[i-1], dates[i]], [nb_process_running[i-1], nb_process_running[i-1]], y2=0, edgecolor = 'b')
      if nb_process_running[i] > nb_proc_max:
        nb_proc_max = nb_process_running[i]
    
    if nb_proc_max != 0:
      self.axes.set_ylim(0, nb_proc_max+1)

    self.axes.set_xlabel("Time")
    self.axes.set_ylabel("Nb of proc")
    self.figure.autofmt_xdate(rotation=80)
  
    self.canvas.draw()
    self.update()
 

class WorkflowGraphView(QtGui.QWidget):
  
  def __init__(self, connection, parent = None):
    super(WorkflowGraphView, self).__init__(parent)
    self.ui = Ui_GraphWidget()
    self.ui.setupUi(self)
    
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
    self.ui.button_refresh.clicked.connect(self.refresh)
                                        
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
  def refresh(self):
    self.dataChanged(force = True)
    
  @QtCore.pyqtSlot()
  def dataChanged(self, force = False):
    if self.workflow and (force or self.ui.checkbox_auto_update.isChecked()):
      print "graph update"
      image_file_path = self.printWorkflow()
      image = QtGui.QImage(image_file_path)
      pixmap = QtGui.QPixmap.fromImage(image)
      self.image_label.setPixmap(pixmap)
      self.ui.scrollArea.setWidget(self.image_label)
      self.image_label.resize(self.image_label.pixmap().size()*self.scale_factor)
    else:
      self.ui.scrollArea.takeWidget()

  
  def printWorkflow(self):
      
    output_dir = "/tmp/"
    
    GRAY="\"#C8C8B4\""
    BLUE="\"#00C8FF\""
    RED="\"#FF6432\""
    GREEN="\"#9BFF32\""
    LIGHT_BLUE="\"#C8FFFF\""
    
    names = dict()
    current_id = 0
    
    dot_file_path = output_dir + "tmp.dot"
    graph_file_path = output_dir + "tmp.png"
    if dot_file_path and os.path.isfile(dot_file_path):
      os.remove(dot_file_path)
    file = open(dot_file_path, "w")
    print >> file, "digraph G {"
    for node in self.workflow.jobs:
      current_id = current_id + 1
      names[node] = ("node" + repr(current_id), "\""+node.name+"\"")
    for ar in self.workflow.dependencies:
      print >> file, names[ar[0]][0] + " -> " + names[ar[1]][0] 
    for node in self.workflow.jobs:
      if isinstance(node, Job):
        if node.job_id == -1:
          print >> file, names[node][0] + "[shape=box label="+ names[node][1] +"];"
        else:
          status = self.connection.job_status(node.job_id)
          if status == NOT_SUBMITTED:
            print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + GRAY +"];"
          elif status == DONE:
            exit_status, exit_value, term_signal, resource_usage = self.connection.job_termination_status(node.job_id)
            if exit_status == FINISHED_REGULARLY and exit_value == 0:
              print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + LIGHT_BLUE +"];"
            else: 
              print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + RED +"];"
          elif status == FAILED:
            print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + RED +"];"
          else:
            print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + GREEN +"];"
      if isinstance(node, FileTransfer):
        if not node.engine_path:
          print >> file, names[node][0] + "[label="+ names[node][1] +"];"
        else:
          status = self.connection.transfer_status(node.engine_path)[0]
          if status == FILES_DONT_EXIST:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + GRAY +"];"
          elif status == FILES_ON_CR or status == FILES_ON_CLIENT_AND_CR or status == FILES_ON_CLIENT:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + BLUE +"];"
          elif status == TRANSFERING_FROM_CLIENT_TO_CR or status == TRANSFERING_FROM_CR_TO_CLIENT:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + GREEN +"];"
          elif status == FILES_UNDER_EDITION:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + LIGHT_BLUE +"];"
          
    print >> file, "}"
    file.close()
    
    command = "dot -Tpng " + dot_file_path + " -o " + graph_file_path
    #print command
    #dot_process = subprocess.Popen(command, shell = True)
    commands.getstatusoutput(command)
    return graph_file_path

      
########################################################
############   MODEL FOR THE TREE VIEW   ###############
########################################################
    
class WorkflowItemModel(QtCore.QAbstractItemModel):
  
  def __init__(self, gui_workflow, parent=None):
    '''
    @type gui_workflow: L{GuiWorkflow}
    '''
    super(WorkflowItemModel, self).__init__(parent)
    self.workflow = gui_workflow 

    self.group_done_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/group_done.png"))
    self.group_failed_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/group_failed.png"))
    self.group_running_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/group_running.png"))
    self.group_no_status_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/group_no_status.png"))
    self.group_warning_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/group_warning.png"))

    self.running_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/running.png"))
    self.failed_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/failed.png"))
    self.done_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/success.png"))
    self.pending_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/pending.png"))
    self.queued_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/queued.png"))
    self.undetermined_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/undetermined.png"))
    self.warning_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/warning.png"))
    self.kill_delete_pending_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/kill_delete_pending.png"))
    self.no_status_icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/no_status.png"))


    self.transfer_files_dont_exit = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/transfer_files_dont_exist.png"))
    self.transfer_files_on_client = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/transfer_files_on_client.png"))
    self.transfer_files_on_both = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/transfer_files_on_both.png"))
    self.transfer_files_on_cr = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/transfer_files_on_cr.png"))
    self.transfering_from_client_to_cr = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/transfering_from_client_to_cr.png"))
    self.transfering_from_cr_to_client = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/transfering_from_cr_to_client.png"))
    self.files_under_edition = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/files_under_edition.png"))

    
  def index(self, row, column, parent=QtCore.QModelIndex()):
    if row < 0 or not column == 0:
      return QtCore.QModelIndex()
 
    if not parent.isValid():
      if row < len(self.workflow.root_item.children):
        return self.createIndex(row, column, self.workflow.items[self.workflow.root_item.children[row]])
    else:
      parent_item = parent.internalPointer()
      if row < len(parent_item.children):
        return self.createIndex(row, column, self.workflow.items[parent_item.children[row]])
      
    return QtCore.QModelIndex()
    
  def parent(self, index):
    if not index.isValid():
      return QtCore.QModelIndex()
    
    item = index.internalPointer()
    if not item.parent == -1:
      parent_item = self.workflow.items[item.parent]
      return self.createIndex(parent_item.row, 0, self.workflow.items[item.parent])
    
    return QtCore.QModelIndex()

  def rowCount(self, parent):
    if not parent.isValid():
      return len(self.workflow.root_item.children)
    else:
      parent_item = parent.internalPointer()
      return len(parent_item.children)

  def columnCount(self, parent):
    children_nb = 0
    if not parent.isValid():
      children_nb = len(self.workflow.root_item.children)
    else:
      children_nb = len(parent.internalPointer().children)
        
    if children_nb == 0:
      return 0
    else:
      return 1

  def data(self, index, role):
    if not index.isValid():
      return QtCore.QVariant()
  
    item = index.internalPointer()
    
    #if not item.initiated:
      # WIP
      
    #### Groups ####
    if isinstance(item,GuiGroup):
      if role == QtCore.Qt.FontRole:
        font = QtGui.QFont()
        font.setBold(True)
        return font
      if role == QtCore.Qt.DisplayRole:
        return item.name
      else:
        if item.status == GuiGroup.GP_NO_STATUS:
          if role == QtCore.Qt.DecorationRole:
            return self.group_no_status_icon
        if item.status == GuiGroup.GP_NOT_SUBMITTED:
          if role == QtCore.Qt.DecorationRole:
            return QtGui.QIcon()
        if item.status == GuiGroup.GP_DONE:
          if role == QtCore.Qt.DecorationRole:
            return self.group_done_icon
        if item.status == GuiGroup.GP_FAILED:
          if role == QtCore.Qt.DecorationRole:
            return self.group_failed_icon
        if item.status == GuiGroup.GP_RUNNING:
          if role == QtCore.Qt.DecorationRole:
            return self.group_running_icon
        if item.status == GuiGroup.GP_WARNING:
          if role == QtCore.Qt.DecorationRole:
            return self.group_warning_icon
    
    #### Jobs ####
    if isinstance(item, GuiJob): 
      if item.job_id == -1:
        if role == QtCore.Qt.DisplayRole:
          return item.name
        if role == QtCore.Qt.DecorationRole:
            return self.no_status_icon
      else:
        status = item.status
        # Done or Failed
        if status == DONE or status == FAILED:
          exit_status, exit_value, term_signal, resource_usage = item.exit_info
          if role == QtCore.Qt.DisplayRole:
            return item.name #+ " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
          if role == QtCore.Qt.DecorationRole:
            if status == DONE and exit_status == FINISHED_REGULARLY and exit_value == 0:
              return self.done_icon
            elif status == DONE and exit_status == None:
              return self.undetermined_icon
            else:
              return self.failed_icon
              

        if role == QtCore.Qt.DisplayRole:
          return item.name #+ " " + status 

        if role == QtCore.Qt.DecorationRole:
          if status == NOT_SUBMITTED:
            return QtGui.QIcon()
          if status == UNDETERMINED:
            return self.undetermined_icon
          elif status == QUEUED_ACTIVE:
            return self.queued_icon
          elif status == RUNNING:
            return self.running_icon
          elif status == DELETE_PENDING or status == KILL_PENDING:
            return self.kill_delete_pending_icon
          elif status == SUBMISSION_PENDING:
            return self.pending_icon
          elif status == WARNING:
            return self.warning_icon
          else: #SYSTEM_ON_HOLD USER_ON_HOLD USER_SYSTEM_ON_HOLD
                #SYSTEM_SUSPENDED USER_SUSPENDED USER_SYSTEM_SUSPENDED
            return self.warning_icon
        
    #### FileTransfers ####
    if isinstance(item, GuiTransfer):
      if isinstance(item, GuiInputTransfer):
        #if role == QtCore.Qt.ForegroundRole:
          #return QtGui.QBrush(RED)
        if item.transfer_status == TRANSFERING_FROM_CLIENT_TO_CR or item.transfer_status == TRANSFERING_FROM_CR_TO_CLIENT:
          display = "in: " + item.name + " " + repr(item.percentage_achievement) + "%"
        else:
          display = "in: " + item.name
      if isinstance(item, GuiOutputTransfer):
        #if role == QtCore.Qt.ForegroundRole:
          #return QtGui.QBrush(BLUE)
        if item.transfer_status == TRANSFERING_FROM_CLIENT_TO_CR or item.transfer_status == TRANSFERING_FROM_CR_TO_CLIENT:
          display = "out: " + item.name + " " + repr(item.percentage_achievement) + "%"
        else:
          display = "out: " + item.name
        
      if not item.engine_path:
        if role == QtCore.Qt.DisplayRole:
          return display
      else:
        status = item.transfer_status
        if role == QtCore.Qt.DisplayRole:
          return display #+ " => " + status
        if status == FILES_DO_NOT_EXIST:
          if role == QtCore.Qt.DecorationRole:
            return self.transfer_files_dont_exit
        if status == FILES_ON_CLIENT:
          if role == QtCore.Qt.DecorationRole:
            return self.transfer_files_on_client
        if status == FILES_ON_CR:
          if role == QtCore.Qt.DecorationRole:
            return self.transfer_files_on_cr
        if status == FILES_ON_CLIENT_AND_CR:
          if role == QtCore.Qt.DecorationRole:
            return self.transfer_files_on_both
        if status == TRANSFERING_FROM_CLIENT_TO_CR:
          if role == QtCore.Qt.DecorationRole:
            return self.transfering_from_client_to_cr
        if status == TRANSFERING_FROM_CR_TO_CLIENT:
          if role == QtCore.Qt.DecorationRole:
            return self.transfering_from_cr_to_client
        if status == FILES_UNDER_EDITION:
          if role == QtCore.Qt.DecorationRole:
            return self.files_under_edition
      
    
    return QtCore.QVariant()
    
########################################################
#######################   MODEL   ######################
########################################################

class GuiModel(QtCore.QObject):
  '''
  Model for the GUI. This model was created to provide faster
  GUI minimizing communications with the server.
  The instances of this class hold the connections and the GuiWorkflow instances 
  created in the current session of the GUI.
  The current workflow is periodically updated and the signal data_changed is
  emitted if necessary.
  '''
  
  def __init__(self, parent = None):
    
    super(GuiModel, self).__init__(parent)

    
    self.connections = {} # ressource_id => connection
    self.workflows = {} # gui_workflows # resource_id => workflow_id => GuiWorkflow
    self.expiration_dates = {} # resource_id => workflow_ids => expiration_dates
        
    self.current_connection = None
    self.current_resource_id = None
    self.current_workflow = None
    self.current_wf_id = -1
    self.expiration_date = None
    
    self.update_interval = 3 # update period in seconds
    self.auto_update = True
    self.hold = False
    
    self._lock = threading.RLock()
    
    def updateLoop(self):
      # update only the current workflows
      while True:
        with self._lock:
          if not self.hold and self.auto_update and self.current_workflow :
              try:
                if self.current_workflow.wf_id == -1:
                  wf_status = None
                else:
                  #print " ==> communication with the server " + repr(self.wf_id)
                  #begining = datetime.now()
                  wf_status = self.current_connection.workflow_elements_status(self.current_workflow.wf_id)
                  #end = datetime.now() - begining
                  #print " <== end communication" + repr(self.wf_id) + " : " + repr(end.seconds)
              except ConnectionClosedError, e:
                self.emit(QtCore.SIGNAL('connection_closed_error()'))
                self.hold = True
              except UnknownObjectError, e:
                self.delete_workflow()
              else: 
                if self.current_workflow and self.current_workflow.updateState(wf_status): 
                  self.emit(QtCore.SIGNAL('workflow_state_changed()'))
        time.sleep(self.update_interval)
    
    self.__update_thread = threading.Thread(name = "GuiModelUpdateLoop",
                                           target = updateLoop,
                                           args = ([self]))
    self.__update_thread.setDaemon(True)
    self.__update_thread.start()
   
   
  def addConnection(self, resource_id, connection):
    '''
    Adds a connection and use it as the current connection
    '''
    with self._lock:
      self.connections[resource_id] = connection
      self.workflows[resource_id] = {}
      self.expiration_dates[resource_id] = {}
      self.current_resource_id = resource_id
      self.current_connection = connection
      self.current_workflow = None
      self.current_wf_id = -1
      self.expiration_date = None
      self.emit(QtCore.SIGNAL('current_connection_changed()'))
      self.hold = False

  def deleteCurrentConnection(self):
    '''
    Delete the current connection.
    If any other connections exist the new current connection will be one of them.
    If not the current connection is set to None.
    '''
    with self._lock:
      del self.connections[self.current_resource_id] 
      del self.workflows[self.current_resource_id]
      del self.expiration_dates[self.current_resource_id]
      self.current_resource_id = None
      self.current_connection = None
      self.current_workflow = None
      self.current_wf_id = -1
      self.expiration_date = None
    
      if len(self.connections.keys()):
        self.current_resource_id = self.connections.keys()[0]
        self.current_connection = self.connections[self.current_resource_id]
      self.emit(QtCore.SIGNAL('current_connection_changed()'))
      self.hold = False
    
  def setCurrentConnection(self, resource_id):
    with self._lock:
      if resource_id != self.current_resource_id:
        assert(resource_id in self.connections.keys())
        self.current_resource_id = resource_id
        self.current_connection = self.connections[resource_id]
        self.emit(QtCore.SIGNAL('current_connection_changed()'))
        self.hold = False

  def reinitCurrentConnection(self, connection):
    with self._lock:
      self.current_connection = connection
      self.connections[self.current_resource_id] = connection
      self.hold = False
    
  def addWorkflow(self, workflow, expiration_date):
    '''
    Build a GuiWorkflow from a soma.workflow.client.Worklfow and 
    use it as the current workflow. 
    @type worklfow: soma.workflow.client.Workflow
    '''
    with self._lock:
      self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
      self.hold = True 
      self.current_workflow = GuiWorkflow(workflow)
      self.current_wf_id = self.current_workflow.wf_id
      self.expiration_date = expiration_date
      if self.current_wf_id != -1:
        self.workflows[self.current_resource_id][self.current_workflow.wf_id] = self.current_workflow
        self.expiration_dates[self.current_resource_id][self.current_workflow.wf_id] = self.expiration_date
        try:
          wf_status = self.current_connection.workflow_elements_status(self.current_workflow.wf_id)
        except ConnectionClosedError, e:
          self.emit(QtCore.SIGNAL('connection_closed_error()'))
        else: 
          self.current_workflow.updateState(wf_status)
      self.emit(QtCore.SIGNAL('current_workflow_changed()'))
      self.hold = False

  def restartCurrentWorkflow(self):
    self.current_workflow.restart()
    
  def delete_workflow(self):
    with self._lock:
      self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
      if self.current_workflow and self.current_workflow.wf_id in self.workflows.keys():
        del self.workflows[wf_id]
        del self.expiration_dates[wf_id]
      self.current_workflow = None
      self.current_wf_id = -1
      self.expiration_date = datetime.now()
    
      self.emit(QtCore.SIGNAL('current_workflow_changed()'))
    
  def clearCurrentWorkflow(self):
    with self._lock:
      if self.current_workflow != None or  self.current_wf_id != -1:
        self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
        self.current_workflow = None
        self.current_wf_id = -1
        self.expiration_date = datetime.now()
        self.emit(QtCore.SIGNAL('current_workflow_changed()'))
    
  def setCurrentWorkflow(self, wf_id):
    with self._lock:
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
  
  
class GuiWorkflow(object):

  # id of the workflow in soma-workflow
  wf_id = None
  #
  wf_status = None
  # 
  root_item = None
  # dict: id => WorkflowItems
  items = None
  # Workflow or EngineWorkflow
  server_workflow = None
  # dict: FileTransfer id => sequence of gui item transfers id
  server_file_transfers = None
  # dict: Job id => gui item job id
  server_jobs = None
  
  
  def __init__(self, workflow):
    
    #print("wf " +repr(workflow))
    self.name = workflow.name 
  
    if isinstance(workflow, EngineWorkflow):
      self.wf_id = workflow.wf_id
    else:
      self.wf_id = -1

    self.wf_status = None
    
    ids = {} # workflow element => sequence of ids
    self.items = {} # id => WorkflowItem
    self.root_item = None
   
    id_cnt = 0  # unique id for the items
    
    self.server_workflow = workflow 
    self.server_jobs = {} 
    self.server_file_transfers = {} 
    
    #print " ==> building the workflow "
    #begining = datetime.now()
    # retrieving the set of job and the set of file transfers
    w_js = set([]) 
    w_fts = set([]) 
   
    for job in workflow.jobs:
      w_js.add(job)
      
    # Processing the Jobs to create the corresponding GuiJob instances
    for job in w_js:
      item_id = id_cnt
      id_cnt = id_cnt + 1
      if isinstance(workflow, EngineWorkflow):
        job_id = workflow.job_mapping[job].job_id
        command = workflow.job_mapping[job].plain_command()
      else:
        job_id = -1
        command = job.command
      gui_job = GuiJob(it_id = item_id, 
                       command=command,
                       parent=-1, 
                       row=-1, 
                       data=job, 
                       children_nb=len(job.referenced_input_files)+len(job.referenced_output_files),
                       name=job.name,
                       job_id=job_id)
      ids[job] = item_id
      self.items[item_id] = gui_job
      self.server_jobs[gui_job.job_id] = item_id
      for ft in job.referenced_input_files:
        w_fts.add(ft)
      for ft in job.referenced_output_files:
        w_fts.add(ft)
      
    # Create the GuiGroup instances
    self.root_item = GuiGroup(self, 
                              it_id=-1, 
                              parent=-1, 
                              row=-1, 
                              data=workflow.root_group, 
                              children_nb=len(workflow.root_group))
                  

    for group in workflow.groups:
      item_id = id_cnt
      id_cnt = id_cnt + 1
      ids[group] = item_id
      self.items[item_id] = GuiGroup(self,
                                     it_id=item_id, 
                                     parent=-1, 
                                     row=-1, 
                                     data=group, 
                                     children_nb=len(group.elements),
                                     name=group.name)

    # parent and children research for jobs and groups
    for item in self.items.values():
      if isinstance(item, GuiGroup) or isinstance(item, GuiJob):
        if item.data in workflow.root_group:
          item.parent = -1
          item.row = workflow.root_group.index(item.data)
          self.root_item.children[item.row]=item.it_id
        for group in workflow.groups:
          if item.data in group.elements:
            item.parent = ids[group]
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
      #print " ft " + repr(ft)
      ids[ft] = []
      for job in w_js:
        ref_in = list(job.referenced_input_files)
        ref_in.sort(compFileTransfers)
        ref_out = list(job.referenced_output_files)
        ref_out.sort(compFileTransfers)
        if ft in ref_in: 
          item_id = id_cnt
          id_cnt = id_cnt + 1
          ids[ft].append(item_id)
          row = ref_in.index(ft)
          if isinstance(workflow, EngineWorkflow):
            engine_path = workflow.transfer_mapping[ft].engine_path
          else:
            engine_path = None
          gui_transfer = GuiInputTransfer(it_id=item_id, 
                                          parent=ids[job], 
                                          row=row, 
                                          data=ft,
                                          name=ft.name,
                                          engine_path=engine_path)
          self.items[item_id] = gui_transfer
          if gui_transfer.engine_path in self.server_file_transfers.keys():
            self.server_file_transfers[gui_transfer.engine_path].append(item_id)
          else:
            self.server_file_transfers[gui_transfer.engine_path] = [item_id]
          self.items[ids[job]].children[row]=item_id
          #print repr(job.name) + " " + repr(self.items[ids[job]].children)
        if ft in ref_out: #
          item_id = id_cnt
          id_cnt = id_cnt + 1
          ids[ft].append(item_id)
          row = len(ref_in)+ref_out.index(ft)
          if isinstance(workflow, EngineWorkflow):
            engine_path = workflow.transfer_mapping[ft].engine_path
          else:
            engine_path = None
          gui_ft = GuiOutputTransfer(it_id=item_id, 
                                     parent=ids[job], 
                                     row=row, 
                                     data=ft,
                                     name=ft.name,
                                     engine_path=engine_path)
          self.items[item_id] = gui_ft
          if gui_ft.engine_path in self.server_file_transfers.keys():
            self.server_file_transfers[gui_ft.engine_path].append(item_id)
          else:
            self.server_file_transfers[gui_ft.engine_path] = [item_id]
          
          self.items[ids[job]].children[row]=item_id
          #print repr(job.name) + " " + repr(self.items[ids[job]].children)

    #for item in self.items.itervalues():
      #print repr(item.children)
          
    #end = datetime.now() - begining
    #print " <== end building worflow " + repr(end.seconds)
                                  
    ########## #print model ####################
    #print "dependencies : " + repr(len(workflow.dependencies))
    #if workflow.full_dependencies: 
      #print "full_dependencies : " + repr(len(workflow.full_dependencies)) 
    #for dep in workflow.dependencies:
      #print dep[0].name + " -> " + dep[1].name
    #for item in self.items.values():
      #print repr(item.it_id) + " " + repr(item.parent) + " " + repr(item.row) + " " + repr(item.it_type) + " " + repr(item.name) + " " + repr(item.children)   
    #raw_input()
    ###########################################
    
  def updateState(self, wf_status):
    if self.wf_id == -1: 
      return False
    data_changed = False
    
    if not wf_status:
      return False
    #updating jobs:
    for job_info in wf_status[0]:
      job_id, status, exit_info, date_info = job_info
      #date_info = (None, None, None) # (submission_date, execution_date, ending_date)
      item = self.items[self.server_jobs[job_id]]
      data_changed = item.updateState(status, exit_info, date_info) or data_changed

    #end = datetime.now() - begining
    #print " <== end updating jobs" + repr(self.wf_id) + " : " + repr(end.seconds)
    
    #print " ==> updating transfers " + repr(self.wf_id)
    #begining = datetime.now()
    
    #updating file transfer
    for transfer_info in wf_status[1]:
      engine_file_path, complete_status = transfer_info 
      for item_id in self.server_file_transfers[engine_file_path]:
        item = self.items[item_id]
        data_changed = item.updateState(complete_status) or data_changed
    
    #end = datetime.now() - begining
    #print " <== end updating transfers" + repr(self.wf_id) + " : " + repr(end.seconds) + " " + repr(data_changed)
    
    #updateing groups 
    self.root_item.updateState()
    
    self.wf_status = wf_status[2]
    
    return data_changed

  def restart(self):
    for item in self.items.itervalues():
      if isinstance(item, GuiJob):
        item.stdout = ""
        item.stderr = ""
        item.submission_date = None
        item.execution_date = None
        item.ending_date = None

class GuiWorkflowItem(object):
  '''
  Abstract class for workflow items.
  '''
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0):
    
    self.it_id = it_id
    self.parent = parent # parent_id
    self.row = row
    self.data = data
    self.children = [-1 for i in range(children_nb)]   
    
    self.initiated = False

class GuiGroup(GuiWorkflowItem):
  
  GP_NOT_SUBMITTED = "not_submitted"
  GP_DONE = "done"
  GP_FAILED = "failed"
  GP_RUNNING = "running"
  GP_NO_STATUS = "no_status"
  GP_WARNING = "warning"
  
  def __init__(self,
               gui_workflow,
               it_id, 
               parent=-1, 
               row=-1,
               data=None,
               children_nb=0,
               name="no name"):
    super(GuiGroup, self).__init__(it_id, parent, row, data, children_nb)  
    
    self.gui_workflow = gui_workflow
    
    self.status = GuiGroup.GP_NO_STATUS

    self.name = name
    
    self.not_sub = []
    self.done = []
    self.failed = []
    self.running = []
    
    self.first_sub_date = None
    self.last_end_date = None
    self.theoretical_serial_time = None
    
    self.input_to_transfer = []
    self.input_transfer_ended = []
    self.output_ready = []
    self.output_transfer_ended = []
    
  def updateState(self):
    self.initiated = True
    state_changed = False
    
    self.first_sub_date = datetime.max 
    self.last_end_date = datetime.min
    self.theoretical_serial_time = timedelta(0, 0, 0)
    
    self.input_to_transfer = []
    self.input_transfer_ended = []
    self.output_ready = []
    self.output_transfer_ended = []
    
    self.not_sub = []
    self.done = []
    self.failed = []
    self.running = []

    no_status = False
    warning = False
    
    for child in self.children:
      item = self.gui_workflow.items[child]
      # TO DO : explore files 
      if isinstance(item, GuiJob):
        if item.job_id == -1:
          no_status = True
          break
        if item.status == WARNING:
          warning = True
          break
        if item.status == NOT_SUBMITTED:
          self.not_sub.append(item)
        elif item.status == DONE or item.status == FAILED:
          exit_status, exit_value, term_signal, resource_usage = item.exit_info
          if item.status == DONE and exit_status == FINISHED_REGULARLY and exit_value == 0:
            self.done.append(item)
          elif item.status == DONE and exit_status == None:
            self.running.append(item)
          else:
            self.failed.append(item)
        else:
          self.running.append(item)
        if item.ending_date:
          self.theoretical_serial_time = self.theoretical_serial_time + (item.ending_date - item.execution_date)
          if item.ending_date > self.last_end_date:
            self.last_end_date = item.ending_date
        if item.submission_date and item.submission_date < self.first_sub_date:
           self.first_sub_date = item.submission_date
          
      if isinstance(item, GuiGroup):
        item.updateState()
        self.not_sub.extend(item.not_sub)
        self.done.extend(item.done)
        self.failed.extend(item.failed)
        self.running.extend(item.running)
        self.input_to_transfer.extend(item.input_to_transfer)
        self.input_transfer_ended.extend(item.input_transfer_ended)
        self.output_ready.extend(item.output_ready)
        self.output_transfer_ended.extend(item.output_transfer_ended)
        if item.first_sub_date and item.first_sub_date < self.first_sub_date:
          self.first_sub_date = item.first_sub_date
        if item.last_end_date and item.last_end_date > self.last_end_date:
          self.last_end_date = item.last_end_date 
        self.theoretical_serial_time = self.theoretical_serial_time + item.theoretical_serial_time
           
    if no_status:
      new_status = GuiGroup.GP_NO_STATUS
    elif warning:
      new_status = GuiGroup.GP_WARNING
    elif len(self.failed) > 0:
      new_status = GuiGroup.GP_FAILED
    elif len(self.not_sub) == 0 and len(self.failed) == 0 and len(self.running) == 0:
      new_status = GuiGroup.GP_DONE
    elif len(self.running) == 0 and len(self.done) == 0 and len(self.failed) == 0:
      new_status = GuiGroup.GP_NOT_SUBMITTED
      self.first_sub_date = None
      self.last_end_date = None
    else:
      new_status = GuiGroup.GP_RUNNING
      self.last_end_date = None
        
    state_changed = self.status != new_status
    self.status = new_status
    return state_changed

class GuiJob(GuiWorkflowItem):
  
  def __init__(self,
               it_id,
               command,
               parent=-1, 
               row=-1,
               it_type=None,
               data=None,
               children_nb=0,
               name="no name",
               job_id=-1,):
    super(GuiJob, self).__init__(it_id, parent, row, data, children_nb)
    
    self.status = "not submitted"
    self.exit_info = ("", "", "", "")
    self.stdout = ""
    self.stderr = ""
    self.submission_date = None
    self.execution_date = None
    self.ending_date = None
    
    self.name = name
    self.job_id = job_id
    #if isinstance(data, EngineJob):
      #self.job_id = data.job_id
    #else:
      #self.job_id = -1
    
    cmd_seq = []
    for command_el in command:
      if isinstance(command_el, tuple) and isinstance(command_el[0], FileTransfer):
        cmd_seq.append("<FileTransfer " + command_el[0].client_path + " >")
      elif isinstance(command_el, FileTransfer):
        cmd_seq.append("<FileTransfer " + command_el.client_path + " >")
      elif isinstance(command_el, SharedResourcePath):
        cmd_seq.append("<SharedResourcePath " + command_el.namespace + " " + command_el.uuid + " " +  command_el.relative_path + " >")
      elif isinstance(command_el, unicode) or isinstance(command_el, unicode):
        cmd_seq.append(unicode(command_el).encode('utf-8'))
      else:
        cmd_seq.append(repr(command_el))
    separator = " " 
    self.command = separator.join(cmd_seq)
    
  def updateState(self, status, exit_info, date_info):
    self.initiated = True
    state_changed = False
    state_changed = self.status != status or state_changed
    self.status = status
    state_changed = self.exit_info != exit_info or state_changed
    self.exit_info = exit_info
    self.submission_date = date_info[0]
    self.execution_date = date_info[1]
    self.ending_date = date_info[2]
    if self.exit_info: 
      exit_status, exit_value, term_signal, resource_usage = self.exit_info
      if resource_usage:
        ru = resource_usage.split()
        for ruel in ru:
          ruel = ruel.split("=")
          if ruel[0] == "start_time" and ruel[1] != "0":
            t = time.localtime(float(ruel[1].replace(',', '.')))
            self.execution_date = datetime(year = t[0], month = t[1], day = t[2], hour = t[3], minute = t[4], second = t[5])
          elif ruel[0] == "end_time" and ruel[1] != "0":
            t = time.localtime(float(ruel[1].replace(',', '.')))
            self.ending_date = datetime(year = t[0], month = t[1], day = t[2], hour = t[3], minute = t[4], second = t[5])
          elif ruel[0] == "submission_time" and ruel[1] != "0":
            t = time.localtime(float(ruel[1].replace(',', '.')))
            self.submission_date = datetime(year = t[0], month = t[1], day = t[2], hour = t[3], minute = t[4], second = t[5])
        
        
    return state_changed
    
  def updateStdOutErr(self, connection):
    if self.data:
      stdout_path = "/tmp/soma_workflow_stdout"
      stderr_path = "/tmp/soma_workflow_stderr"
      connection.retrieve_job_stdouterr(self.job_id, stdout_path, stderr_path)
      
      f = open(stdout_path, "rt")
      line = f.readline()
      stdout = ""
      while line:
        stdout = stdout + line + "\n"
        line = f.readline()
      self.stdout = stdout
      f.close()
      
      f = open(stderr_path, "rt")
      line = f.readline()
      stderr = ""
      while line:
        stderr = stderr + line + "\n"
        line = f.readline()
      self.stderr = stderr
      f.close()
      
      
class GuiTransfer(GuiWorkflowItem):
  
  DIRECTORY = "directory"
  FILE = "file"
  
  def __init__(self,
               it_id, 
               parent=-1, 
               row=-1,
               data=None,
               children_nb=0,
               name="no name",
               engine_path=None):
    super(GuiTransfer, self).__init__(it_id, parent, row, data, children_nb)
    
    self.transfer_status = " "
    self.size = None
    self.transmitted = None
    self.elements_status = None
    
    self.percentage_achievement = 0
    self.transfer_type = None
    self.name = name
    self.engine_path = engine_path

  
  def updateState(self, transfer_status_info):
    self.initiated = True
    state_changed = False
    transfer_status = transfer_status_info[0]
    
    if transfer_status_info[1]:
      if len(transfer_status_info[1]) == 2:
        self.transfer_type = GuiTransfer.FILE
        size, transmitted = transfer_status_info[1]
        elements_status = None
      elif len(transfer_status_info[1]) == 3:
        self.transfer_type = GuiTransfer.DIRECTORY
        (size, transmitted, elements_status) = transfer_status_info[1]
      self.percentage_achievement = int(float(transmitted)/size * 100.0)
    else:
      (size, transmitted, elements_status) = (None, None, None)
      self.percentage_achievement = 0
      
    state_changed = state_changed or transfer_status != self.transfer_status
    state_changed = state_changed or size != self.size
    state_changed = state_changed or transmitted != self.transmitted
    state_changed = state_changed or elements_status != self.elements_status
    
    self.transfer_status = transfer_status
    self.size = size
    self.transmitted = transmitted
    self.elements_status = elements_status
    
    return state_changed
  
  

class GuiInputTransfer(GuiTransfer):
  
  def __init__(self,
               it_id, 
               parent=-1, 
               row=-1,
               data=None,
               children_nb=0,
               name="no name",
               engine_path=None):
    super(GuiInputTransfer, self).__init__(it_id, 
                                           parent, 
                                           row, 
                                           data, 
                                           children_nb, 
                                           name, 
                                           engine_path)
    
    
class GuiOutputTransfer(GuiTransfer):
  
  def __init__(self,
               it_id, 
               parent=-1, 
               row=-1,
               data=None,
               children_nb=0,
               name="no name",
               engine_path=None):
    super(GuiOutputTransfer, self).__init__(it_id, 
                                            parent, 
                                            row, 
                                            data, 
                                            children_nb, 
                                            name,
                                            engine_path)
  

