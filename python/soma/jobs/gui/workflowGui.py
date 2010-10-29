from __future__ import with_statement
from PyQt4 import QtGui, QtCore
from PyQt4 import uic
from soma.jobs.jobClient import Workflow, Group, FileSending, FileRetrieving, FileTransfer, UniversalResourcePath, JobTemplate
from soma.jobs.constants import *
import time
import threading
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
import matplotlib, numpy
matplotlib.use('Qt4Agg')
import pylab
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from Pyro.errors import ConnectionClosedError 




GRAY=QtGui.QColor(200, 200, 180)
BLUE=QtGui.QColor(0,200,255)
RED=QtGui.QColor(255,100,50)
GREEN=QtGui.QColor(155,255,50)
LIGHT_BLUE=QtGui.QColor(200,255,255)

Ui_WorkflowMainWindow = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'WorkflowMainWindow.ui' ))[0]
Ui_JobInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'JobInfo.ui' ))[0]
Ui_GraphWidget = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'graphWidget.ui' ))[0]
Ui_PlotWidget = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'PlotWidget.ui' ))[0]
Ui_TransferInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'TransferInfo.ui' ))[0]
Ui_GroupInfo = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'GroupInfo.ui' ))[0]
#Ui_TransferProgressionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'TransferProgressionDlg.ui' ))[0]
Ui_ConnectionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'connectionDlg.ui' ))[0]
Ui_FirstConnectionDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ), 'firstConnectionDlg.ui' ))[0]
Ui_WorkflowExampleDlg = uic.loadUiType(os.path.join( os.path.dirname( __file__ ),  'workflowExampleDlg.ui' ))[0]


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
  

class WorkflowWidget(QtGui.QMainWindow):
  
  def __init__(self, controler, client_model, parent = None, flags = 0):
    super(WorkflowWidget, self).__init__(parent)
    
    self.ui = Ui_WorkflowMainWindow()
    self.ui.setupUi(self)
    
    self.setCorner(QtCore.Qt.BottomLeftCorner, QtCore.Qt.LeftDockWidgetArea)
    self.setCorner(QtCore.Qt.BottomRightCorner, QtCore.Qt.RightDockWidgetArea)
    
    self.tabifyDockWidget(self.ui.dock_graph, self.ui.dock_plot)
    
    self.controler = controler
    self.model = client_model
    
    self.connect(self.model, QtCore.SIGNAL('current_connection_changed()'), self.currentConnectionChanged)
    self.connect(self.model, QtCore.SIGNAL('current_workflow_changed()'),  self.currentWorkflowChanged)
    self.connect(self.model, QtCore.SIGNAL('connection_closed_error()'), self.reconnectAfterConnectionClosed)

    self.resource_list = self.controler.getRessourceIds()
    self.ui.combo_resources.addItems(self.resource_list)
    
    self.setWindowTitle("Workflows")
   
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
    self.ui.dockWidgetContents_wf_info.setLayout(wfInfoLayout)
    
    self.graphWidget = WorkflowGraphView(self.controler, self)
    graphWidgetLayout = QtGui.QVBoxLayout()
    graphWidgetLayout.setContentsMargins(2,2,2,2)
    graphWidgetLayout.addWidget(self.graphWidget)
    self.ui.dockWidgetContents_graph.setLayout(graphWidgetLayout)
    
    self.workflowPlotWidget = WorkflowPlot(self.model, self)
    plotLayout = QtGui.QVBoxLayout()
    plotLayout.setContentsMargins(2,2,2,2)
    plotLayout.addWidget(self.workflowPlotWidget)
    self.ui.dockWidgetContents_plot.setLayout(plotLayout)
    
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
      self.updateWorkflowList()
      self.model.addWorkflow(workflow, datetime.now() + timedelta(days=5))
      
      
    
  @QtCore.pyqtSlot()
  def createWorkflowExample(self):
    worflowExample_dlg = QtGui.QDialog(self)
    ui = Ui_WorkflowExampleDlg()
    ui.setupUi(worflowExample_dlg)
    ui.comboBox_example_type.addItems(self.controler.getWorkflowExampleList())
    if worflowExample_dlg.exec_() == QtGui.QDialog.Accepted:
      with_file_transfer = ui.checkBox_file_transfers.checkState() == QtCore.Qt.Checked
      with_universal_resource_path = ui.checkBox_universal_resource_path.checkState() == QtCore.Qt.Checked
      example_type = ui.comboBox_example_type.currentIndex()
      file_path = QtGui.QFileDialog.getSaveFileName(self, "Create a workflow example");
      if file_path:
        self.controler.generateWorkflowExample(with_file_transfer, with_universal_resource_path, example_type, file_path)

  @QtCore.pyqtSlot()
  def submitWorkflow(self):
    assert(self.model.current_workflow)
    
    name = unicode(self.ui.lineedit_wf_name.text())
    if name == "": name = None
    qtdt = self.ui.dateTimeEdit_expiration.dateTime()
    date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                    qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
    while True:
      try:
        workflow = self.controler.submitWorkflow(self.model.current_workflow.server_workflow, name, date, self.model.current_connection)
      except ConnectionClosedError, e:
        if not self.reconnectAfterConnectionClosed():
          return
      else:
        break
    self.updateWorkflowList()
    self.model.addWorkflow(workflow, date) 

    
  @QtCore.pyqtSlot()
  def transferInputFiles(self):
    def transfer(self):
      try:
        self.controler.transferInputFiles(self.model.current_workflow.server_workflow, self.model.current_connection, buffer_size = 256**2)
      except ConnectionClosedError, e:
        pass
      except SystemExit, e:
        pass
    thread = threading.Thread(name = "TransferInputFiles",
                              target = transfer,
                              args =([self]))
    thread.setDaemon(True)
    thread.start()

  @QtCore.pyqtSlot()
  def transferOutputFiles(self):
    def transfer(self):
      try:
        self.controler.transferOutputFiles(self.model.current_workflow.server_workflow, self.model.current_connection, buffer_size = 256**2)
      except ConnectionClosedError, e:
        pass
      except SystemExit, e:
        pass
    thread = threading.Thread(name = "TransferOuputFiles",
                              target = transfer,
                              args =([self]))
    thread.setDaemon(True)
    thread.start()

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
      test_no = 1
      (connection, msg) = self.controler.getConnection(resource_id, login, password, test_no)
      if connection:
        return connection
      else:
        QtGui.QMessageBox.information(self, "Connection failed", msg)
    return None

  @QtCore.pyqtSlot()
  def deleteWorkflow(self):
    assert(self.model.current_workflow and self.model.current_wf_id != -1)
    
    if self.model.current_workflow.name:
      name = self.model.current_workflow.name
    else: 
      name = repr(self.model.current_wf_id)
    
    answer = QtGui.QMessageBox.question(self, "confirmation", "Do you want to delete the workflow " + name +"?", QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
    if answer != QtGui.QMessageBox.Yes: return
    while True:
      try:
        self.controler.deleteWorkflow(self.model.current_workflow.wf_id, self.model.current_connection)
      except ConnectionClosedError, e:
        if not self.reconnectAfterConnectionClosed():
          return
      else:
        break
    self.updateWorkflowList()
    self.model.deleteWorkflow()
    
  @QtCore.pyqtSlot()
  def changeExpirationDate(self):
    qtdt = self.ui.dateTimeEdit_expiration.dateTime()
    date = datetime(qtdt.date().year(), qtdt.date().month(), qtdt.date().day(), 
                    qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
    
    while True:
      try:
        change_occured = self.controler.changeWorkflowExpirationDate(self.model.current_wf_id, date,  self.model.current_connection)
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
    self.setWindowTitle("Workflows - " + self.model.current_resource_id)
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
  
      self.connect(self.model, QtCore.SIGNAL('workflow_state_changed()'), self.graphWidget.dataChanged)
      
      #=> TEMPORARY : the graph view has to be built from the clientModel
      self.graphWidget.setWorkflow(self.model.current_workflow.server_workflow, self.model.current_connection)
      
      
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
    while True:
      try:
        submittedWorflows = self.controler.getSubmittedWorkflows(self.model.current_connection)
      except ConnectionClosedError, e:
        if not self.reconnectAfterConnectionClosed():
          return
      else:
        break
    for wf_info in submittedWorflows:
      wf_id, expiration_date, workflow_name = wf_info
      if not workflow_name: workflow_name = repr(wf_id)
      self.ui.combo_submitted_wfs.addItem(workflow_name, wf_id)
    self.ui.combo_submitted_wfs.currentIndexChanged.connect(self.workflowSelectionChanged)
    
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

  def __init__(self, client_model, parent = None):
    super(WorkflowTree, self).__init__(parent)

    self.tree_view = QtGui.QTreeView(self)
    self.tree_view.setHeaderHidden(True)
    self.vLayout = QtGui.QVBoxLayout(self)
    self.vLayout.setContentsMargins(0,0,0,0)
    self.vLayout.addWidget(self.tree_view)

    self.model = client_model
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
  
  def __init__(self, client_model, parent = None):
    super(WorkflowInfo, self).__init__(parent)
    
    self.infoWidget = None
    self.vLayout = QtGui.QVBoxLayout(self)
    self.model = client_model
    
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
  
  def __init__(self, client_model, parent = None):
    super(WorkflowPlot, self).__init__(parent)
    
    self.plotWidget = None
    self.vLayout = QtGui.QVBoxLayout(self)
    self.model = client_model
    
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
  
  def __init__(self, client_model, parent = None):
    super(WorkflowElementInfo, self).__init__(parent)
    
    self.selectionModel = None
    self.infoWidget = None
    self.model = client_model # used to update stderr and stdout only
    
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
    if isinstance(item, ClientJob):
      self.infoWidget = JobInfoWidget(item, self.model.current_connection, self)
    elif isinstance(item, ClientTransfer):
      self.infoWidget = TransferInfoWidget(item, self)
    elif isinstance(item, ClientGroup):
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
 
    setLabelFromString(self.ui.job_name, self.job_item.data.name)
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
    
    setLabelFromString(self.ui.transfer_name, self.transfer_item.data.name)
    setLabelFromString(self.ui.transfer_status, self.transfer_item.transfer_status)
    setLabelFromString(self.ui.remote_path, self.transfer_item.data.remote_path)
    setLabelFromString(self.ui.local_path, self.transfer_item.data.local_path)
    
    if self.transfer_item.data.remote_paths:
      self.ui.remote_paths.insertItems(0, self.transfer_item.data.remote_paths)
    else:
      self.ui.remote_paths.clear()
    

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
    
    input_file_nb = len(self.group_item.input_to_transfer) + len(self.group_item.input_transfer_ended)
    ended_input_transfer_nb = len(self.group_item.input_transfer_ended)
    
    setLabelFromString(self.ui.status, self.group_item.status)
    setLabelFromInt(self.ui.job_nb, job_nb)
    setLabelFromInt(self.ui.ended_job_nb, ended_job_nb)
    setLabelFromTimeDelta(self.ui.total_time, total_time)
    setLabelFromTimeDelta(self.ui.theoretical_serial_time, self.group_item.theoretical_serial_time)
    setLabelFromInt(self.ui.input_file_nb, input_file_nb)
    setLabelFromInt(self.ui.ended_input_transfer_nb, ended_input_transfer_nb)
    
    if self.group_item.input_to_transfer:
      self.ui.comboBox_input_to_transfer.insertItems(0,  self.group_item.input_to_transfer)
    else:
      self.ui.comboBox_input_to_transfer.clear()
    
    if self.group_item.output_ready:
      self.ui.comboBox_output_to_transfer.insertItems(0,  self.group_item.output_ready)
    else:
      self.ui.comboBox_output_to_transfer.clear()
    

class PlotView(QtGui.QWidget):
  
  def __init__(self, group_item, parent = None):
    super(PlotView, self).__init__(parent)
    
    self.ui = Ui_PlotWidget()
    self.ui.setupUi(self)
    self.vlayout = QtGui.QVBoxLayout(self)
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
  
    self.canvas.draw()
    self.update()
 

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
      image_file_path = self.controler.printWorkflow(self.workflow, self.connection)
      image = QtGui.QImage(image_file_path)
      pixmap = QtGui.QPixmap.fromImage(image)
      self.image_label.setPixmap(pixmap)
      self.ui.scrollArea.setWidget(self.image_label)
      self.image_label.resize(self.image_label.pixmap().size()*self.scale_factor)
    else:
      self.ui.scrollArea.takeWidget()
      
########################################################
############   MODEL FOR THE TREE VIEW   ###############
########################################################
    
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
    if isinstance(item,ClientGroup):
      if role == QtCore.Qt.FontRole:
        font = QtGui.QFont()
        font.setBold(True)
        return font
      if role == QtCore.Qt.DisplayRole:
     
        return item.data.name
      else:
        #if item.status == ClientGroup.GP_NOT_SUBMITTED:
          #if role == QtCore.Qt.DecorationRole:
            #return self.standby_icon
            ##return GRAY
        if item.status == ClientGroup.GP_DONE:
          if role == QtCore.Qt.DecorationRole:
            return self.done_icon
        if item.status == ClientGroup.GP_FAILED:
          if role == QtCore.Qt.DecorationRole:
            return self.failed_icon
        if item.status == ClientGroup.GP_RUNNING:
          if role == QtCore.Qt.DecorationRole:
            return self.running_icon
    
    #### JobTemplates ####
    if isinstance(item, ClientJob): 
      if item.data.job_id == -1:
        if role == QtCore.Qt.DisplayRole:
          return item.data.name
      else:
        status = item.status
        #status = self.jobs.status(item.data.job_id)
        # not submitted
        if status == NOT_SUBMITTED:
          if role == QtCore.Qt.DisplayRole:
            return item.data.name
          if role == QtCore.Qt.DecorationRole:
            return QtGui.QIcon() #self.standby_icon
            #return GRAY
        # Done or Failed
        if status == DONE or status == FAILED:
          exit_status, exit_value, term_signal, resource_usage = item.exit_info
          if role == QtCore.Qt.DisplayRole:
            return item.data.name #+ " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal) 
          if role == QtCore.Qt.DecorationRole:
            if status == DONE and exit_status == FINISHED_REGULARLY and exit_value == 0:
              return self.done_icon
            else:
              return self.failed_icon
              
          
        # Running
        if role == QtCore.Qt.DisplayRole:
          return item.data.name + " " + status #" running..."
        if role == QtCore.Qt.DecorationRole:
          return self.running_icon
        
    #### FileTransfers ####
    if isinstance(item, ClientTransfer):
      if isinstance(item, ClientInputTransfer):
        if role == QtCore.Qt.ForegroundRole:
          return QtGui.QBrush(RED)
        if item.transfer_status == TRANSFERING or item.transfer_status == READY_TO_TRANSFER:
          display = "input: " + item.data.name + " " + repr(item.percentage_achievement) + "%"
        else:
          display = "input: " + item.data.name
      if isinstance(item, ClientOutputTransfer):
        if role == QtCore.Qt.ForegroundRole:
          return QtGui.QBrush(BLUE)
        if item.transfer_status == TRANSFERING or item.transfer_status == READY_TO_TRANSFER:
          display = "output: " + item.data.name + " " + repr(item.percentage_achievement) + "%"
        else:
          display = "output: " + item.data.name
        
      if not item.data.local_path:
        if role == QtCore.Qt.DisplayRole:
          return display
      else:
        status = item.transfer_status
        if role == QtCore.Qt.DisplayRole:
          return display + " => " + status
        if status == TRANSFER_NOT_READY:
          if role == QtCore.Qt.DecorationRole:
            return GRAY
        if status == READY_TO_TRANSFER:
          if role == QtCore.Qt.DecorationRole:
            return BLUE
        if status == TRANSFERING:
          if role == QtCore.Qt.DecorationRole:
            return GREEN
        if status == TRANSFERED:
          if role == QtCore.Qt.DecorationRole:
            return LIGHT_BLUE
    
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
        if not self.hold and self.auto_update and self.current_workflow :
            try:
              if self.current_workflow.server_workflow.wf_id == -1:
                wf_status = None
              else:
                #print " ==> communication with the server " + repr(self.server_workflow.wf_id)
                #begining = datetime.now()
                wf_status = self.current_connection.workflowStatus(self.current_workflow.server_workflow.wf_id)
                #end = datetime.now() - begining
                #print " <== end communication" + repr(self.server_workflow.wf_id) + " : " + repr(end.seconds)
            except ConnectionClosedError, e:
              self.emit(QtCore.SIGNAL('connection_closed_error()'))
              self.hold = True
            else: 
              if self.current_workflow and self.current_workflow.updateState(wf_status): 
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
    if resource_id != self.current_resource_id:
      assert(resource_id in self.connections.keys())
      self.current_resource_id = resource_id
      self.current_connection = self.connections[resource_id]
      self.emit(QtCore.SIGNAL('current_connection_changed()'))
      self.hold = False

  def reinitCurrentConnection(self, connection):
    self.current_connection = connection
    self.connections[self.current_resource_id] = connection
    self.hold = False
    
  def addWorkflow(self, workflow, expiration_date):
    '''
    Build a ClientWorkflow from a soma.jobs.jobClient.Worklfow and 
    use it as the current workflow. 
    @type worklfow: soma.jobs.jobClient.Workflow
    '''
    self.emit(QtCore.SIGNAL('current_workflow_about_to_change()'))
    self.hold = True 
    self.current_workflow = ClientWorkflow(workflow)
    self.current_wf_id = self.current_workflow.wf_id
    self.expiration_date = expiration_date
    if self.current_wf_id != -1:
      self.workflows[self.current_resource_id][self.current_workflow.wf_id] = self.current_workflow
      self.expiration_dates[self.current_resource_id][self.current_workflow.wf_id] = self.expiration_date
    try:
      wf_status = self.current_connection.workflowStatus(self.current_workflow.server_workflow.wf_id)
    except ConnectionClosedError, e:
      self.emit(QtCore.SIGNAL('connection_closed_error()'))
    else: 
      self.current_workflow.updateState(wf_status)
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
  
  def __init__(self, workflow):
    
    self.name = workflow.name 
    self.wf_id = workflow.wf_id
    
    self.ids = {} # ids => {workflow element: sequence of ids}
    self.root_id = -1 # id of the root node
    self.items = {} # items => {id : WorkflowItem}
    self.root_item = None
   
    id_cnt = 0  # unique id for the items
    
    self.server_workflow = workflow 
    self.server_jobs = {} # server job id => client job id
    self.server_file_transfers = {} # server file path => sequence of client transfer id
    
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
          if ft.local_path in self.server_file_transfers.keys():
            self.server_file_transfers[ft.local_path].append(item_id)
          else:
            self.server_file_transfers[ft.local_path] = [ item_id ]
          self.ids[ft].append(item_id)
          if ft in ref_in:
            row = ref_in.index(ft)
          else: 
            row = ref_in.index(ft.local_path)
          self.items[item_id] = ClientInputTransfer( it_id = item_id, 
                                                      parent=self.ids[job], 
                                                      row = row, 
                                                      data = ft)
          self.items[self.ids[job]].children[row]=item_id
        if ft in ref_out or ft.local_path in ref_out:
          item_id = id_cnt
          id_cnt = id_cnt + 1
          if ft.local_path in self.server_file_transfers.keys():
            self.server_file_transfers[ft.local_path].append(item_id)
          else:
            self.server_file_transfers[ft.local_path] = [ item_id ]
          self.ids[ft].append(item_id)
          if ft in ref_out:
            row = len(ref_in)+ref_out.index(ft)
          else:
            row = len(ref_in)+ref_out.index(ft.local_path)
          self.items[item_id] = ClientOutputTransfer( it_id = item_id, 
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
    #print " <== end updating jobs" + repr(self.server_workflow.wf_id) + " : " + repr(end.seconds)
    
    #print " ==> updating transfers " + repr(self.server_workflow.wf_id)
    #begining = datetime.now()
    
    #updating file transfer
    for transfer_info in wf_status[1]:
      local_file_path, complete_status = transfer_info 
      for item_id in self.server_file_transfers[local_file_path]:
        item = self.items[item_id]
        data_changed = item.updateState(complete_status) or data_changed
    
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
    
    self.status = ClientGroup.GP_NOT_SUBMITTED
    
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
    
    for child in self.children:
      item = self.client_workflow.items[child]
      if isinstance(item, ClientJob):
        # TO DO : explore files 
        if item.status == NOT_SUBMITTED:
          self.not_sub.append(item)
        elif item.status == DONE or item.status == FAILED:
          exit_status, exit_value, term_signal, resource_usage = item.exit_info
          if item.status == DONE and exit_status == FINISHED_REGULARLY and exit_value == 0:
            self.done.append(item)
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
          
      if isinstance(item, ClientGroup):
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
           
    if len(self.failed) > 0:
      new_status = ClientGroup.GP_FAILED
    elif len(self.not_sub) == 0 and len(self.failed) == 0 and len(self.running) == 0:
      new_status = ClientGroup.GP_DONE
    elif len(self.running) == 0 and len(self.done) == 0 and len(self.failed) == 0:
      new_status = ClientGroup.GP_NOT_SUBMITTED
      self.first_sub_date = None
      self.last_end_date = None
    else:
      new_status = ClientGroup.GP_RUNNING
      self.last_end_date = None
        
    state_changed = self.status != new_status
    self.status = new_status
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
    self.submission_date = None
    self.execution_date = None
    self.ending_date = None
    
    cmd_seq = []
    for command_el in data.command:
      if isinstance(command_el, tuple) and isinstance(command_el[0], FileTransfer):
        cmd_seq.append("<FileTransfer " + command_el[0].remote_path + " >")
      elif isinstance(command_el, FileTransfer):
        cmd_seq.append("<FileTransfer " + command_el.remote_path + " >")
      elif isinstance(command_el, UniversalResourcePath):
        cmd_seq.append("<UniversalResourcePath " + command_el.namespace + " " + command_el.uuid + " " +  command_el.relative_path + " >")
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
          if ruel[0] == "start_time":
            t = time.localtime(float(ruel[1].replace(',', '.')))
            self.execution_date = datetime(year = t[0], month = t[1], day = t[2], hour = t[3], minute = t[4], second = t[5])
          elif ruel[0] == "end_time":
            t = time.localtime(float(ruel[1].replace(',', '.')))
            self.ending_date = datetime(year = t[0], month = t[1], day = t[2], hour = t[3], minute = t[4], second = t[5])
          elif ruel[0] == "submission_time":
            t = time.localtime(float(ruel[1].replace(',', '.')))
            self.submission_date = datetime(year = t[0], month = t[1], day = t[2], hour = t[3], minute = t[4], second = t[5])
        
        
    return state_changed
    
  def updateStdOutErr(self, connection):
    if self.data:
      stdout_path = "/tmp/soma_workflow_stdout"
      stderr_path = "/tmp/soma_workflow_stderr"
      connection.retrieveStdOutErr(self.data.job_id, stdout_path, stderr_path)
      
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
      self.stderr =stderr
      f.close()
      
      
class ClientTransfer(ClientWorkflowItem):
  
  DIRECTORY = "directory"
  FILE = "file"
  
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0 ):
    super(ClientTransfer, self).__init__(it_id, parent, row, data, children_nb)
    
    self.transfer_status = " "
    self.size = None
    self.transmitted = None
    self.elements_status = None
    
    self.percentage_achievement = 0
    self.transfer_type = None

  
  def updateState(self, transfer_status_info):
    self.initiated = True
    state_changed = False
    transfer_status = transfer_status_info[0]
    
    if transfer_status_info[1]:
      if len(transfer_status_info[1]) == 2:
        self.transfer_type = ClientTransfer.FILE
        size, transmitted = transfer_status_info[1]
        elements_status = None
      elif len(transfer_status_info[1]) == 3:
        self.transfer_type = ClientTransfer.DIRECTORY
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
  
  

class ClientInputTransfer(ClientTransfer):
  
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0 ):
    super(ClientInputTransfer, self).__init__(it_id, parent, row, data, children_nb)
    
class ClientOutputTransfer(ClientTransfer):
  
  def __init__(self,
               it_id, 
               parent = -1, 
               row = -1,
               data = None,
               children_nb = 0 ):
    super(ClientOutputTransfer, self).__init__(it_id, parent, row, data, children_nb)

