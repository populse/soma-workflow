# -*- coding: utf-8 -*-

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from __future__ import with_statement, print_function
from __future__ import absolute_import

import time
import threading
import os
from datetime import datetime
from datetime import timedelta
import socket
import weakref
from soma_workflow import subprocess
from soma_workflow import connection
import sys
import traceback
# import cProfile
# import pdb

PYQT4 = "pyqt4"
PYQT5 = "pyqt5"
PYQT6 = "pyqt6"
PYSIDE = "pyside"
QT_BACKEND = None

if 'PyQt4' in sys.modules:
    QT_BACKEND = PYQT4
elif 'PyQt6' in sys.modules:
    QT_BACKEND = PYQT6
elif 'PyQt5' in sys.modules:
    QT_BACKEND = PYQT5
elif 'PySide' in sys.modules:
    QT_BACKEND = PYSIDE

if QT_BACKEND is None:
    qt_api = os.environ.get('QT_API')
    if qt_api == 'pyqt6':
        QT_BACKEND = PYQT6
    elif qt_api == 'pyqt5':
        QT_BACKEND = PYQT5
    elif qt_api in ('pyqt4', 'pyqt'):
        QT_BACKEND = PYQT4
    elif qt_api == 'pyside':
        QT_BACKEND = PYSIDE

if QT_BACKEND is None:
    try:
        from PyQt6 import QtGui, QtCore, QtWidgets
        QT_BACKEND = PYQT6
    except ImportError as e:
        try:
            from PyQt5 import QtGui, QtCore, QtWidgets
            QT_BACKEND = PYQT5
        except ImportError as e:
            raise Exception("Soma-workflow Gui requires PyQt4, PyQt5, PyQt6 or PySide.")

if QT_BACKEND is None:
    try:
        import sip
        sip_classes = ['QString', 'QVariant', 'QDate', 'QDateTime',
                       'QTextStream', 'QTime', 'QUrl', 'QStringList']
        for sip_class in sip_classes:
            try:
                sip.setapi(sip_class, 2)
            except ValueError as e:
                pass
        from PyQt4 import QtGui, QtCore  # noqa: F811
        QT_BACKEND = PYQT4
    except ImportError as e:
        pass

if QT_BACKEND is None:
    try:
        from PySide import QtGui, QtCore  # noqa: F811
        QT_BACKEND = PYSIDE
    except ImportError as e:
        pass

use_qvariant = False

if QT_BACKEND == PYQT4:
    import sip
    from PyQt4 import QtCore, QtGui, uic  # noqa: F811
    from PyQt4.uic import loadUiType
    use_qvariant = False
    if sip.getapi('QVariant') < 2:
        use_qvariant = True
    QtCore.Slot = QtCore.pyqtSlot
    QtCore.Signal = QtCore.pyqtSignal

elif QT_BACKEND == PYQT5:
    from PyQt5 import QtCore, QtGui, QtWidgets, uic  # noqa: F811
    from PyQt5.uic import loadUiType
    QtCore.Slot = QtCore.pyqtSlot
    QtCore.Signal = QtCore.pyqtSignal
    # copy QtWidgets contents into QtGui
    for key in QtWidgets.__dict__:
        if not key.startswith('__') and key not in QtGui.__dict__:
            setattr(QtGui, key, getattr(QtWidgets, key))
    # more hacks
    QtGui.QSortFilterProxyModel = QtCore.QSortFilterProxyModel
    QtGui.QItemSelectionModel = QtCore.QItemSelectionModel
    try:
        from PyQt5 import sip
        sys.modules['sip'] = sip
    except ImportError:
        import sip

elif QT_BACKEND == PYQT6:
    from PyQt6 import QtCore, QtGui, QtWidgets, uic  # noqa: F811
    from PyQt6.uic import loadUiType
    QtCore.Slot = QtCore.pyqtSlot
    QtCore.Signal = QtCore.pyqtSignal
    # copy QtWidgets contents into QtGui
    for key in QtWidgets.__dict__:
        if not key.startswith('__') and key not in QtGui.__dict__:
            setattr(QtGui, key, getattr(QtWidgets, key))
    # more hacks
    QtGui.QSortFilterProxyModel = QtCore.QSortFilterProxyModel
    QtGui.QItemSelectionModel = QtCore.QItemSelectionModel
    try:
        from PyQt6 import sip
        sys.modules['sip'] = sip
    except ImportError:
        import sip

elif QT_BACKEND == PYSIDE:

    from PySide import QtCore, QtGui, QtUiTools


from soma_workflow.client import Workflow, Group, FileTransfer, SharedResourcePath, TemporaryPath, Job, WorkflowController, Helper
from soma_workflow.engine_types import EngineWorkflow, EngineJob, EngineTransfer
import soma_workflow.constants as constants
import soma_workflow.configuration as configuration
from soma_workflow.test.workflow_tests import WorkflowExamples
from soma_workflow.test.workflow_tests import WorkflowExamplesLocal
from soma_workflow.test.workflow_tests import WorkflowExamplesShared
from soma_workflow.test.workflow_tests import WorkflowExamplesSharedTransfer
from soma_workflow.test.workflow_tests import WorkflowExamplesTransfer
from soma_workflow.errors import UnknownObjectError, ConfigurationError, SerializationError, WorkflowError, JobError, ConnectionError
import soma_workflow.version as version

import six
from six.moves import range

def utf8(string):
    """Convert a Unicode or utf-8 encoded string to a native Python str.
    
    This function's name is misleading: while on Python 2 it really
    returns a utf-8-encoded string, on Python 3 it return a native
    Unicode Python str.
    
    This function is robust to incorrectly-encoded utf-8 strings, and
    will replace undecodable sequences with the Unicode
    U+FFFD REPLACEMENT CHARACTER.
    """
    if isinstance(string, bytes):
        unicode_string = string.decode('utf-8', errors='replace')
    else:
        unicode_string = six.text_type(string)  # to handle QString
    return six.ensure_str(unicode_string, 'utf-8')


MATPLOTLIB = True
try:
    import matplotlib
    if QT_BACKEND == PYQT6:
        matplotlib.use('Qt5Agg')  # apparently not Qt6Agg
    elif QT_BACKEND == PYQT5:
        matplotlib.use('Qt5Agg')
    elif QT_BACKEND == PYQT4:
        matplotlib.use('Qt4Agg')
    elif QT_BACKEND == PYSIDE:
        matplotlib.use('Qt5Agg')
        if 'backend.qt4' in list(matplotlib.rcParams.keys()):
            matplotlib.rcParams['backend.qt4'] = 'PySide'
        else:
            print("Could not use Matplotlib, the backend using PySide "
                "is missing.")
            MATPLOTLIB = False
    if QT_BACKEND in (PYQT4, ):
        from matplotlib.backends.backend_qt4agg \
            import FigureCanvasQTAgg as FigureCanvas
    else:
        from matplotlib.backends.backend_qt5agg \
            import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    import matplotlib.pyplot
    import matplotlib.dates as mdates
except ImportError as e:
    print("Could not use Matplotlib: %s %s" % (type(e), e))
    MATPLOTLIB = False

# from soma.utils.sip_compat:

import inspect
import enum


def sip4_to_sip6_enums(module, recursive=True):
    ''' Convert Sip4 style enums to sip6 style enums.

    Sip4 exports C++ enums as in C++, ex::

        instance.ENUM_VALUE  # where ENUM_VALUE is an instance of EnumType

    Sip6 keeps values inside the enum type::

        instance.EnumType.ENUM_VALUE

    In order to maintain code compatible, we duplicat these values. If using
    sip4, then we copy values inside the enum types as sip6 does.

    If recursive (the default), sub-modules are also scanned and modified.
    '''
    todo = [module]
    done = set()
    while todo:
        module = todo.pop(0)
        if id(module) in done:
            continue

        done.add(id(module))
        enums = set()
        for iname, item in module.__dict__.items():
            if type(item).__name__ == 'enumtype' and id(item) not in done:
                enums.add(item)
                done.add(id(item))
            elif inspect.isclass(item) or (recursive
                                           and inspect.ismodule(item)):
                todo.append(item)
        if enums:
            for iname, item in module.__dict__.items():
                if type(item) in enums:
                    setattr(type(item), iname, item)


def sip6_to_sip4(module, recursive=True):
    ''' Convert Sip6 style enums to sip4 style enums.

    Sip4 exports C++ enums as in C++, ex::

        instance.ENUM_VALUE  # where ENUM_VALUE is an instance of EnumType

    Sip6 keeps values inside the enum type::

        instance.EnumType.ENUM_VALUE

    In order to maintain code compatible, we duplicat these values. If using
    sip6, then we copy values outside the enum types as sip4 does.

    If recursive (the default), sub-modules are also scanned and modified.
    '''
    todo = [module]
    done = set()
    while todo:
        module = todo.pop(0)
        if id(module) in done:
            continue

        done.add(id(module))
        enums = []
        for iname, item in module.__dict__.items():
            if type(item) is enum.EnumMeta and id(item) not in done:
                enums.append(item)
                done.add(id(item))
            elif inspect.isclass(item) or (recursive
                                           and inspect.ismodule(item)):
                todo.append(item)
        for entype in enums:
            for name in entype._member_names_:
                setattr(module, name, getattr(entype, name))


def sip_export_enums(module, recursive=True):
    ''' Convert Sip6 style enums to sip4 style enums, or the contrary.

    Sip4 exports C++ enums as in C++, ex::

        instance.ENUM_VALUE  # where ENUM_VALUE is an instance of EnumType

    Sip6 keeps values inside the enum type::

        instance.EnumType.ENUM_VALUE

    In order to maintain code compatible, we duplicat these values. If using
    sip6, then we copy values outside the enum types as sip4 does. If using
    sip4, then we copy values inside the enum types as sip6 does.

    If recursive (the default), sub-modules are also scanned and modified.
    '''
    sip = sys.modules['sip']
    if sip.SIP_VERSION >= 0x060000:
        sip6_to_sip4(module, recursive=recursive)
    else:
        sip4_to_sip6_enums(module, recursive=recursive)

# ---

sip_export_enums(QtCore)
sip_export_enums(QtGui)
sip_export_enums(QtWidgets)


class ProtocolError(Exception):
    pass


class ConnectionClosedError(ProtocolError):
    pass


# QFileDialog options:
# in binary packages containing thirdpary libs/python, we must use 
# DontUseNativeDialog to prevent loading external libs from the system.
if QT_BACKEND == PYQT6:
    filedialog_options = QtGui.QFileDialog.Option.DontUseNativeDialog
else:
    filedialog_options = QtGui.QFileDialog.DontUseNativeDialog

#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

NOT_SUBMITTED_WF_ID = -1
NOT_SUBMITTED_JOB_ID = -1

GRAY = QtGui.QColor(200, 200, 180)
BLUE = QtGui.QColor(0, 200, 255)
RED = QtGui.QColor(255, 100, 50)
GREEN = QtGui.QColor(155, 255, 50)
LIGHT_BLUE = QtGui.QColor(200, 255, 255)


if QT_BACKEND in (PYQT4, PYQT5, PYQT6):
    '''
    The types can be loaded directly from the ui files (useful during developpement)
    '''
    Ui_JobInfo = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                             'JobInfo.ui'))[0]

    Ui_GraphWidget = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                 'graphWidget.ui'))[0]

    Ui_PlotWidget = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                'PlotWidget.ui'))[0]

    Ui_TransferInfo = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                  'TransferInfo.ui'))[0]

    Ui_GroupInfo = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                               'GroupInfo.ui'))[0]

    Ui_ConnectionDlg = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                   'connection_dlg.ui'))[0]

    Ui_WorkflowExampleDlg = uic.loadUiType(
        os.path.join(os.path.dirname(__file__),
                     'workflowExampleDlg.ui'))[0]

    Ui_SubmissionDlg = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                   'submissionDlg.ui'))[0]

    Ui_ResourceWfSelect = uic.loadUiType(
        os.path.join(os.path.dirname(__file__),
                     'resource_wf_select.ui'))[0]

    Ui_MainWindow = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                'main_window.ui'))[0]

    Ui_WStatusNameDate = uic.loadUiType(
        os.path.join(os.path.dirname(__file__),
                     'wf_status_name_date.ui'))[0]

    Ui_SWMiniWidget = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                  'sw_mini.ui'))[0]

    Ui_SearchWidget = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                                  'search_widget.ui'))[0]

    Ui_LocalSchedulerConfigController = uic.loadUiType(
        os.path.join(os.path.dirname(__file__),
                     'local_scheduler_widget.ui'))[0]

    Ui_WorkflowEngineConfigController = uic.loadUiType(
        os.path.join(os.path.dirname(__file__),
                     'engine_controller_widget.ui'))[0]

    Ui_ServerManagement = uic.loadUiType(
        os.path.join(os.path.dirname(__file__),
                     'ServerManagement.ui'))[0]

    Ui_NewServer = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                               'ServerNewDlg.ui'))[0]

    Ui_RequirePW = uic.loadUiType(os.path.join(os.path.dirname(__file__),
                                               'RequirePW.ui'))[0]

else:
    from soma_workflow.gui.ui_job_info import Ui_JobInfo
    from soma_workflow.gui.ui_graph_widget import Ui_GraphWidget
    from soma_workflow.gui.ui_plot_widget import Ui_PlotWidget
    from soma_workflow.gui.ui_transfer_info import Ui_TransferInfo
    from soma_workflow.gui.ui_group_info import Ui_GroupInfo
    from soma_workflow.gui.ui_connection_dlg import Ui_ConnectionDlg
    from soma_workflow.gui.ui_workflow_example_dlg import Ui_WorkflowExampleDlg
    from soma_workflow.gui.ui_submission_dlg import Ui_SubmissionDlg
    from soma_workflow.gui.ui_resource_wf_select import Ui_ResourceWfSelect
    from soma_workflow.gui.ui_main_window import Ui_MainWindow
    from soma_workflow.gui.ui_wf_status_name_date import Ui_WStatusNameDate
    from soma_workflow.gui.ui_sw_mini_widget import Ui_SWMiniWidget
    from soma_workflow.gui.ui_search_widget import Ui_SearchWidget
    from soma_workflow.gui.ui_local_scheduler_cfg_ctrl import Ui_LocalSchedulerConfigController
    from soma_workflow.gui.ui_workflow_engine_cfg_ctrl import Ui_WorkflowEngineConfigController
    # from soma_workflow.gui.ui_server_management import Ui_ServerManagement
    # from soma_workflow.gui.ui_new_server import Ui_NewServer
    # Ui_RequirePW


#-----------------------------------------------------------------------------
# Local utilities
#-----------------------------------------------------------------------------
def setLabelFromString(label, value):
    '''
    @type value: string
    '''
    if value == label.text():
        return
    if value:
        label.setText(utf8(value))
    else:
        label.setText("")


def setTextEditFromString(text_edit, value):
    '''
    @type value: string
    '''
    if value == text_edit.toPlainText():
        return
    if value:
        text_edit.setText(utf8(value))
    else:
        text_edit.setText("")


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
        hours = value.seconds // 3600
        mins = (value.seconds % 3600) // 60
        seconds = (value.seconds % 3600) % 60
        hours = hours + value.days * 24
        label.setText("%s:%s:%s" % (repr(hours), repr(mins), repr(seconds)))
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


def workflow_status_icon(status=None):
    '''
    return the icon file path
    '''
    file_path = None
    if status == None:
        file_path = None
    elif status == constants.WORKFLOW_NOT_STARTED:
        file_path = os.path.join(
            os.path.dirname(__file__), "icon/no_status.png")
    elif status == constants.WORKFLOW_IN_PROGRESS:
        file_path = os.path.join(os.path.dirname(__file__), "icon/running.png")
    elif status == constants.WORKFLOW_DONE:
        file_path = os.path.join(os.path.dirname(__file__), "icon/done.png")
    elif status == constants.DELETE_PENDING:
        file_path = os.path.join(
            os.path.dirname(__file__), "icon/kill_delete_pending.png")
    elif status == constants.WARNING:
        file_path = os.path.join(os.path.dirname(__file__), "icon/warning.png")
    return file_path


class QResizeMessageBox(QtGui.QMessageBox):
    def __init__(self, *args, **kwargs):
        QtGui.QMessageBox.__init__(self, *args, **kwargs)
        self.setSizeGripEnabled(True)

    def event(self, e):
        result = QtGui.QMessageBox.event(self, e)

        self.setMinimumHeight(0)
        self.setMaximumHeight(16777215)
        self.setMinimumWidth(0)
        self.setMaximumWidth(16777215)
        self.setSizePolicy(QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Expanding)

        textEdit = self.findChild(QtGui.QTextEdit)
        if textEdit != None :
            textEdit.setMinimumHeight(0)
            textEdit.setMaximumHeight(16777215)
            textEdit.setMinimumWidth(0)
            textEdit.setMaximumWidth(16777215)
            textEdit.setSizePolicy(QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Expanding)

        return result


def detailed_critical_message_box(msg, title, parent):
    long_msg_indic = "**More details:**"
    if long_msg_indic in msg:
        indic_index = msg.index(long_msg_indic)
        long_msg = msg[indic_index + len(long_msg_indic):]
        short_msg = msg[:indic_index]
        message_box = QResizeMessageBox(parent)
        message_box.setIcon(QtGui.QMessageBox.Critical)
        message_box.setWindowTitle(title)
        message_box.setText(short_msg)
        message_box.setDetailedText(long_msg)
        message_box.setSizeGripEnabled(True)
        message_box.setSizePolicy(QtGui.QSizePolicy.Expanding,
                                  QtGui.QSizePolicy.Expanding)
        message_box.exec()
    else:
        QResizeMessageBox.critical(parent, "error", "%s" % (msg))


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------
class Controller(object):

    @staticmethod
    def delete_workflow(wf_id,
                        force,
                        wf_ctrl):
        return wf_ctrl.delete_workflow(wf_id, force)

    @staticmethod
    def delete_all_workflows(force,
                             wf_ctrl):
        return Helper.delete_all_workflows(wf_ctrl, force)

    @staticmethod
    def stop_workflow(wf_id,
                      wf_ctrl):
        return wf_ctrl.stop_workflow(wf_id)

    @staticmethod
    def change_workflow_expiration_date(wf_id,
                                        date,
                                        wf_ctrl):
        return wf_ctrl.change_workflow_expiration_date(wf_id, date)

    @staticmethod
    def get_submitted_workflows(wf_ctrl):
        if wf_ctrl:
            return wf_ctrl.workflows()
        else:
            return {}

    @staticmethod
    def restart_workflow(workflow_id, queue, wf_ctrl):
        return wf_ctrl.restart_workflow(workflow_id, queue)

    @staticmethod
    def stop_jobs(wf_id, job_ids, wf_ctrl):
        return wf_ctrl.stop_jobs(wf_id, job_ids)

    @staticmethod
    def restart_jobs(wf_id, job_ids, wf_ctrl):
        return wf_ctrl.restart_jobs(wf_id, job_ids)

    @staticmethod
    def get_connection(resource_id,
                       login,
                       password,
                       rsa_key_pass,
                       config=None,
                       isolated_light_mode=None):
        print("Lauching workflow controller")
        wf_ctrl = WorkflowController(resource_id=resource_id,
                                     login=login,
                                     password=password,
                                     config=config,
                                     rsa_key_pass=rsa_key_pass,
                                     isolated_light_mode=isolated_light_mode)
        return wf_ctrl

    @staticmethod
    def disconnect(wf_ctrl):
        wf_ctrl.disconnect()

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
    def submit_workflow(workflow, expiration_date, name, queue, wf_ctrl):
        wf_id = wf_ctrl.submit_workflow(
            workflow=workflow,
            expiration_date=expiration_date,
            name=name,
            queue=queue)
        # TEST
        # return wf_id
        workflow = wf_ctrl.workflow(wf_id)
        return workflow

    @staticmethod
    def transfer_input_files(workflow_id, wf_ctrl, buffer_size=256 ** 2):
        Helper.transfer_input_files(workflow_id, wf_ctrl, buffer_size)

    @staticmethod
    def transfer_output_files(workflow_id, wf_ctrl, buffer_size=256 ** 2):
        Helper.transfer_output_files(workflow_id, wf_ctrl, buffer_size)


class SomaWorkflowMiniWidget(QtGui.QWidget):

    # SomaWorkflowWidget
    sw_widget = None

    # soma_workflow.gui.WorkflowGui.ApplicationModel
    model = None

    action_show_more_less = None

    def __init__(self, model, sw_widget, parent=None):
        super(SomaWorkflowMiniWidget, self).__init__(parent)

        self.ui = Ui_SWMiniWidget()
        self.ui.setupUi(self)

        self.sw_widget = sw_widget

        self.model = model

        self.resource_ids = []

        self.action_add_resource = QtGui.QAction("Add a resource", self)
        self.addAction(self.action_add_resource)

        self.action_disconnect_resource = QtGui.QAction(
            "Disconnect a resource", self)
        self.addAction(self.action_disconnect_resource)
        self.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)

        self.ui.table.doubleClicked.connect(self.sw_widget.show)
        self.ui.table.doubleClicked.connect(self.raise_sw_widget)
        self.action_add_resource.triggered.connect(self.add_resource)
        self.action_disconnect_resource.triggered.connect(
            self.disconnect_resource)

        self.ui.add_resource_tool_button.setDefaultAction(
            self.action_add_resource)
        self.ui.add_resource_tool_button.setText("Add")

        self.ui.disconnect_resource_tool_button.setDefaultAction(
            self.action_disconnect_resource)
        self.ui.disconnect_resource_tool_button.setText("Disconnect")

        self.model.global_workflow_state_changed.connect(self.refresh)
        self.model.current_connection_changed.connect(self.connection_changed)
        self.ui.table.itemSelectionChanged.connect(
            self.resource_selection_changed)

        self.connection_changed()

    @QtCore.Slot()
    def raise_sw_widget(self):
        self.sw_widget.raise_()

    @QtCore.Slot()
    def resource_selection_changed(self):
        selected_items = self.ui.table.selectedItems()
        if len(selected_items) == 0:
            return
        item = selected_items[0]
        if use_qvariant:
            rid = utf8(six.text_type(
                item.data(QtCore.Qt.UserRole).toString()))
        else:
            rid = utf8(item.data(QtCore.Qt.UserRole))
        # print('resource_selection_changed:', repr(rid))
        self.model.set_current_connection(rid)

    @QtCore.Slot()
    def connection_changed(self):
        if self.model.current_resource_id == None:
            return
        submitted_wf = []
        if self.model.current_resource_id not in self.resource_ids:
            while True:
                try:
                    submitted_wf = Controller.get_submitted_workflows(
                        self.model.current_connection)
                except ConnectionClosedError as e:
                    if not self.reconnectAfterConnectionClosed():
                        return
                else:
                    break
            workflow_ids = self.sw_widget.workflow_filter(submitted_wf)

            for wf_id in workflow_ids:
                if self.model.is_loaded_workflow(wf_id):
                    self.model.set_current_workflow(wf_id)
                else:
                    workflow_info_dict \
                        = self.model.current_connection.workflows(
                            [wf_id])
                    if len(workflow_info_dict) > 0:
                        # The workflow exist
                        workflow_status \
                            = self.model.current_connection.workflow_status(
                                wf_id)
                        workflow_info = workflow_info_dict[wf_id]
                        self.model.add_to_submitted_workflows(
                            wf_id,
                            workflow_exp_date=workflow_info[1],
                            workflow_name=workflow_info[0],
                            workflow_status=workflow_status)
            self.resource_ids.append(self.model.current_resource_id)
            self.refresh()

        self.ui.table.selectRow(
            self.resource_ids.index(self.model.current_resource_id))
        self.model.set_no_current_workflow()

    @QtCore.Slot()
    def add_resource(self):
        (resource_id, new_connection) = self.sw_widget.createConnection()
        if new_connection:
            self.model.add_connection(resource_id, new_connection)

    @QtCore.Slot()
    def disconnect_resource(self):
        selected = self.ui.table.selectedItems()
        resources = []
        for item in selected:
            if use_qvariant:
                rid = utf8(six.text_type(
                    item.data(QtCore.Qt.UserRole).toString()))
            else:
                rid = utf8(item.data(QtCore.Qt.UserRole))
            if rid and rid != "None":
                resources.append(rid)
        if len(resources) >= len(self.resource_ids):
            QResizeMessageBox.warning(
                self, "Cannot disconnect all resources",
                "We must keep at least one valid resource.")
            return
        if len(resources) != 0:
            resp = QResizeMessageBox.question(
                self, "Disconnect the following resources ?",
                "\n".join(resources),
                QtGui.QMessageBox.Ok | QtGui.QMessageBox.Cancel)
            if resp == QtGui.QMessageBox.Ok:
                for resource_id in resources:
                    if resource_id in self.resource_ids:
                        self.resource_ids.remove(resource_id)
                    self.model.delete_connection(resource_id)

    @QtCore.Slot()
    def refresh(self):
        try:
            self.ui.table.clear()
            self.ui.table.setColumnCount(3)
            self.ui.table.setRowCount(len(self.resource_ids))
            row = 0
            for rid in self.resource_ids:
                if not rid in self.model.resource_pool.resource_ids():
                    self.resource_ids.remove(rid)
                    continue
                status_list = self.model.list_workflow_status(rid)
                running = status_list.count(constants.WORKFLOW_IN_PROGRESS)
                warning = status_list.count(constants.WARNING)
                to_display = ""
                if running == 0 and warning == 0:
                    icon = QtGui.QIcon(
                        os.path.join(os.path.dirname(__file__), "icon/done.png"))
                elif warning > 0:
                    icon = QtGui.QIcon(
                        os.path.join(os.path.dirname(__file__), "icon/warning.png"))
                    if warning == 1:
                        to_display = to_display + " (1 warning)"
                    else:
                        to_display = to_display + \
                            " (" + repr(warning) + " warnings)"
                    if running == 1:
                        to_display = to_display + " (1 is running)"
                    elif running > 1:
                        to_display = to_display + \
                            " (" + repr(running) + " are running)"
                else:
                    icon = QtGui.QIcon(
                        os.path.join(os.path.dirname(__file__), "icon/running.png"))
                    if running == 1:
                        to_display = to_display + " (1 is running)"
                    else:
                        to_display = to_display + \
                            " (" + repr(running) + " are running)"

                item = QtGui.QTableWidgetItem(rid + " ")
                item.setData(QtCore.Qt.UserRole, rid)
                self.ui.table.setItem(row, 0, item)
                self.ui.table.setItem(row, 1,  QtGui.QTableWidgetItem(
                    icon, repr(len(status_list)) + " workflows" + to_display))
                resource = self.model.resource_pool.connection(rid)
                if resource.config.get_scheduler_type() \
                        == configuration.LOCAL_SCHEDULER:
                    scheduler_config = resource.scheduler_config
                    if not scheduler_config and resource.engine_config_proxy:
                        scheduler_config \
                            = self.model.current_connection.scheduler_config
                    if scheduler_config:
                        scheduler_widget = LocalSchedulerConfigController(
                            scheduler_config,
                            self)
                        self.ui.table.setCellWidget(row, 2, scheduler_widget)
                        self.ui.table.resizeColumnToContents(2)
                elif resource.engine_config_proxy.get_queue_limits() \
                        or resource.engine_config_proxy.get_running_jobs_limits():
                    controller_widget = WorkflowEngineConfigController(
                        resource.engine_config_proxy,
                        self)
                    self.ui.table.setCellWidget(row, 2, controller_widget)
                    self.ui.table.resizeColumnToContents(2)

                row = row + 1
            if self.model.current_resource_id != None and \
              self.model.current_resource_id in self.resource_ids:
                self.ui.table.selectRow(
                    self.resource_ids.index(self.model.current_resource_id))
            self.ui.table.resizeColumnsToContents()
        except Exception as e:
            print('exception in Qt slot:', e, file=sys.stderr)
            traceback.print_exc()


class LocalSchedulerConfigController(QtGui.QWidget):

    scheduler_config = None

    def __init__(self, scheduler_config, parent=None):
        super(LocalSchedulerConfigController, self).__init__(parent)

        self.ui = Ui_LocalSchedulerConfigController()
        self.ui.setupUi(self)

        self.scheduler_config = scheduler_config

        cpu_count = scheduler_config.get_cpu_count()
        self.ui.advice_label.setText(" " + repr(cpu_count) + " CPUs detected")
        proc_nb = scheduler_config.get_proc_nb()
        if proc_nb is None:
            self.ui.spin_box.setValue(-1)
        else:
            self.ui.spin_box.setValue(proc_nb)
        max_proc_nb = scheduler_config.get_max_proc_nb()
        if max_proc_nb is None:
            self.ui.max_spin_box.setValue(0)
        else:
            self.ui.max_spin_box.setValue(max_proc_nb)

        self.ui.spin_box.valueChanged.connect(self.nb_proc_changed)
        self.ui.max_spin_box.valueChanged.connect(self.max_proc_changed)

    def nb_proc_changed(self, nb_proc):
        self.scheduler_config.set_proc_nb(nb_proc)

    def max_proc_changed(self, nb_proc):
        self.scheduler_config.set_max_proc_nb(nb_proc)


class WorkflowEngineConfigController(QtGui.QWidget):

    engine_config = None
    queue_limits = None
    running_jobs_limits = None

    def __init__(self, engine_config, parent=None):
        super(WorkflowEngineConfigController, self).__init__(parent)

        self.ui = Ui_WorkflowEngineConfigController()
        self.ui.setupUi(self)

        self.engine_config = engine_config

        self.queue_limits = self.engine_config.get_queue_limits()
        self.running_jobs_limits = self.engine_config.get_running_jobs_limits()

        queues = ['default'] + sorted([x
                                       for x in self.engine_config.get_queues()
                                       if x is not None])
        if 'default' in queues[1:]:
            del queues[queues.index('default', 1)]

        for queue_name in queues:
            if queue_name is not None:
                self.ui.combo_queue.addItem(queue_name)

        self.ui.combo_queue.currentIndexChanged.connect(self.update_limit)
        self.ui.limit.valueChanged.connect(self.limit_changed)
        self.ui.max_running.valueChanged.connect(self.max_running_changed)

        self.update_limit()

    def update_limit(self):
        self.ui.limit.blockSignals(True)
        self.ui.max_running.blockSignals(True)
        queue_name = utf8(self.ui.combo_queue.currentText())
        if queue_name == "default":
            queue_name = None
        self.ui.limit.setValue(self.queue_limits.get(queue_name, 0))
        self.ui.max_running.setValue(self.running_jobs_limits.get(
            queue_name, 0))
        self.ui.limit.blockSignals(False)
        self.ui.max_running.blockSignals(False)

    def limit_changed(self, limit):
        queue_name = utf8(self.ui.combo_queue.currentText())
        if queue_name == "default":
            queue_name = None
        self.engine_config.change_queue_limits(queue_name, limit)
        self.queue_limits = self.engine_config.get_queue_limits()

    def max_running_changed(self, limit):
        queue_name = utf8(self.ui.combo_queue.currentText())
        if queue_name == "default":
            queue_name = None
        self.engine_config.change_running_jobs_limits(queue_name, limit)
        self.running_jobs_limits = self.engine_config.get_running_jobs_limits()


class RequirePWDialog(QtGui.QDialog):

    ui = None
    is_install = False
    strPW = None
    strRSAPW = None

    def __init__(self,    parent=None):
        super(RequirePWDialog, self).__init__(parent=parent)
        self.ui = Ui_RequirePW()
        self.ui.setupUi(self)
        self.ui.pushButton_ok.clicked.connect(self.EventOK)

    def EventOK(self):
        self.strPW = self.ui.lineEdit_PW.text()
        self.strPW = utf8(self.strPW)

        self.strRSAPW = self.ui.lineEdit_RSAPW.text()
        self.strRSAPW = utf8(self.strRSAPW)

        self.close()
        pass


class NewServerDialog(QtGui.QDialog):

    ui = None
    is_install = False

    def __init__(self,    parent=None):
        super(NewServerDialog, self).__init__(parent=parent)
        self.ui = Ui_NewServer()
        self.ui.setupUi(self)
        self.update_schedulers()
        self.ui.lineEdit_login.textChanged.connect(self.EventLoginTextChanged)
        self.ui.lineEdit_cluster_add.textChanged.connect(self.UpdateResName)

        self.ui.pushButton_Install.clicked.connect(self.InstallServer)
        self.ui.pushButton_Connect.clicked.connect(
            self.SetupServerNoInstallation)
#      from soma_workflow.setup_client2server import GetHomeDirOnServer
#      GetHomeDirOnServer()

    def EventLoginTextChanged(self):
        self.UpdateResName()
        self.UpdateInstallationPath()

    def UpdateResName(self):
        strLogin = self.ui.lineEdit_login.text()
        strLogin = utf8(strLogin)

        strAdd = self.ui.lineEdit_cluster_add.text()
        strAdd = utf8(strAdd)

        ResName = strLogin + "@" + strAdd
        self.ui.lineEdit_ResName.setText(ResName)

    def UpdateInstallationPath(self):
        strLogin = self.ui.lineEdit_login.text()
        strLogin = utf8(strLogin)
        self.ui.lineEdit_InstallPath.setText(
            "/home/" + strLogin + "/.soma-workflow")

    def update_schedulers(self):
        from soma_workflow import scheduler
        schedulers = scheduler.get_schedulers_list()
        for scheduler_inst in schedulers:
            # add only non-builtin
            if scheduler_inst[0] not in ('local_basic', 'mpi', 'drmaa'):
                self.ui.comboBox_schedulerType.addItem(scheduler_inst[0])

    def InstallServer(self):
        from soma_workflow.setup_client2server import InstallSomaWF2Server, check_if_somawfdb_on_server

        strLogin = self.ui.lineEdit_login.text()
        strLogin = utf8(strLogin)

        strPort = self.ui.lineEdit_Port.text()
        strPort = utf8(strPort)
        intPort = int(strPort)

        strAdd = self.ui.lineEdit_cluster_add.text()
        strAdd = utf8(strAdd)

        ResName = self.ui.lineEdit_ResName.text()
        ResName = utf8(ResName)

        strPW = self.ui.lineEdit_PW.text()
        strPW = utf8(strPW)

        strPWRSA = self.ui.lineEdit_RSAKeyPW.text()
        strPWRSA = utf8(strPWRSA)

        strInstallPath = self.ui.lineEdit_InstallPath.text()
        strInstallPath = utf8(strInstallPath)

        options = {}

        scheduler_type = self.ui.comboBox_schedulerType.currentText()
        scheduler_type = utf8(scheduler_type).lower()
        if scheduler_type == 'local basic (SWF)':
            options['scheduler_type'] = 'local_basic'
        elif scheduler_type != 'drmaa':
            options['scheduler_type'] = scheduler_type

        if check_if_somawfdb_on_server(ResName, strLogin, strAdd, userpw=strPW, sshport=intPort):
            reply = QtGui.QMessageBox.question(self, 'Message',
                                               "Soma-workflow is running on your server. Are you sure to remove it and install it ?",
                                               QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)

            if reply == QtGui.QMessageBox.No:
                return

        try:
            InstallSomaWF2Server(
                strLogin, strAdd, ResName, userpw=strPW,
                install_swf_path_server=strInstallPath, sshport=intPort,
                config_options=options)
        except Exception as e:
            import traceback
            tb = traceback.format_tb(sys.exc_info()[2])
            tb = '\n'.join(tb)
            QtGui.QMessageBox.critical(
                self, "Oops...", "Oops...Failed to install soma-workflow.\n"
                "%s:\n%s\n\n"
                "traceback:\n%s" % (str(type(e)), str(e), tb))
            self.is_install = False
        else:
            self.is_install = True
            QtGui.QMessageBox.information(
                self, "Information", "Succeed to install soma-workflow on %s" % (strInstallPath))
            self.close()

    def SetupServerNoInstallation(self):

        from soma_workflow.setup_client2server import SetupSomaWF2Server

        strLogin = self.ui.lineEdit_login.text()
        strLogin = utf8(strLogin)

        strPort = self.ui.lineEdit_Port.text()
        strPort = utf8(strPort)
        intPort = int(strPort)

        strAdd = self.ui.lineEdit_cluster_add.text()
        strAdd = utf8(strAdd)

        ResName = self.ui.lineEdit_ResName.text()
        ResName = utf8(ResName)

        strPW = self.ui.lineEdit_PW.text()
        strPW = utf8(strPW)

        strPWRSA = self.ui.lineEdit_RSAKeyPW.text()
        strPWRSA = utf8(strPWRSA)

        strInstallPath = self.ui.lineEdit_InstallPath.text()
        strInstallPath = utf8(strInstallPath)

        try:
            SetupSomaWF2Server(
                strLogin, strAdd, ResName, userpw=strPW, sshport=intPort)
        except Exception as e:
            QtGui.QMessageBox.critical(self, "Oops...", "Oops...%s" % (e))
            self.is_install = False
        else:
            self.is_install = True
            QtGui.QMessageBox.information(
                self, "Information", "Succeed to connect soma-workflow")
            self.close()


class ServerManagementDialog(QtGui.QDialog):

    login_list = ""
    resource_list = []
    login_list = {}
    config_file_path = None
    add_widget = None

    def __init__(self,    parent=None):

        super(ServerManagementDialog, self).__init__(parent=parent)

        self.ui = Ui_ServerManagement()
        self.ui.setupUi(self)
        self.ui.btn_add_server.clicked.connect(self.add_server)
        self.ui.btn_remove_server.clicked.connect(self.remove_server)
        self.ui.btn_rm_serveronclient.clicked.connect(
            self.remove_server_on_client)

        self.ui.combo_resources.currentIndexChanged.connect(self.update_login)

        self.UpdateInterface()

    @QtCore.Slot()
    def UpdateInterface(self):

        try:
            self.config_file_path = configuration.Configuration.search_config_path(
            )
            self.resource_list = configuration.Configuration.get_configured_resources(
                self.config_file_path)
            self.login_list = configuration.Configuration.get_logins(
                self.config_file_path)
        except ConfigurationError as e:
            QtGui.QMessageBox.critical(
                self, "Configuration problem", "%s" % (e))
            self.close()

        self.ui.combo_resources.clear()
        self.ui.combo_resources.addItems(self.resource_list)
        self.ui.combo_resources.setEnabled(True)

        self.update_login()

    @QtCore.Slot()
    def update_login(self):
        resource_id = six.text_type(
            self.ui.combo_resources.currentText()).encode('utf-8')
        if resource_id == '' or resource_id == None:
            return

        login = None
        if resource_id in self.login_list:
            login = self.login_list[resource_id]

        if login != None:
            self.ui.lineEdit_login.setText(login)
        else:
            self.ui.lineEdit_login.clear()

        self.ui.combo_queues.clear()
        self.ui.lineEdit_InstallPath.clear()

        config = None

        if self.config_file_path != None:
            config = configuration.Configuration.load_from_file(
                resource_id, self.config_file_path)

        self.ui.lineEdit_cluster_add.clear()

        if config != None:
            queues = sorted(config.get_queues())
            cluster_address = config.get_cluster_address()
            installpath = config.get_res_install_path()

            if queues != None:
                self.ui.combo_queues.addItems(queues)
            if cluster_address != None:
                self.ui.lineEdit_cluster_add.setText(cluster_address)
            if installpath != None:
                self.ui.lineEdit_InstallPath.setText(installpath)

    @QtCore.Slot()
    def add_server(self):
        self.add_widget = NewServerDialog(self)
        self.add_widget.exec()
        self.UpdateInterface()

    @QtCore.Slot()
    def remove_server(self):
        from soma_workflow.setup_client2server import RemoveSomaWF2Server

        reply = QtGui.QMessageBox.question(self, 'Message',
                                           "Are you sure to remove soma-workflow on your server ? "
                                           "(Please make sure you are **NOT** connecting to the server that is going to be removed)",
                                           QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)

        if reply == QtGui.QMessageBox.No:
            return

        resource_id = six.text_type(
            self.ui.combo_resources.currentText()).encode('utf-8')

        if self.config_file_path != None:
            config = configuration.Configuration.load_from_file(
                resource_id, self.config_file_path)
            sshport = config.get_ssh_port()
            installpath = config.get_res_install_path()
            login = self.login_list[resource_id]
            cluster_add = config.get_cluster_address()

            getPWDlg = RequirePWDialog(self)
            RemoveSomaWF2Server(
                installpath, resource_id, login, cluster_add, getPWDlg.strPW, int(sshport))

        self.UpdateInterface()

        QtGui.QMessageBox.information(
            self, "Information", "Finish to remove soma-workflow on the cluster.")

    @QtCore.Slot()
    def remove_server_on_client(self):
        from soma_workflow.setup_client2server import RemoveResNameOnConfigureFile

        resource_id = six.text_type(
            self.ui.combo_resources.currentText()).encode('utf-8')
        if resource_id != None:
            RemoveResNameOnConfigureFile(resource_id)

        self.UpdateInterface()


class ConnectionDialog(QtGui.QDialog):

    def __init__(self,
                 login_list,
                 resource_list,
                 resource_id=None,
                 editable_resource=True,
                 parent=None):

        super(ConnectionDialog, self).__init__(parent=parent)

        self.ui = Ui_ConnectionDlg()
        self.ui.setupUi(self)

        self.login_list = login_list

        self.ui.combo_resources.addItems(resource_list)
        self.ui.combo_resources.setEnabled(editable_resource)
        if resource_id != None:
            index = resource_list.index(resource_id)
            self.ui.combo_resources.setCurrentIndex(index)

        self.ui.combo_resources.currentIndexChanged.connect(self.update_login)
        self.update_login()
        self.ui.kill_button.clicked.connect(self.kill_servers)

    @QtCore.Slot()
    def update_login(self):
        resource_id = six.text_type(
            self.ui.combo_resources.currentText())
        resource_id = six.ensure_str(resource_id, 'utf-8')
        login = self.login_list[resource_id]
        if login != None:
            self.ui.lineEdit_login.setText(login)
        else:
            self.ui.lineEdit_login.clear()

    @QtCore.Slot()
    def kill_servers(self):
        resource_id = six.text_type(self.ui.combo_resources.currentText())
        erase_db = self.ui.erase_db_checkbox.isChecked()
        login = six.text_type(self.ui.lineEdit_login.text())
        passwd = six.text_type(self.ui.lineEdit_password.text())
        #rsa_passwd = unicode(self.ui.lineEdit_rsa_password.text())
        print('kill_servers', resource_id, erase_db)
        connection.RemoteConnection.kill_remote_servers(
            resource_id, login=login, passwd=passwd, clear_db=erase_db)


class SomaWorkflowWidget(QtGui.QWidget):

    ui = None

    model = None

    resource_list = None

    config_file_path = None

    login_list = None

    update_workflow_list_from_model = None

    workflow_info_widget = None

    closing = QtCore.Signal()

    def __init__(self,
                 model,
                 user=None,
                 auto_connect=False,
                 computing_resource=None,
                 parent=None,
                 flags=0,
                 config_file=None,
                 db_file=None,
                 interactive=False,
                 isolated_light_mode=None):

        super(SomaWorkflowWidget, self).__init__(parent)

        self.ui = Ui_ResourceWfSelect()
        self.ui.setupUi(self)

        self.model = model

        self.update_workflow_list_from_model = False

        self.workflow_info_widget = WorkflowInfoWidget(self.model, parent=self)
        self.ui.wf_info_layout.addWidget(self.workflow_info_widget)

        self.model.current_connection_changed.connect(
            self.currentConnectionChanged)
        self.model.current_workflow_changed.connect(
            self.current_workflow_changed)
        self.model.connection_closed_error[()].connect(
            self.reconnectAfterConnectionClosed)
        self.model.connection_closed_error[str].connect(
            self.reconnectAfterConnectionClosed)
        self.model.global_workflow_state_changed.connect(
            self.update_workflow_status_icons)
        self.config_file_path = config_file

        self.UpdateLocalparameters()

        self.ui.combo_resources.addItems(self.resource_list)

        self.workflow_info_widget.hide()

        self.ui.toolButton_button_delete_wf.setDefaultAction(
            self.ui.action_delete_workflow)
        self.ui.toolButton_delete_all.setDefaultAction(
            self.ui.action_delete_all)

        self.ui.action_about.setIcon(
            QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/soma_workflow_icon.png")))
        self.ui.action_about.triggered.connect(self.display_about_dlg)

        self.ui.action_submit.triggered.connect(self.submit_workflow)
        self.ui.action_transfer_infiles.triggered.connect(
            self.transferInputFiles)
        self.ui.action_transfer_outfiles.triggered.connect(
            self.transferOutputFiles)
        self.ui.action_open_wf.triggered.connect(self.openWorkflow)
        self.ui.action_create_wf_ex.triggered.connect(
            self.createWorkflowExample)
        self.ui.action_delete_workflow.triggered.connect(self.delete_workflow)
        self.ui.action_delete_all.triggered.connect(self.delete_all_workflows)
        self.ui.action_change_expiration_date.triggered.connect(
            self.change_expiration_date)
        self.ui.action_save.triggered.connect(self.saveWorkflow)
        self.ui.action_restart.triggered.connect(self.restart_workflow)
        self.ui.action_stop_wf.triggered.connect(self.stop_workflow)
        self.ui.actionServer_Management.triggered.connect(
            self.openServerManagement)
        # disable server management until it is fixed
        #self.ui.actionServer_Management.setEnabled(False)
        #self.ui.actionServer_Management.setVisible(False)

        self.ui.list_widget_submitted_wfs.itemSelectionChanged.connect(
            self.workflowSelectionChanged)
        self.ui.combo_resources.currentIndexChanged.connect(
            self.resourceSelectionChanged)

        self.ui.wf_list_refresh_button.clicked.connect(
            self.refreshWorkflowList)

        self.connection_dlg = ConnectionDialog(self.login_list,
                                               self.resource_list,
                                               parent=self)
        self.connection_dlg.accepted.connect(self.firstConnection)
        self.connection_dlg.rejected.connect(self.close)

        self.db_file = db_file
        self.isolated_light_mode = isolated_light_mode

        # First connection:
        # Try to connect directly:
        if computing_resource is None and self.config_file_path is not None:
            computing_resource \
                = configuration.Configuration.get_local_resource_id(
                    config=None, config_file_path=self.config_file_path)
        if interactive:
            if computing_resource is not None:
                print('interactive, resource:', computing_resource)
                self.connection_dlg.ui.combo_resources.setCurrentText(
                    computing_resource)
            if user is not None:
                self.connection_dlg.ui.lineEdit_login.setText(user)
            self.connection_dlg.show()
        else:
            if computing_resource:
                print('connect to computing resource:', computing_resource)
                self.connect_to_controller(computing_resource, user)
            else:
                if auto_connect and user is not None \
                        and len(self.resource_list) > 0:
                    print('connect to computing resource:',
                          self.resource_list[0])
                    self.connect_to_controller(self.resource_list[0], user)
                else:
                    print('connect to computing resource:',
                          socket.gethostname())
                    self.connect_to_controller(socket.gethostname())

        if self.model.current_resource_id != None:
            self.currentConnectionChanged()

    def UpdateLocalparameters(self):

        try:
            self.config_file_path \
                = configuration.Configuration.search_config_path()
            self.resource_list \
                = configuration.Configuration.get_configured_resources(
                    self.config_file_path)
            self.login_list = configuration.Configuration.get_logins(
                self.config_file_path)
        except ConfigurationError as e:
            QtGui.QMessageBox.critical(
                self, "Configuration problem", "%s" % (e))
            self.close()

    def closeEvent(self, event):
        self.closing.emit()

    def display_about_dlg(self):
        message_box = QtGui.QMessageBox(QtGui.QMessageBox.NoIcon,
                                        "About Soma-workflow",
                                        "\n\nVersion: %s \n\nDocumentation and examples: http://www.brainvisa.info/soma-workflow" % (
                                            version.shortVersion),
                                        parent=self)
        message_box.setIconPixmap(
            QtGui.QPixmap(os.path.join(os.path.dirname(__file__),
                                       "icon/logo.png")))

        message_box.exec()

    def connect_to_controller(self,
                              resource_id,
                              login=None,
                              password=None,
                              rsa_key_pass=None):

        if self.model.resource_pool.resource_exist(resource_id):
            self.model.set_current_connection(resource_id)
            return

        wf_ctrl = None
        QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        config = None
        local_resource_id = configuration.Configuration.get_local_resource_id(
            None, config_file_path=self.config_file_path)
        if self.config_file_path is not None \
                or (resource_id in ('localhost', socket.gethostname(),
                                    local_resource_id)
                    and self.db_file is not None):
            config = configuration.Configuration.load_from_file(
                resource_id=resource_id,
                config_file_path=self.config_file_path)
            if resource_id in ('localhost', socket.gethostname(),
                               local_resource_id) \
                    and self.db_file is not None:
                config._database_file = self.db_file
        try:
            wf_ctrl = Controller.get_connection(
                resource_id,
                login,
                password,
                rsa_key_pass,
                config=config,
                isolated_light_mode=self.isolated_light_mode)
            QtGui.QApplication.restoreOverrideCursor()
        except ConfigurationError as e:
            QtGui.QApplication.restoreOverrideCursor()
            QtGui.QMessageBox.critical(
                self, "Configuration problem", "%s" % (e))
            self.connection_dlg.ui.lineEdit_password.clear()
            self.connection_dlg.show()
        except Exception as e:
            QtGui.QApplication.restoreOverrideCursor()
            msg = str(e) + '\n**More details:**\n' + traceback.format_exc()
            detailed_critical_message_box(msg=msg,
                                          title="Connection failed",
                                          parent=self)
            self.connection_dlg.ui.lineEdit_password.clear()
            self.connection_dlg.show()
        else:
            self.model.add_connection(resource_id, wf_ctrl)
            self.connection_dlg.hide()

        # pass

    @QtCore.Slot()
    def firstConnection(self):
        resource_id = six.text_type(
            self.connection_dlg.ui.combo_resources.currentText())
        if self.connection_dlg.ui.lineEdit_login.text():
            login = six.text_type(
                self.connection_dlg.ui.lineEdit_login.text()).encode('utf-8')
        else:
            login = None
        if self.connection_dlg.ui.lineEdit_password.text():
            password = six.text_type(
                self.connection_dlg.ui.lineEdit_password.text()).encode(
                    'utf-8')
        else:
            password = None
        if self.connection_dlg.ui.lineEdit_rsa_password.text():
            rsa_key_pass = six.text_type(
                self.connection_dlg.ui.lineEdit_rsa_password.text()).encode(
                    'utf-8')
        else:
            rsa_key_pass = None

        self.connect_to_controller(resource_id, login, password, rsa_key_pass)
        # pass

    @QtCore.Slot()
    def openServerManagement(self):
        self.server_widget = ServerManagementDialog(self)
        self.server_widget.exec()
        # pass

    @QtCore.Slot()
    def openWorkflow(self):
        file_path = QtGui.QFileDialog.getOpenFileName(
            self, "Open a workflow", "", "", "", filedialog_options)
        file_path = file_path[0]
        if file_path:
            try:
                workflow = Controller.unserialize_workflow(file_path)
            except SerializationError as e:
                QtGui.QMessageBox.warning(
                    self, "Error opening the workflow", "%s" % (e))
            else:
                self.model.add_workflow(NOT_SUBMITTED_WF_ID,
                                        datetime.now() + timedelta(days=5),
                                        workflow.name,
                                        constants.WORKFLOW_NOT_STARTED,
                                        workflow)
                self.updateWorkflowList()

    @QtCore.Slot()
    def saveWorkflow(self):
        file_path = QtGui.QFileDialog.getSaveFileName(
            self, "Save the current workflow", "", "", "",
            filedialog_options)
        file_path = file_path[0]

        if file_path:
            try:
                Controller.serialize_workflow(
                    file_path, self.model.current_workflow().server_workflow)
            except SerializationError as e:
                QtGui.QMessageBox.warning(
                    self, "Error", "%s: %s" % (type(e), e))

    @QtCore.Slot()
    def createWorkflowExample(self):
        workflowExample_dlg = QtGui.QDialog(self)
        ui = Ui_WorkflowExampleDlg()
        ui.setupUi(workflowExample_dlg)
        ui.comboBox_example_type.addItems(
            WorkflowExamples.get_workflow_example_list())
        if workflowExample_dlg.exec() == QtGui.QDialog.Accepted:
            with_file_transfer = ui.checkBox_file_transfers.checkState(
            ) == QtCore.Qt.Checked
            with_shared_resource_path = ui.checkBox_shared_resource_path.checkState(
            ) == QtCore.Qt.Checked
            example_type = ui.comboBox_example_type.currentIndex()
            file_path = QtGui.QFileDialog.getSaveFileName(self,
                                                          "Create a workflow example")
            file_path = QtGui.QFileDialog.getSaveFileName(
                self, "Create a workflow example", "", "", "",
                filedialog_options)
            if file_path:
                try:
                    if with_file_transfer and not with_shared_resource_path:
                        wf_examples = WorkflowExamplesTransfer()
                    elif with_file_transfer and with_shared_resource_path:
                        wf_examples = WorkflowExamplesSharedTransfer()
                    elif not with_file_transfer and with_shared_resource_path:
                        wf_examples = WorkflowExamplesShared()
                    else:
                        wf_examples = WorkflowExamplesLocal()
                except ConfigurationError as e:
                    QtGui.QMessageBox.warning(self, "Error", "%s" % (e))
                else:
                    workflow = wf_examples.get_workflow_example(example_type)
                    try:
                        Controller.serialize_workflow(file_path, workflow)
                    except SerializationError as e:
                        QtGui.QMessageBox.warning(self, "Error", "%s" % (e))

    @QtCore.Slot()
    def submit_workflow(self,
                        date=None,
                        name=None,
                        queue=None):

        assert(self.model.current_workflow())

        # date is the only mandatory argument
        # if date is None the submission dialog has to be open.
        if date == None:
            submission_dlg = QtGui.QDialog(self)
            ui = Ui_SubmissionDlg()
            ui.setupUi(submission_dlg)
            ui.resource_label.setText(self.model.current_resource_id)

            if self.model.current_workflow().name == None:
                ui.lineedit_wf_name.setText("")
            else:
                ui.lineedit_wf_name.setText(
                    self.model.current_workflow().server_workflow.name)

            ui.dateTimeEdit_expiration.setDateTime(
                datetime.now() + timedelta(days=5))

            queues = ["default"]
            queues.extend(sorted([q for q in Controller.get_queues(
                self.model.current_connection) if q not in (None, 'default')]))
            ui.combo_queue.addItems(queues)

            if submission_dlg.exec() != QtGui.QDialog.Accepted:
                return (None, None)

            if ui.lineedit_wf_name.text():
                name = utf8(ui.lineedit_wf_name.text())
            else:
                name = None
            qtdt = ui.dateTimeEdit_expiration.dateTime()
            date = datetime(
                qtdt.date().year(), qtdt.date().month(), qtdt.date().day(),
                qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
            queue = utf8(ui.combo_queue.currentText())
            if queue == "default":
                queue = None

        while True:
            try:
                workflow = Controller.submit_workflow(
                # TEST
                # workflow_id = Controller.submit_workflow(
                    self.model.current_workflow(
                    ).server_workflow,
                    date,
                    name,
                    queue,
                    self.model.current_connection)
            except WorkflowError as e:
                QtGui.QMessageBox.warning(self,
                                          "Workflow submission error",
                                          "%s" % (e))
                return (None, None)
            except JobError as e:
                QtGui.QMessageBox.warning(self,
                                          "Workflow submission error",
                                          "%s" % (e))
                return (None, None)
            except ConnectionClosedError as e:
                if not self.reconnectAfterConnectionClosed():
                    return (None, None)
            else:
                break
        # TEST
        # self.model.add_workflow(workflow_id,
        #                        date,
        #                        name,
        #                        constants.WORKFLOW_NOT_STARTED,
        #                        self.model.current_workflow().server_workflow)
        self.model.add_workflow(workflow.wf_id,
                                date,
                                workflow.name,
                                constants.WORKFLOW_NOT_STARTED,
                                workflow)
        self.updateWorkflowList()
        # TEST
        # return (workflow_id, self.model.current_resource_id)
        return (workflow.wf_id, self.model.current_resource_id)

    @QtCore.Slot()
    def restart_workflow(self):

        queue = None
        date = None
        if Controller.get_queues(self.model.current_connection):
            submission_dlg = QtGui.QDialog(self)
            ui = Ui_SubmissionDlg()
            ui.setupUi(submission_dlg)
            ui.resource_label.setText(self.model.current_resource_id)
            if self.model.workflow_name:
                ui.lineedit_wf_name.setText(self.model.workflow_name)
            else:
                ui.lineedit_wf_name.setText(repr(self.model.current_wf_id))
            ui.lineedit_wf_name.setEnabled(False)
            ui.dateTimeEdit_expiration.setDateTime(
                datetime.now() + timedelta(days=5))
            submission_dlg.setWindowTitle("Restart")
            queues = ["default"]
            queues.extend(sorted([q for q in Controller.get_queues(
                self.model.current_connection) if q not in (None, 'default')]))
            ui.combo_queue.addItems(queues)
            previous_queue = self.model.current_workflow().queue
            if previous_queue == None:
                previous_queue = "default"
            index = queues.index(previous_queue)
            ui.combo_queue.setCurrentIndex(index)

            if submission_dlg.exec() != QtGui.QDialog.Accepted:
                return
            queue = utf8(ui.combo_queue.currentText())
            if queue == "default":
                queue = None

            qtdt = ui.dateTimeEdit_expiration.dateTime()
            date = datetime(
                qtdt.date().year(), qtdt.date().month(), qtdt.date().day(),
                qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())

        done = False
        try:
            done = Controller.restart_workflow(self.model.current_wf_id,
                                               queue,
                                               self.model.current_connection)
            if done and date != None:
                Controller.change_workflow_expiration_date(
                    self.model.current_wf_id,
                    date,
                    self.model.current_connection)
        except ConnectionClosedError as e:
            pass
        except SystemExit as e:
            pass
        if not done:
            QtGui.QMessageBox.warning(self,
                                      "Restart workflow",
                                      "The workflow is already running.")
        else:
            self.model.restart_current_workflow()
            self.model.update()

    @QtCore.Slot()
    def transferInputFiles(self):
        def transfer(self):
            try:
                self.ui.action_transfer_infiles.setEnabled(False)
                Controller.transfer_input_files(
                    self.model.current_wf_id,
                    self.model.current_connection,
                    buffer_size=256 ** 2)
            except ConnectionClosedError as e:
                self.ui.action_transfer_infiles.setEnabled(True)
                pass
            except SystemExit as e:
                pass
            self.ui.action_transfer_infiles.setEnabled(True)
        thread = threading.Thread(name="TransferInputFiles",
                                  target=transfer,
                                  args=([self]))
        thread.daemon = True
        thread.start()

    @QtCore.Slot()
    def transferOutputFiles(self):
        def transfer(self):
            try:
                self.ui.action_transfer_outfiles.setEnabled(False)
                Controller.transfer_output_files(
                    self.model.current_wf_id,
                    self.model.current_connection,
                    buffer_size=256 ** 2)
            except ConnectionClosedError as e:
                self.ui.action_transfer_outfiles.setEnabled(True)
            except SystemExit as e:
                pass
            self.ui.action_transfer_outfiles.setEnabled(True)
        thread = threading.Thread(name="TransferOuputFiles",
                                  target=transfer,
                                  args=([self]))
        thread.daemon = True
        thread.start()

    @QtCore.Slot()
    def workflowSelectionChanged(self):
        selected_items = self.ui.list_widget_submitted_wfs.selectedItems()
        if not selected_items:
            return
        QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        try:
            if use_qvariant:
                wf_id = selected_items[0].data(QtCore.Qt.UserRole).toInt()[0]
            else:
                wf_id = int(selected_items[0].data(QtCore.Qt.UserRole))
            if wf_id != NOT_SUBMITTED_WF_ID:
                if self.model.is_loaded_workflow(wf_id):
                    self.model.set_current_workflow(wf_id)
                else:
                    workflow_info_dict = self.model.current_connection.workflows(
                        [wf_id])
                    if len(workflow_info_dict) > 0:
                        # The workflow exist
                        workflow_status = self.model.current_connection.workflow_status(
                            wf_id)
                        workflow_info = workflow_info_dict[wf_id]
                        self.model.add_workflow(wf_id,
                                                workflow_exp_date=workflow_info[
                                                    1],
                                                workflow_name=workflow_info[0],
                                                workflow_status=workflow_status)
                    else:
                        self.model.clear_current_workflow()
                        self.updateWorkflowList()
            else:
                self.model.clear_current_workflow()
            QtGui.QApplication.restoreOverrideCursor()
        except ConnectionClosedError as e:
            QtGui.QApplication.restoreOverrideCursor()
            self.reconnectAfterConnectionClosed()
        except Exception as e:
            QtGui.QApplication.restoreOverrideCursor()
            raise e

    @QtCore.Slot(int)
    def resourceSelectionChanged(self, index):
        if index < 0 or index >= self.ui.combo_resources.count():
            index = self.ui.combo_resources.findText(
                self.model.current_resource_id)
            self.ui.combo_resources.setCurrentIndex(index)
            return

        resource_id = utf8(six.text_type(
            self.ui.combo_resources.itemText(index)))
        if resource_id == " ":
            index = self.ui.combo_resources.findText(
                self.model.current_resource_id)
            self.ui.combo_resources.setCurrentIndex(index)
            return

        if self.model.resource_pool.resource_exist(resource_id):
            self.model.set_current_connection(resource_id)
            return
        else:
            (resource_id, new_connection) = self.createConnection(resource_id)
            if new_connection:
                self.model.add_connection(resource_id, new_connection)

    def createConnection(self,
                         resource_id=None,
                         editable_resource=True,
                         replace=False):
        '''
        returns a tuple (resource_id, connection)
        '''

        self.UpdateLocalparameters()

        connection_invalid = True
        try_again = True
        while connection_invalid or try_again:
            connection_dlg = ConnectionDialog(self.login_list,
                                              self.resource_list,
                                              resource_id,
                                              editable_resource)
            connection_dlg.setModal(True)
            if connection_dlg.exec() != QtGui.QDialog.Accepted:
                try_again = False
                index = self.ui.combo_resources.findText(
                    self.model.current_resource_id)
                self.ui.combo_resources.setCurrentIndex(index)
                break
            index = connection_dlg.ui.combo_resources.currentIndex()
            resource_id = self.resource_list[index]
            if connection_dlg.ui.lineEdit_login.text():
                login = six.text_type(
                    connection_dlg.ui.lineEdit_login.text()).encode('utf-8')
            else:
                login = None
            if connection_dlg.ui.lineEdit_password.text():
                password = six.text_type(
                    connection_dlg.ui.lineEdit_password.text()).encode('utf-8')
            else:
                password = None
            if connection_dlg.ui.lineEdit_rsa_password.text():
                rsa_key_pass = six.text_type(
                    connection_dlg.ui.lineEdit_rsa_password.text()).encode('utf-8')
            else:
                rsa_key_pass = None

            if not replace and resource_id in self.model.resource_pool.resource_ids():
                QtGui.QMessageBox.information(
                    self, "Connection already exists", "The connection to the resource %s already exists." % (resource_id))
                return (resource_id, None)

            QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
            try:
                wf_ctrl = Controller.get_connection(
                    resource_id,
                    login,
                    password,
                    rsa_key_pass,
                    isolated_light_mode=self.isolated_light_mode)
                QtGui.QApplication.restoreOverrideCursor()
            except ConfigurationError as e:
                QtGui.QApplication.restoreOverrideCursor()
                QtGui.QMessageBox.information(
                    self, "Configuration error", "%s" % (e))
                return (resource_id, None)
            except Exception as e:
                QtGui.QApplication.restoreOverrideCursor()
                msg = str(e) + '\n**More details:**\n' + traceback.format_exc()
                detailed_critical_message_box(msg=msg,
                                              title="Connection failed",
                                              parent=self)
            else:
                return (resource_id, wf_ctrl)
        return (resource_id, None)

    @QtCore.Slot()
    def stop_workflow(self):
        assert(self.model.current_workflow() and
               self.model.current_wf_id != NOT_SUBMITTED_WF_ID)

        if self.model.current_workflow().name:
            name = self.model.current_workflow().name
        else:
            name = repr(self.model.current_wf_id)

        if self.model.workflow_status != constants.WARNING:
            answer = QtGui.QMessageBox.question(
                self, "confirmation", "The running jobs will be killed and the jobs in the queue will be removed. \nDo you want to stop the workflow " + name + " anyway?", QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
            if answer != QtGui.QMessageBox.Yes:
                return
        stopped_properly = False
        while True:
            try:
                stopped_properly = Controller.stop_workflow(
                    self.model.current_wf_id,
                    self.model.current_connection)
            except ConnectionClosedError as e:
                if not self.reconnectAfterConnectionClosed():
                    return
            else:
                if not stopped_properly and \
                   self.model.current_connection.config.get_mode() != configuration.LIGHT_MODE:
                    QtGui.QMessageBox.warning(self,
                                              "Stop workflow",
                                              "The workflow was stopped. \n However, some jobs "
                                              "may still be active and burden the computing "
                                              "resource. \n In case of long jobs, please "
                                              "inspect the active jobs (running or in the "
                                              "queue) using the DRMS interface.")
                break

        self.model.update()

    @QtCore.Slot()
    def delete_all_workflows(self):
        workflows = self.model.workflows(self.model.current_resource_id)
        if not workflows:
            return
        workflow_names = []
        for wf_id, (wf_name, exp_date) in six.iteritems(workflows):
            if wf_name:
                workflow_names.append(wf_name)
            else:
                workflow_names.append(str(wf_id))

        separator = ", "
        names = separator.join(workflow_names)
        answer = QtGui.QMessageBox.question(self,
                                            "confirmation",
                                            "Do you want to delete the "
                                            "workflows: \n" + names + "?",
                                            QtGui.QMessageBox.Yes,
                                            QtGui.QMessageBox.No)
        if answer != QtGui.QMessageBox.Yes:
            return
        force = self.ui.check_box_force_delete.isChecked()
        while True:
            try:
                deleled_properly = Controller.delete_all_workflows(
                    force, self.model.current_connection)
            except ConnectionClosedError as e:
                if not self.reconnectAfterConnectionClosed():
                    return
            else:
                break

        if force:
            self.model.delete_workflow()
            if not deleled_properly and \
                  self.model.current_connection.config.get_mode() \
                      != configuration.LIGHT_MODE:
                QtGui.QMessageBox.warning(
                    self,
                    "Delete workflow",
                    "The workflow were deleted. \n However, some jobs "
                    "may still be active and burden the computing "
                    "resource. \n In case of long jobs, please "
                    "inspect the active jobs (running or in the "
                    "queue) using the DRMS interface.")
        self.refreshWorkflowList()

    @QtCore.Slot()
    def delete_workflow(self):
        assert(self.model.current_workflow() and
               self.model.current_wf_id != NOT_SUBMITTED_WF_ID)

        if self.model.current_workflow().name:
            name = self.model.current_workflow().name
        else:
            name = repr(self.model.current_wf_id)

        answer = QtGui.QMessageBox.question(
            self, "confirmation", "Do you want to delete the workflow " + name + "?", QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
        if answer != QtGui.QMessageBox.Yes:
            return
        force = self.ui.check_box_force_delete.isChecked()
        while True:
            try:
                deleled_properly = Controller.delete_workflow(
                    self.model.current_wf_id,
                    force,
                    self.model.current_connection)
            except ConnectionClosedError as e:
                if not self.reconnectAfterConnectionClosed():
                    return
            else:
                break

        if force:
            self.model.delete_workflow()
            self.updateWorkflowList()
            if not deleled_properly and \
                  self.model.current_connection.config.get_mode() \
                      != configuration.LIGHT_MODE:
                QtGui.QMessageBox.warning(
                    self,
                    "Delete workflow",
                    "The workflow was deleted. \n However, some jobs "
                    "may still be active and burden the computing "
                    "resource. \n In case of long jobs, please "
                    "inspect the active jobs (running or in the "
                    "queue) using the DRMS interface.")

    @QtCore.Slot()
    def change_expiration_date(self):
        dlg = QtGui.QDialog(self)
        ui = Ui_SubmissionDlg()
        ui.setupUi(dlg)
        ui.resource_label.setText(self.model.current_resource_id)
        if self.model.workflow_name:
            ui.lineedit_wf_name.setText(self.model.workflow_name)
        else:
            ui.lineedit_wf_name.setText(repr(self.model.current_wf_id))
        ui.lineedit_wf_name.setReadOnly(True)
        ui.lineedit_wf_name.setFrame(False)
        ui.dateTimeEdit_expiration.setDateTime(self.model.workflow_exp_date)
        dlg.setWindowTitle("Change expiration date")
        queue = self.model.current_workflow().queue
        if queue == None:
            queue = "default queue"
        ui.combo_queue.addItem(queue)
        ui.combo_queue.setEnabled(False)

        if dlg.exec() != QtGui.QDialog.Accepted:
            return

        qtdt = ui.dateTimeEdit_expiration.dateTime()
        date = datetime(
            qtdt.date().year(), qtdt.date().month(), qtdt.date().day(),
            qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())

        while True:
            try:
                change_occured = Controller.change_workflow_expiration_date(
                    self.model.current_wf_id,
                    date,
                    self.model.current_connection)
            except ConnectionClosedError as e:
                if not self.reconnectAfterConnectionClosed():
                    return
            else:
                break
        if not change_occured:
            QtGui.QMessageBox.information(
                self, "information",
                "The workflow expiration date was not changed.")
        else:
            self.model.change_expiration_date(date)
            self.workflow_info_widget.current_workflow_changed()

    @QtCore.Slot()
    def currentConnectionChanged(self):
        self.model.clear_current_workflow()
        self.updateWorkflowList()
        if self.model.current_resource_id is not None:
            index = self.ui.combo_resources.findText(
                self.model.current_resource_id)
            self.ui.combo_resources.setCurrentIndex(index)

    @QtCore.Slot()
    def current_workflow_changed(self):
        if not self.model.current_connection:
            return
        scheduler_type = self.model.current_connection.engine_config_proxy.get_scheduler_type(
        )
        control_authorized = (scheduler_type != configuration.MPI_SCHEDULER)

        if self.model.current_wf_id == None:
            # No workflow

            self.ui.action_submit.setEnabled(False)
            self.ui.action_change_expiration_date.setEnabled(False)
            self.ui.action_stop_wf.setEnabled(False)
            self.ui.action_restart.setEnabled(False)
            self.ui.action_delete_workflow.setEnabled(False)
            self.ui.action_transfer_infiles.setEnabled(False)
            self.ui.action_transfer_outfiles.setEnabled(False)
            self.ui.action_save.setEnabled(False)

            self.workflow_info_widget.hide()

            self.ui.list_widget_submitted_wfs.clearSelection()

        else:

            if self.model.current_wf_id == NOT_SUBMITTED_WF_ID:
                # Workflow not submitted
                self.ui.action_submit.setEnabled(control_authorized)
                self.ui.action_change_expiration_date.setEnabled(False)
                self.ui.action_delete_workflow.setEnabled(False)
                self.ui.action_stop_wf.setEnabled(False)
                self.ui.action_restart.setEnabled(False)
                self.ui.action_transfer_infiles.setEnabled(False)
                self.ui.action_transfer_outfiles.setEnabled(False)

                self.workflow_info_widget.show()

                self.ui.list_widget_submitted_wfs.clearSelection()

            else:
                # Submitted workflow

                self.ui.action_submit.setEnabled(False)
                self.ui.action_change_expiration_date.setEnabled(True)
                self.ui.action_delete_workflow.setEnabled(True)
                self.ui.action_stop_wf.setEnabled(control_authorized)
                self.ui.action_restart.setEnabled(control_authorized)
                self.ui.action_transfer_infiles.setEnabled(control_authorized)
                self.ui.action_transfer_outfiles.setEnabled(control_authorized)
                self.ui.action_save.setEnabled(True)

                self.workflow_info_widget.show()

                index = None
                for i in range(0, self.ui.list_widget_submitted_wfs.count()):
                    if use_qvariant:
                        wf_id = self.ui.list_widget_submitted_wfs.item(
                            i).data(QtCore.Qt.UserRole).toInt()[0]
                    else:
                        wf_id = int(
                            self.ui.list_widget_submitted_wfs.item(i).data(QtCore.Qt.UserRole))
                    if self.model.current_wf_id == wf_id:
                        self.ui.list_widget_submitted_wfs.setCurrentRow(i)
                        break

    @QtCore.Slot()
    def refreshWorkflowList(self):
        self.model.clear_current_workflow()
        self.updateWorkflowList(force_not_from_model=True)

    def workflow_filter(self, workflows):
        '''
        Reimplement this function to filter the displayed workflows.
        '''
        return workflows

    @QtCore.Slot()
    def update_workflow_status_icons(self):
        for i in range(0, self.ui.list_widget_submitted_wfs.count()):
            item = self.ui.list_widget_submitted_wfs.item(i)
            if use_qvariant:
                wf_id = item.data(QtCore.Qt.UserRole).toInt()[0]
            else:
                wf_id = int(item.data(QtCore.Qt.UserRole))
            status = self.model.get_workflow_status(
                self.model.current_resource_id, wf_id)
            icon_path = workflow_status_icon(status)
            if status != constants.WORKFLOW_DONE and icon_path != None:
                item.setIcon(QtGui.QIcon(icon_path))
            else:
                item.setIcon(QtGui.QIcon())

    def updateWorkflowList(self, force_not_from_model=False):
        if force_not_from_model or \
            not self.update_workflow_list_from_model or \
                len(self.model.list_workflow_names(
                        self.model.current_resource_id)) == 0:
            while True:
                try:
                    submitted_wf = Controller.get_submitted_workflows(
                        self.model.current_connection)
                except ConnectionClosedError as e:
                    if not self.reconnectAfterConnectionClosed():
                        return
                else:
                    break
        else:
            submitted_wf = self.model.workflows(self.model.current_resource_id)

        submitted_wf = self.workflow_filter(submitted_wf)
        self.ui.list_widget_submitted_wfs.itemSelectionChanged.disconnect(
            self.workflowSelectionChanged)
        self.ui.list_widget_submitted_wfs.clear()
        wf_id_info = sorted(submitted_wf.items(), key=lambda elem: elem[1],
                            reverse=True)

        for wf_id, wf_info in wf_id_info:
            workflow_name, expiration_date = wf_info
            if not workflow_name:
                workflow_name = repr(wf_id)
            status = self.model.get_workflow_status(
                self.model.current_resource_id, wf_id)
            icon_path = workflow_status_icon(status)
            if status != constants.WORKFLOW_DONE and icon_path != None:
                item = QtGui.QListWidgetItem(QtGui.QIcon(icon_path),
                                             workflow_name,
                                             self.ui.list_widget_submitted_wfs)
            else:
                item = QtGui.QListWidgetItem(
                    workflow_name, self.ui.list_widget_submitted_wfs)
            item.setData(QtCore.Qt.UserRole, wf_id)
            self.ui.list_widget_submitted_wfs.addItem(item)
        self.ui.list_widget_submitted_wfs.itemSelectionChanged.connect(
            self.workflowSelectionChanged)

    #@QtCore.Slot(QtCore.QString)
    def reconnectAfterConnectionClosed(self, resource_id=None):
        if resource_id == None:
            resource_id = self.model.current_resource_id
        else:
            resource_id = six.ensure_str(resource_id)
        answer = QtGui.QMessageBox.question(None,
                                            "Connection closed",
                                            "The connection to  " + resource_id +
                                            " closed.\n  Do you want to try a reconnection?",
                                            QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
        if answer == QtGui.QMessageBox.Yes:
            (new_resource_id, new_connection) = self.createConnection(
                resource_id, editable_resource=False, replace=True)
            if new_connection:
                self.model.reinit_connection(new_resource_id, new_connection)
                return True

        self.model.delete_connection(resource_id)
        return False


class MainWindow(QtGui.QMainWindow):

    ui = None

    model = None

    sw_widget = None

    server_widget = None

    def __init__(self,
                 model,
                 user=None,
                 auto_connect=False,
                 computing_resource=None,
                 parent=None,
                 flags=0,
                 config_file=None,
                 db_file=None,
                 interactive=False,
                 isolated_light_mode=None):
        '''
        Parameters
        ----------
        model: ApplicationModel
        '''

        super(MainWindow, self).__init__(parent)

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.setWindowIcon(QtGui.QIcon(os.path.join(os.path.dirname(__file__),
                                                    "icon/soma_workflow_icon.png")))

        self.setCorner(
            QtCore.Qt.BottomLeftCorner, QtCore.Qt.LeftDockWidgetArea)
        self.setCorner(
            QtCore.Qt.BottomRightCorner, QtCore.Qt.RightDockWidgetArea)

        self.tabifyDockWidget(self.ui.dock_graph, self.ui.dock_plot)

        self.setWindowTitle("soma-workflow")

        self.model = model

        self.model.current_connection_changed.connect(
            self.currentConnectionChanged)

        self.sw_widget = SomaWorkflowWidget(self.model,
                                            user,
                                            auto_connect,
                                            computing_resource,
                                            self,
                                            flags,
                                            config_file=config_file,
                                            db_file=db_file,
                                            interactive=interactive,
                                            isolated_light_mode
                                                =isolated_light_mode)

        if True:
            self.mini_widget = SomaWorkflowMiniWidget(self.model,
                                                      self.sw_widget,
                                                      self)
            self.mini_widget.layout().setContentsMargins(0, 0, 0, 0)
            self.sw_widget.ui.combo_resources.hide()
            self.sw_widget.ui.resource_selection_frame.layout().addWidget(
                self.mini_widget)

        resourcesWfLayout = QtGui.QVBoxLayout()
        resourcesWfLayout.setContentsMargins(2, 2, 2, 2)
        resourcesWfLayout.addWidget(self.sw_widget)
        self.ui.dockWidgetContents_resource_wf.setLayout(resourcesWfLayout)

        self.sw_widget.closing.connect(self.close)

        self.treeWidget = WorkflowTree(self.model, parent=self)
        treeWidgetLayout = QtGui.QVBoxLayout()
        treeWidgetLayout.setContentsMargins(2, 2, 2, 2)
        treeWidgetLayout.addWidget(self.treeWidget)
        self.ui.centralwidget.setLayout(treeWidgetLayout)

        self.itemInfoWidget = WorkflowElementInfo(
            model=self.model, proxy_model=self.treeWidget.proxy_model,
            parent=self)
        itemInfoLayout = QtGui.QVBoxLayout()
        itemInfoLayout.setContentsMargins(2, 2, 2, 2)
        itemInfoLayout.addWidget(self.itemInfoWidget)
        self.ui.dockWidgetContents_intemInfo.setLayout(itemInfoLayout)
        self.itemInfoWidget.job_selected.connect(self.treeWidget.select_job)

        self.treeWidget.selection_model_changed.connect(
            self.itemInfoWidget.setSelectionModel)

        self.itemInfoWidget.connection_closed_error.connect(
            self.sw_widget.reconnectAfterConnectionClosed)

        self.workflowInfoWidget = WorkflowGroupInfo(self.model, self)
        wfInfoLayout = QtGui.QVBoxLayout()
        wfInfoLayout.setContentsMargins(2, 2, 2, 2)
        wfInfoLayout.addWidget(self.workflowInfoWidget)
        self.ui.widget_wf_info.setLayout(wfInfoLayout)

        self.graphWidget = WorkflowGraphView(self.model, self)
        graphWidgetLayout = QtGui.QVBoxLayout()
        graphWidgetLayout.setContentsMargins(2,2,2,2)
        graphWidgetLayout.addWidget(self.graphWidget)
        self.ui.dockWidgetContents_graph.setLayout(graphWidgetLayout)

        # no graph for now
        #self.ui.dock_graph.hide()
        #self.ui.dock_graph.toggleViewAction().setVisible(False)
        self.ui.dock_graph.visibilityChanged.connect(
            self.graphWidget.graph_visibility_changed)

        self.workflowPlotWidget = WorkflowPlot(self.model, parent=self)
        plotLayout = QtGui.QVBoxLayout()
        plotLayout.setContentsMargins(2, 2, 2, 2)
        plotLayout.addWidget(self.workflowPlotWidget)
        self.ui.dockWidgetContents_plot.setLayout(plotLayout)
        self.workflowPlotWidget.job_selected.connect(
            self.treeWidget.select_job)

        if not MATPLOTLIB:
            self.ui.dock_plot.hide()
            self.ui.dock_plot.toggleViewAction().setVisible(False)

        self.ui.menu_file.addAction(self.sw_widget.ui.action_open_wf)
        self.ui.menu_file.addAction(self.sw_widget.ui.action_save)
        self.ui.menu_file.addSeparator()
        self.ui.menu_file.addAction(self.sw_widget.ui.action_create_wf_ex)
        self.ui.menu_file.addAction(self.sw_widget.ui.action_create_wf_ex)
        self.ui.menu_file.addSeparator()
        if QtCore.QT_VERSION >= 0x060000:
            self.ui.menu_file.addAction(
                'Quit',
                QtGui.QKeySequence(QtCore.Qt.Key_Q
                                   | QtCore.Qt.ControlModifier),
                self.close)
        else:
            self.ui.menu_file.addAction(
                'Quit',
                self.close,
                QtGui.QKeySequence(QtCore.Qt.Key_Q
                                   | QtCore.Qt.ControlModifier))

        self.ui.menu_workflow.addAction(self.sw_widget.ui.action_submit)
        self.ui.menu_workflow.addAction(self.sw_widget.ui.action_stop_wf)
        self.ui.menu_workflow.addAction(self.sw_widget.ui.action_restart)
        self.ui.menu_workflow.addSeparator()
        self.ui.menu_workflow.addAction(
            self.sw_widget.ui.action_transfer_infiles)
        self.ui.menu_workflow.addAction(
            self.sw_widget.ui.action_transfer_outfiles)
        self.ui.menu_workflow.addSeparator()
        self.ui.menu_workflow.addAction(
            self.sw_widget.ui.action_delete_workflow)
        self.ui.menu_workflow.addAction(self.sw_widget.ui.action_delete_all)
        self.ui.menu_workflow.addAction(
            self.sw_widget.ui.action_change_expiration_date)

        self.ui.menu_view.addAction(self.ui.dock_plot.toggleViewAction())
        self.ui.menu_view.addAction(self.ui.dock_graph.toggleViewAction())

        self.ui.menu_help.addAction(self.sw_widget.ui.action_about)

        self.ui.tool_bar.addAction(self.sw_widget.ui.action_open_wf)
        self.ui.tool_bar.addSeparator()
        self.ui.tool_bar.addAction(self.sw_widget.ui.action_submit)
        self.ui.tool_bar.addAction(self.sw_widget.ui.action_stop_wf)
        self.ui.tool_bar.addAction(self.sw_widget.ui.action_restart)
        self.ui.tool_bar.addSeparator()
        self.ui.tool_bar.addAction(self.sw_widget.ui.action_transfer_infiles)
        self.ui.tool_bar.addAction(self.sw_widget.ui.action_transfer_outfiles)
        self.ui.tool_bar.addAction(self.sw_widget.ui.actionServer_Management)
        self.ui.tool_bar.addSeparator()

        # self.showMaximized()

    def canExit(self):
        for res_id in self.model.resource_pool.resource_ids():
            connection = self.model.resource_pool.connection(res_id)
            for workflow_id in self.model.workflows(res_id):
                try:
                    wf_elements_status = connection.workflow_elements_status(
                        workflow_id)
                except Exception:
                    continue # workflow deleted
                for transfer_info in wf_elements_status[1]:
                    status = transfer_info[1][0]
                    if status == constants.TRANSFERING_FROM_CR_TO_CLIENT or \
                       status == constants.TRANSFERING_FROM_CLIENT_TO_CR:
                        reply = QtGui.QMessageBox.question(
                            None,
                            'Warning!!',
                            "Files are transfering, "
                            "If you close it, the workflow will be broken."
                            "Do you want to close it?",
                            QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
                        if reply == QtGui.QMessageBox.Yes:
                            return True
                        else:
                            return False
        return True

    @QtCore.Slot()
    def currentConnectionChanged(self):
        current_resource_id = self.model.current_resource_id
        if current_resource_id is None:
            current_resource_id = '<none>'
        self.setWindowTitle(
            "soma-workflow - " + current_resource_id)

    @QtCore.Slot()
    def closeEvent(self, event):
                # do stuff
        if self.canExit():
            event.accept()  # let the window close
            self.model.resource_pool.delete_all()
        else:
            event.ignore()


class WorkflowInfoWidget(QtGui.QWidget):

    def __init__(self,
                 model,
                 assigned_wf_id=None,
                 assigned_resource_id=None,
                 parent=None):
        super(WorkflowInfoWidget, self).__init__(parent)

        self.ui = Ui_WStatusNameDate()
        self.ui.setupUi(self)

        self.model = model
        self.assigned_wf_id = assigned_wf_id
        self.assigned_resource_id = assigned_resource_id

        self.model.current_workflow_changed.connect(
            self.current_workflow_changed)
        self.model.workflow_state_changed.connect(self.update_workflow_status)

    def check_workflow(self):
        return self.assigned_wf_id == None or \
            (self.assigned_wf_id == self.model.current_wf_id and
                self.assigned_resource_id == self.model.current_resource_id)

    @QtCore.Slot()
    def update_workflow_status(self):
        if self.check_workflow():
            self.update_workflow_status_widgets(self.model.workflow_status,
                                                self.model.current_workflow().queue)

    def update_workflow_status_widgets(self, status, queue):
        if status == None:
            self.ui.wf_status.clear()
            self.ui.wf_queue.clear()
        else:
            self.ui.wf_status.setText(status)
            if queue == None:
                self.ui.wf_queue.setText("default queue")
            else:
                self.ui.wf_queue.setText(queue)
        icon_file_path = workflow_status_icon(status)
        if icon_file_path == None:
            pixmap = QtGui.QPixmap()
        else:
            image = QtGui.QImage(icon_file_path).scaled(30, 30)
            pixmap = QtGui.QPixmap.fromImage(image)
        self.ui.wf_status_icon.setPixmap(pixmap)

    @QtCore.Slot()
    def current_workflow_changed(self):
        if self.check_workflow():
            self.setEnabled(True)
            self.ui.wf_name.setEnabled(True)
            self.ui.wf_status_icon.setEnabled(True)
            self.ui.wf_id.setEnabled(True)
            if self.model.current_wf_id == None:
                self.ui.wf_name.clear()
                self.ui.wf_id.clear()
                self.update_workflow_status_widgets(None, None)
                self.ui.dateTimeEdit_expiration.setDateTime(datetime.now())
            elif self.model.current_wf_id == NOT_SUBMITTED_WF_ID:
                if self.model.workflow_name:
                    self.ui.wf_name.setText(self.model.workflow_name)
                else:
                    self.ui.wf_name.clear()
                self.ui.wf_id.clear()
                self.ui.wf_status.setText("not submitted")
                self.ui.wf_status_icon.setPixmap(QtGui.QPixmap())
                self.ui.dateTimeEdit_expiration.setDateTime(
                    datetime.now() + timedelta(days=5))
            else:
                if self.model.workflow_name:
                    self.ui.wf_name.setText(self.model.workflow_name)
                else:
                    self.ui.wf_name.setText(repr(self.model.current_wf_id))
                self.ui.wf_id.setText(repr(self.model.current_wf_id))
                self.update_workflow_status_widgets(self.model.workflow_status,
                                                    self.model.current_workflow().queue)
                self.ui.dateTimeEdit_expiration.setDateTime(
                    self.model.workflow_exp_date)
        elif self.assigned_wf_id != None:
            self.setEnabled(False)
            self.ui.wf_name.setEnabled(False)
            self.ui.wf_id.setEnabled(False)
            self.ui.wf_status_icon.setEnabled(False)


class SearchWidget(QtGui.QWidget):

    workflow_tree = None

    def __init__(self, workflow_tree, parent=None):
        '''
        * workflow_tree *WorkflowTree*
        '''
        super(SearchWidget, self).__init__(parent)

        self.workflow_tree = weakref.ref(workflow_tree)

        self.ui = Ui_SearchWidget()
        self.ui.setupUi(self)

        self.statuses = []
        self.statuses.append([])
        self.statuses.append([constants.FAILED])
        self.statuses.append([constants.RUNNING])
        self.statuses.append([constants.QUEUED_ACTIVE])
        self.statuses.append([constants.SUBMISSION_PENDING])
        self.statuses.append([constants.RUNNING, constants.QUEUED_ACTIVE])
        self.statuses.append(
            [constants.RUNNING, constants.QUEUED_ACTIVE, constants.SUBMISSION_PENDING])
        self.statuses.append([constants.DONE])

        self.ui.status_combo_box.addItem("All")
        self.ui.status_combo_box.addItem(
            QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/failed.png")), constants.FAILED)
        self.ui.status_combo_box.addItem(
            QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/running.png")), constants.RUNNING)
        self.ui.status_combo_box.addItem(
            QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/queued.png")), constants.QUEUED_ACTIVE)
        self.ui.status_combo_box.addItem(
            QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/pending.png")), constants.SUBMISSION_PENDING)
        self.ui.status_combo_box.addItem(
            constants.QUEUED_ACTIVE + " or " + constants.RUNNING)
        self.ui.status_combo_box.addItem(
            constants.SUBMISSION_PENDING + " or " + constants.QUEUED_ACTIVE + " or " + constants.RUNNING)
        self.ui.status_combo_box.addItem(
            QtGui.QIcon(os.path.join(os.path.dirname(__file__), "icon/success.png")), constants.DONE)

        self.ui.line_edit.textChanged.connect(self.text_changed)
        self.ui.expand_button.clicked.connect(
            self.workflow_tree().tree_view.expandAll)
        self.ui.status_combo_box.currentIndexChanged.connect(
            self.status_changed)

    @QtCore.Slot(int)
    def status_changed(self, index):
        if self.workflow_tree().proxy_model != None:
            self.workflow_tree().proxy_model.setFilterStatus(
                self.statuses[index])

    def text_changed(self, text):
        if self.workflow_tree().proxy_model != None:
            self.workflow_tree().proxy_model.setFilterRegExp(
                QtCore.QRegExp(text,
                               QtCore.Qt.CaseInsensitive,
                               QtCore.QRegExp.FixedString))


class JobFilterProxyModel(QtGui.QSortFilterProxyModel):

    def __init__(self, parent=None):
        super(JobFilterProxyModel, self).__init__(parent)

        self.statuses = []

    def setFilterStatus(self, statuses):
        self.statuses = statuses
        self.invalidateFilter()

    def filterAcceptsRow(self, sourceRow, sourceParent):

        index = self.sourceModel().index(sourceRow, 0, sourceParent)

        if isinstance(index.internalPointer(), GuiGroup):
            if len(self.statuses) != 0:
                group = index.internalPointer()
                if constants.DONE in self.statuses and len(group.done) > 0:
                    return True
                elif constants.FAILED in self.statuses and len(group.failed) > 0:
                    return True
                elif constants.RUNNING in self.statuses and len(group.running) > 0:
                    return True
                elif constants.QUEUED_ACTIVE in self.statuses and len(group.queued) > 0:
                    return True
                elif constants.SUBMISSION_PENDING in self.statuses and len(group.pending) > 0:
                    return True
                else:
                    return False
            return True
        elif isinstance(index.internalPointer(), GuiTransfer):
            return True
        elif isinstance(index.internalPointer(), GuiJob):
            if len(self.statuses) != 0:
                if constants.FAILED in self.statuses:
                    job = index.internalPointer()
                    if job.status == constants.FAILED:
                        return QtGui.QSortFilterProxyModel.filterAcceptsRow(
                            self,
                            sourceRow,
                            sourceParent)
                    elif job.status == constants.DONE:
                        exit_status, exit_value, term_signal, resource_usage = job.exit_info
                        if exit_status != constants.FINISHED_REGULARLY or exit_value != 0:
                            return QtGui.QSortFilterProxyModel.filterAcceptsRow(
                                self,
                                sourceRow,
                                sourceParent)
                        else:
                            return False
                    else:
                        return False
                elif constants.DONE in self.statuses:
                    job = index.internalPointer()
                    if job.status == constants.DONE:
                        exit_status, exit_value, term_signal, resource_usage = job.exit_info
                        if exit_status == constants.FINISHED_REGULARLY and exit_value == 0:
                            return QtGui.QSortFilterProxyModel.filterAcceptsRow(
                                self,
                                sourceRow,
                                sourceParent)
                        else:
                            return False
                    else:
                        return False
                elif constants.RUNNING in self.statuses:
                    job = index.internalPointer()
                    if job.status == constants.RUNNING or job.status == constants.UNDETERMINED:
                        return QtGui.QSortFilterProxyModel.filterAcceptsRow(
                            self,
                            sourceRow,
                            sourceParent)

                if index.internalPointer().status not in self.statuses:
                    return False

        return QtGui.QSortFilterProxyModel.filterAcceptsRow(self,
                                                            sourceRow,
                                                            sourceParent)


class WorkflowTree(QtGui.QWidget):

    '''
    '''

    assigned_wf_id = None

    selection_model_changed = QtCore.Signal(QtGui.QItemSelectionModel)

    tree_view = None

    # JobFilterProxyModel
    proxy_model = None

    search_widget = None

    def __init__(self,
                 model,
                 assigned_wf_id=None,
                 assigned_resource_id=None,
                 parent=None):
        super(WorkflowTree, self).__init__(parent)

        self.model = model
        self.item_model = None
        self.assigned_wf_id = assigned_wf_id
        self.assigned_resource_id = assigned_resource_id

        self.proxy_model = JobFilterProxyModel(self)
        self.model.workflow_state_changed.connect(self.proxy_model.invalidate)

        self.tree_view = QtGui.QTreeView(self)
        self.tree_view.setHeaderHidden(True)
        self.search_widget = SearchWidget(workflow_tree=self, parent=self)
        self.vLayout = QtGui.QVBoxLayout(self)
        self.vLayout.setContentsMargins(0, 0, 0, 0)
        self.vLayout.addWidget(self.tree_view)
        self.vLayout.addWidget(self.search_widget)

        self.model.current_connection_changed.connect(self.clear)
        self.model.current_workflow_about_to_change.connect(
            self.currentWorkflowAboutToChange)
        self.model.current_workflow_changed.connect(
            self.current_workflow_changed)
        self.model.workflow_state_changed.connect(self.dataChanged)
        # enable customContextMenuRequested signal to be emited
        self.tree_view.setContextMenuPolicy(
            QtCore.Qt.CustomContextMenu)
        self.tree_view.customContextMenuRequested.connect(self.openContextMenu)

    def check_workflow(self):
        return self.assigned_wf_id == None or \
            (self.assigned_wf_id == self.model.current_wf_id and
                self.assigned_resource_id == self.model.current_resource_id)

    @QtCore.Slot()
    def clear(self):
        if self.item_model:
            if self.assigned_wf_id != None:
                self.setEnabled(False)
            else:
                self.item_model.modelAboutToBeReset.emit()
                self.item_model = None
                self.tree_view.setModel(None)

    @QtCore.Slot()
    def currentWorkflowAboutToChange(self):
        if self.item_model:
            if self.assigned_wf_id != None:
                self.setEnabled(False)
            else:
                self.item_model.modelAboutToBeReset.emit()
                self.item_model = None
                self.tree_view.setModel(None)

    @QtCore.Slot()
    def current_workflow_changed(self):
        if self.model.current_wf_id != None:
            if self.check_workflow():
                self.setEnabled(True)
                workflow = self.model.current_workflow()
                if workflow != None:
                    self.item_model = WorkflowItemModel(workflow, self)
                    if self.proxy_model:
                        self.proxy_model.setSourceModel(self.item_model)
                        self.tree_view.setModel(self.proxy_model)
                    else:
                        self.tree_view.setModel(self.item_model)
                    self.item_model.modelReset.emit()
                    self.selection_model_changed.emit(
                        self.tree_view.selectionModel())
                else:
                    self.setEnabled(False)
            elif self.assigned_wf_id != None:
                self.setEnabled(False)

    @QtCore.Slot()
    def dataChanged(self):
        if self.item_model:
            if self.check_workflow():
                self.setEnabled(True)
                row = self.item_model.rowCount(QtCore.QModelIndex())
                self.item_model.dataChanged.emit(
                    self.item_model.index(0, 0, QtCore.QModelIndex()),
                    self.item_model.index(row, 0, QtCore.QModelIndex()))
            elif self.assigned_wf_id != None:
                self.setEnabled(False)

    def select_job(self, job_id):
        selection_model = self.tree_view.selectionModel()
        model = self.tree_view.model()
        found_item = None
        item_lists = [QtCore.QModelIndex()]
        while item_lists and found_item is None:
            parent = item_lists.pop(0)
            rows = model.rowCount(parent)
            for row in range(rows):
                index = model.index(row, 0, parent)
                source_index = index
                if self.proxy_model is not None:
                    source_index = model.mapToSource(index)
                data = source_index.internalPointer()
                if isinstance(data, GuiJob):
                    if data.job_id == job_id:
                        found_item = index
                        break
                elif isinstance(data, GuiGroup):
                    item_lists.append(index)
        if found_item is not None:
            selection_model.setCurrentIndex(
                found_item, QtGui.QItemSelectionModel.SelectCurrent)

    def selected_jobs(self, include_groups=True):
        selected_items = self.tree_view.selectedIndexes()
        if selected_items:
            if self.proxy_model is not None:
                model = self.tree_view.model()
                selected_items = [model.mapToSource(item)
                                  for item in selected_items]
            selected_items = [item.internalPointer() for item in selected_items]
            if include_groups:
                classes = (GuiJob, GuiGroup)
            else:
                classes = GuiJob
            selected_jobs = [item for item in selected_items
                             if isinstance(item, classes)]
            return selected_jobs
        return []

    def openContextMenu(self, point):
        # check that the workflow is running (or stopped?)
        wf_status = self.model.get_workflow_status(
            self.model.current_resource_id, self.model.current_wf_id)
        if wf_status != constants.WORKFLOW_IN_PROGRESS:
            print('workflow not running.')
            return

        selected_jobs = self.selected_jobs()
        if selected_jobs:
            popup = QtGui.QMenu()
            stop = popup.addAction('Stop jobs', self.stop_selected_jobs)
            restart = popup.addAction('Restart jobs',
                                      self.restart_selected_jobs)
            popup.exec(QtGui.QCursor.pos())

    def _expand_groups(self, jobs_groups):
        expanded = []
        todo = list(jobs_groups)
        while todo:
            item = todo.pop(0)
            if isinstance(item, GuiJob):
                expanded.append(item)
            elif isinstance(item, GuiGroup):
                todo += [item.gui_workflow.items[child]
                         for child in item.children]
        return expanded

    def stop_selected_jobs(self):
        selected_jobs = self._expand_groups(self.selected_jobs())
        if selected_jobs:
            reply = QtGui.QMessageBox.question(
                None,
                'Warning!!',
                "Stop selected jobs and dependencies ?",
                QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
            if reply == QtGui.QMessageBox.Yes:
                Controller.stop_jobs(self.model.current_wf_id,
                                     [j.job_id for j in selected_jobs],
                                     self.model.current_connection)
                return True
            else:
                return False

    def restart_selected_jobs(self):
        selected_jobs = self._expand_groups(self.selected_jobs())
        if selected_jobs:
            reply = QtGui.QMessageBox.question(
                None,
                'Warning!!',
                "Stop/restart selected jobs and dependencies ?",
                QtGui.QMessageBox.Yes, QtGui.QMessageBox.No)
            if reply == QtGui.QMessageBox.Yes:
                Controller.restart_jobs(self.model.current_wf_id,
                                        [j.job_id for j in selected_jobs],
                                        self.model.current_connection)
                return True
            else:
                return False


class WorkflowGroupInfo(QtGui.QWidget):

    def __init__(self, model, parent=None):
        super(WorkflowGroupInfo, self).__init__(parent)

        self.infoWidget = None
        self.vLayout = QtGui.QVBoxLayout(self)
        self.model = model

        self.model.current_connection_changed.connect(self.clear)
        self.model.current_workflow_about_to_change.connect(self.clear)
        self.model.current_workflow_changed.connect(
            self.current_workflow_changed)
        self.model.workflow_state_changed.connect(self.dataChanged)

    @QtCore.Slot()
    def clear(self):
        if self.infoWidget:
            self.infoWidget.hide()
            self.vLayout.removeWidget(self.infoWidget)
        self.infoWidget = None
        self.dataChanged()

    @QtCore.Slot()
    def current_workflow_changed(self):
        if self.infoWidget:
            self.infoWidget.hide()
            self.vLayout.removeWidget(self.infoWidget)
        if self.model.current_workflow():
            self.infoWidget = GroupInfoWidget(
                self.model.current_workflow().root_item, self)
        if self.infoWidget:
            self.vLayout.addWidget(self.infoWidget)
        self.update()

    @QtCore.Slot()
    def dataChanged(self):
        if self.infoWidget:
            self.infoWidget.dataChanged()


class WorkflowPlot(QtGui.QWidget):

    job_selected = QtCore.Signal(int)

    def __init__(self,
                 model,
                 assigned_wf_id=None,
                 assigned_resource_id=None,
                 parent=None):
        super(WorkflowPlot, self).__init__(parent)

        self.plotWidget = None
        self.vLayout = QtGui.QVBoxLayout(self)
        self.model = model
        self.assigned_wf_id = assigned_wf_id
        self.assigned_resource_id = assigned_resource_id

        if MATPLOTLIB:
            self.model.current_connection_changed.connect(self.clear)
            self.model.current_workflow_about_to_change.connect(self.clear)
            self.model.current_workflow_changed.connect(
                self.current_workflow_changed)
            self.model.workflow_state_changed.connect(self.dataChanged)

    def check_workflow(self):
        return self.assigned_wf_id == None or \
            (self.assigned_wf_id == self.model.current_wf_id and
                self.assigned_resource_id == self.model.current_resource_id)

    @QtCore.Slot()
    def clear(self):
        if self.assigned_wf_id != None:
            self.setEnabled(False)
        else:
            if self.plotWidget:
                self.plotWidget.hide()
                self.vLayout.removeWidget(self.plotWidget)
            self.plotWidget = None
            self.dataChanged()

    @QtCore.Slot()
    def current_workflow_changed(self):
        if self.check_workflow():
            self.setEnabled(True)
            if self.plotWidget:
                self.plotWidget.hide()
                self.vLayout.removeWidget(self.plotWidget)
            if self.model.current_workflow():
                self.plotWidget = PlotView(
                    self.model.current_workflow().root_item, self)
                self.plotWidget.job_selected.connect(self.job_selected)
            if self.plotWidget:
                self.vLayout.addWidget(self.plotWidget)
            self.update()
        elif self.assigned_wf_id != None:
            self.setEnabled(False)

    @QtCore.Slot()
    def dataChanged(self):
        if self.plotWidget:
            if self.check_workflow():
                self.setEnabled(True)
                self.plotWidget.dataChanged()
            elif self.assigned_wf_id != None:
                self.setEnabled(False)


class WorkflowElementInfo(QtGui.QWidget):

    # QtGui.QAbstractProxyModel used in the widget providing the selection
    # model
    proxy_model = None
    
    connection_closed_error = QtCore.Signal()
    job_selected = QtCore.Signal(int)

    def __init__(self, model, proxy_model=None, parent=None):
        super(WorkflowElementInfo, self).__init__(parent)
        self.selectionModel = None
        self.infoWidget = None
        self.model = model  # used to update stderr and stdout only
        self.proxy_model = proxy_model

        self.job_info_current_tab = 0

        self.model.workflow_state_changed.connect(self.dataChanged)
        self.model.current_connection_changed.connect(self.clear)
        self.model.current_workflow_changed.connect(self.clear)

        self.vLayout = QtGui.QVBoxLayout(self)

    @QtCore.Slot(QtGui.QItemSelectionModel)
    def setSelectionModel(self, selectionModel):
        if self.selectionModel:
            self.selectionModel.currentChanged[QtCore.QModelIndex,
                                               QtCore.QModelIndex].disconnect(
                self.currentChanged)

        self.selectionModel = selectionModel

        self.selectionModel.currentChanged[QtCore.QModelIndex, 
                                           QtCore.QModelIndex].connect(
            self.currentChanged)

        if self.infoWidget:
            self.infoWidget.hide()
            self.vLayout.removeWidget(self.infoWidget)
            self.infoWidget = None

    @QtCore.Slot()
    def clear(self):
        if self.infoWidget:
            self.infoWidget.hide()
            self.vLayout.removeWidget(self.infoWidget)
        self.infoWidget = None
        self.dataChanged()

    @QtCore.Slot(QtCore.QModelIndex, QtCore.QModelIndex)
    def currentChanged(self, current, previous):
        #print('WorkflowElementInfo.currentChanged')
        if self.infoWidget:
            if isinstance(self.infoWidget, JobInfoWidget):
                self.job_info_current_tab = self.infoWidget.currentIndex()
            self.infoWidget.hide()
            self.vLayout.removeWidget(self.infoWidget)
        if self.proxy_model != None:
            current = self.proxy_model.mapToSource(current)
        item = current.internalPointer()
        if isinstance(item, GuiJob):
            self.infoWidget \
                = JobInfoWidget(item,
                                weakref.proxy(self.model.current_connection),
                                self.job_info_current_tab,
                                self)
            self.infoWidget.source_job_selected.connect(self.job_selected)
        elif isinstance(item, GuiTransfer):
            self.infoWidget = TransferInfoWidget(item, self)
        elif isinstance(item, GuiGroup):
            self.infoWidget = GroupInfoWidget(item, self)
        else:
            self.infoWidget = None
        if self.infoWidget:
            self.vLayout.addWidget(self.infoWidget)
        self.update()

    @QtCore.Slot()
    def dataChanged(self):
        if self.infoWidget:
            self.infoWidget.dataChanged()

#
# VIEWS   #########################
#

class JobInfoWidget(QtGui.QTabWidget):

    source_job_selected = QtCore.Signal(int)

    def __init__(self,
                 job_item,
                 connection,
                 current_tab_index=0,
                 parent=None):
        super(JobInfoWidget, self).__init__(parent)

        self.ui = Ui_JobInfo()
        self.ui.setupUi(self)

        self.job_item = job_item
        self.connection = connection
        self.tab_index = {i: i for i in range(6)}

        if not job_item.data or (not job_item.data.has_outputs
                                 and not job_item.data.param_dict):
            self.removeTab(4)
            self.tab_index[4] = -1
            self.tab_index[5] = 4
            if current_tab_index == 4:
                current_tab_index = 0
        else:
            if not job_item.data.param_dict:
                self.ui.input_params_contents.hide()
                self.ui.input_params_label.hide()
            elif not job_item.data.has_outputs:
                self.ui.output_params_contents.hide()
                self.ui.output_params_label.hide()
            if job_item.data.param_dict:
                table = self.ui.input_params_contents
                table.setRowCount(len(job_item.data.param_dict))
                table.horizontalHeader().setSectionResizeMode(
                    QtGui.QHeaderView.ResizeToContents)
                if job_item.gui_workflow():
                    workflow = job_item.gui_workflow().server_workflow
                else:
                    workflow = None
                job_mapping = getattr(workflow, 'job_mapping', None)
                engine_wf = True
                if job_mapping is None:
                    # workflow loaded but not submitted
                    engine_wf = False
                    job_mapping = {}
                    for item_id, item \
                            in six.iteritems(job_item.gui_workflow().items):
                        if isinstance(item, GuiJob):
                            job_mapping[item.data] = item.it_id
                for i, param_name in \
                        enumerate(sorted(job_item.data.param_dict.keys())):
                    value = job_item.data.param_dict[param_name]
                    table.setItem(i, 0, QtGui.QTableWidgetItem(param_name))
                    table.setItem(i, 1, QtGui.QTableWidgetItem(repr(value)))
                    if workflow:
                        links = workflow.param_links.get(job_item.data)
                        if links:
                            linkl = links.get(param_name, [])
                            if linkl:
                                linkstr = []
                                job_ids = []
                                for link in linkl:
                                    linkstr.append(
                                        '%s.%s' % (link[0].name, link[1]))
                                    if engine_wf:
                                        job_ids.append(job_mapping.get(
                                            link[0], link[0]).job_id)
                                    else:
                                        job_ids.append(job_mapping.get(
                                            link[0]))
                                linkstr = ', '.join(linkstr)
                                item = QtGui.QTableWidgetItem(linkstr)
                                item.job_ids = job_ids
                                table.setItem(i, 2, item)
                table.cellDoubleClicked.connect(
                    self.input_param_double_clicked)

        self.setCurrentIndex(current_tab_index)

        self.dataChanged()
        if current_tab_index == self.tab_index[4]:
            self.refresh_params()
        elif current_tab_index in (self.tab_index[1], self.tab_index[2]):
            self.refreshStdErrOut()
        elif current_tab_index == self.tab_index[5]:
            self.refresh_envar()

        self.currentChanged.connect(self.currentTabChanged)
        self.ui.stderr_refresh_button.clicked.connect(self.refreshStdErrOut)
        self.ui.stdout_refresh_button.clicked.connect(self.refreshStdErrOut)
        self.ui.params_refresh_button.clicked.connect(self.refresh_params)
        self.ui.envar_copy_btn.clicked.connect(self.copy_envars)

    def dataChanged(self):

        # reread command if needed
        if self.ui.job_status.text() == '' \
                or (self.ui.job_status.text() == constants.NOT_SUBMITTED
                    and self.job_item.status != self.ui.job_status.text()):
            self.job_item.update_job_command(self.connection)
        setLabelFromString(self.ui.job_name, self.job_item.name)
        setLabelFromString(self.ui.job_status, self.job_item.status)
        exit_status, exit_value, term_signal, resource_usage = self.job_item.exit_info
        setLabelFromString(self.ui.exit_status, exit_status)
        setLabelFromInt(self.ui.exit_value, exit_value)
        setLabelFromString(self.ui.term_signal, term_signal)
        setTextEditFromString(self.ui.command, self.job_item.command)
        setLabelFromInt(self.ui.priority, self.job_item.priority)
        setLabelFromString(self.ui.queue, self.job_item.queue)

        if resource_usage:
            if six.PY3 and isinstance(resource_usage, bytes):
                resource_usage = resource_usage.decode()
            self.ui.resource_usage.clear()
            self.ui.resource_usage.insertItems(0, resource_usage.split())
        else:
            self.ui.resource_usage.clear()

        setLabelFromDateTime(
            self.ui.submission_date, self.job_item.submission_date)
        setLabelFromDateTime(
            self.ui.execution_date, self.job_item.execution_date)
        setLabelFromDateTime(self.ui.ending_date, self.job_item.ending_date)
        setLabelFromInt(self.ui.job_id, self.job_item.job_id)
        setLabelFromString(self.ui.drms_job_id, self.job_item.drmaa_id)
        if self.job_item.submission_date:
            if self.job_item.execution_date:
                time_in_queue = self.job_item.execution_date - \
                    self.job_item.submission_date
                setLabelFromTimeDelta(self.ui.time_in_queue, time_in_queue)
                if self.job_item.ending_date:
                    execution_time = self.job_item.serial_duration
                    setLabelFromTimeDelta(
                        self.ui.execution_time, execution_time)

        setTextEditFromString(
            self.ui.stdout_file_contents, self.job_item.stdout)
        setTextEditFromString(
            self.ui.stderr_file_contents, self.job_item.stderr)

    @QtCore.Slot(int)
    def currentTabChanged(self, index):
        if index == self.tab_index[0]:
            try:
                self.job_item.update_job_command(self.connection)
            except ConnectionClosedError as e:
                self.parent.connection_closed_error[()].emit()
            else:
                self.dataChanged()
        elif (index == self.tab_index[1] or index == self.tab_index[2]) \
                and self.job_item.stdout == "":
            try:
                self.job_item.updateStdOutErr(self.connection)
            except ConnectionClosedError as e:
                self.parent.connection_closed_error[()].emit()
            else:
                self.dataChanged()
        elif index == self.tab_index[4]:
            self.refresh_params()
        elif index == self.tab_index[5]:
            self.refresh_envar()

    @QtCore.Slot()
    def refreshStdErrOut(self):
        try:
            self.job_item.updateStdOutErr(self.connection)
        except ConnectionClosedError as e:
            self.parent().connection_closed_error[()].emit()
        self.dataChanged()

    @QtCore.Slot()
    def refresh_params(self):
        if self.job_item.data is None:
            return
        try:
            self.job_item.update_job_params(self.connection)
        except ConnectionClosedError as e:
            self.parent().connection_closed_error[()].emit()
            return
        if self.job_item.data.param_dict:
            table = self.ui.input_params_contents
            param_dict = self.job_item.data.param_dict
            for row in range(table.rowCount()):
                param_name = table.item(row, 0).text()
                if param_name in param_dict:
                    value = param_dict[param_name]
                    table.setItem(row, 1, QtGui.QTableWidgetItem(repr(value)))
        if self.job_item.data.has_outputs:
            table = self.ui.output_params_contents
            table.clearContents()
            output_params = getattr(self.job_item, 'output_params', None)
            if output_params is not None:
                table.setRowCount(len(output_params))
                table.horizontalHeader().setSectionResizeMode(
                    QtGui.QHeaderView.ResizeToContents)
                for row, param_name in enumerate(sorted(output_params.keys())):
                    value = output_params[param_name]
                    table.setItem(row, 0, QtGui.QTableWidgetItem(param_name))
                    table.setItem(row, 1, QtGui.QTableWidgetItem(repr(value)))

    @QtCore.Slot(int, int)
    def input_param_double_clicked(self, row, col):
        if col == 2:
            item = self.ui.input_params_contents.item(row, col)
            job_ids = getattr(item, 'job_ids', None)
            if job_ids:
                self.source_job_selected.emit(job_ids[0])

    def get_envars(self):
        if self.job_item.data is None:
            self.ui.envar_table.clear()
            return {}
        env = {}
        gui_w = self.job_item.gui_workflow()
        if gui_w:
            workflow = gui_w.server_workflow
        else:
            workflow = None
        if workflow:
            job_mapping = getattr(workflow, 'job_mapping', None)
            if job_mapping:
                ejob = job_mapping[self.job_item.data]
                in_param_file = ejob.plain_input_params_file()
                if in_param_file:
                    env['SOMAWF_INPUT_PARAMS'] = in_param_file
                out_param_file = ejob.plain_output_params_file()
                if out_param_file:
                    env['SOMAWF_OUTPUT_PARAMS'] = out_param_file
                if ejob.env:
                    env.update(ejob.env)
        if self.job_item.data.env:
            env.update(self.job_item.data.env)
        return env

    @QtCore.Slot()
    def refresh_envar(self):
        env = self.get_envars()
        table = self.ui.envar_table
        table.clearContents()
        table.setRowCount(len(env))
        row = 0
        for var, value in six.iteritems(env):
            table.setItem(row, 0, QtGui.QTableWidgetItem(var))
            table.setItem(row, 1, QtGui.QTableWidgetItem(value))
            row += 1
        table.horizontalHeader().setSectionResizeMode(
            QtGui.QHeaderView.ResizeToContents)

    def copy_envars(self):
        def _repl(s):
            sl = [c if c != '"' else '\\"' for c in s]
            return ''.join(sl)
        env = self.get_envars()
        txt_env = ' '.join(['%s="%s"' % (k, _repl(v)) for k, v in six.iteritems(env)])
        clipboard = QtGui.qApp.clipboard()
        clipboard.setText(txt_env)


class TransferInfoWidget(QtGui.QTabWidget):

    def __init__(self, transfer_item, parent=None):
        super(TransferInfoWidget, self).__init__(parent)
        self.ui = Ui_TransferInfo()
        self.ui.setupUi(self)

        self.transfer_item = transfer_item
        self.dataChanged()

    def dataChanged(self):

        setLabelFromString(self.ui.transfer_name, self.transfer_item.name)
        setLabelFromString(self.ui.transfer_status,
                           self.transfer_item.transfer_status)
        if isinstance(self.transfer_item.data, FileTransfer):
            setLabelFromString(self.ui.client_path,
                               self.transfer_item.data.client_path)
            if self.transfer_item.data.client_paths:
                self.ui.client_paths.insertItems(0,
                                                 self.transfer_item.data.client_paths)
            else:
                self.ui.client_paths.clear()
        elif isinstance(self.transfer_item.data, TemporaryPath):
            if self.transfer_item.data.is_directory:
                setLabelFromString(
                    self.ui.client_path, '<temporary directory>')
            else:
                setLabelFromString(self.ui.client_path, '<temporary file>')
            self.ui.client_paths.clear()
        setLabelFromString(self.ui.cr_path, self.transfer_item.engine_path)


class GroupInfoWidget(QtGui.QWidget):

    def __init__(self, group_item, parent=None):
        super(GroupInfoWidget, self).__init__(parent)
        self.ui = Ui_GroupInfo()
        self.ui.setupUi(self)

        self.group_item = group_item
        self.dataChanged()

    def dataChanged(self):

        # job_nb = len(self.group_item.not_sub) + len(self.group_item.done) +
        # len(self.group_item.failed) + len(self.group_item.running) +
        # len(self.group_item.warning) + len(self.group_item.queued)

        ended_job_nb = len(self.group_item.done) + len(self.group_item.failed)

        total_time = None
        if self.group_item.first_sub_date != datetime.max:
            if self.group_item.first_sub_date:
                if self.group_item.last_end_date:
                    total_time = self.group_item.last_end_date - \
                        self.group_item.first_sub_date
                else:
                    total_time = datetime.now() - \
                        self.group_item.first_sub_date

        # input_file_nb = len(self.group_item.input_to_transfer) + len(self.group_item.input_transfer_ended)
        # ended_input_transfer_nb = len(self.group_item.input_transfer_ended)
        setLabelFromString(self.ui.status, self.group_item.status)
        setLabelFromInt(self.ui.job_nb, self.group_item.job_count)
        setLabelFromInt(self.ui.ended_job_nb, ended_job_nb)
        setLabelFromTimeDelta(self.ui.total_time, total_time)
        setLabelFromTimeDelta(
            self.ui.theoretical_serial_time, self.group_item.theoretical_serial_time)
        # setLabelFromInt(self.ui.input_file_nb, input_file_nb)
        # setLabelFromInt(self.ui.ended_input_transfer_nb,
        # ended_input_transfer_nb)

        # if self.group_item.input_to_transfer:
            # self.ui.comboBox_input_to_transfer.insertItems(0,  self.group_item.input_to_transfer)
        # else:
            # self.ui.comboBox_input_to_transfer.clear()

        # if self.group_item.output_ready:
            # self.ui.comboBox_output_to_transfer.insertItems(0,  self.group_item.output_ready)
        # else:
            # self.ui.comboBox_output_to_transfer.clear()


class PlotView(QtGui.QWidget):

    job_selected = QtCore.Signal(int)

    def __init__(self, group_item, parent=None):
        super(PlotView, self).__init__(parent)

        self.ui = Ui_PlotWidget()
        self.ui.setupUi(self)
        self.vlayout = QtGui.QVBoxLayout()
        self.ui.frame_plot.setLayout(self.vlayout)

        self.ui.combo_plot_type.addItems(["jobs fct time",
                                          "jobs+cpu fct time",
                                          "nb jobs fct time",
                                          "nb cpu fct time"])
        self.ui.combo_plot_type.setCurrentIndex(0)
        self.plot_type = 0

        self.canvas = None
        self.group_item = group_item
        self.updatePlot()

        self.ui.combo_plot_type.currentIndexChanged.connect(
            self.plotTypeChanged)
        self.ui.button_refresh.clicked.connect(self.refresh)

    def __del__(self):
        if self.canvas is not None:
            del self.axes
            matplotlib.pyplot.close(self.figure)
            self.canvas.setAttribute(QtCore.Qt.WA_DeleteOnClose, True)
            self.canvas.close()

    def update_jobs(self):
        self.jobs = list(self.group_item.done)
        self.jobs.extend(self.group_item.failed)
        self.jobs.extend(self.group_item.running)
        self.jobs.extend(self.group_item.not_sub)
        self.jobs = sorted(self.jobs, key=self.sortkey)

    def sortkey(self, j):
        if j.execution_date:
            return j.execution_date
        else:
            return datetime.max

    @QtCore.Slot(int)
    def plotTypeChanged(self, index):
        self.plot_type = index
        self.updatePlot()

    QtCore.Slot()

    def refresh(self):
        self.updatePlot()

    def dataChanged(self):
        if self.ui.checkbox_auto_update.isChecked():
            self.updatePlot()

    def updatePlot(self):
        if not self.isVisible():
            return
        self.update_jobs()
        if self.plot_type in (0, 1):
            self.jobsFctTime()
        if self.plot_type in (2, 3):
            self.nbProcFctTime()

    def jobsFctTime(self):

        if self.canvas:
            self.axes.clear()
            # self.canvas.hide()
            # self.vlayout.removeWidget(self.canvas)
            # matplotlib.pyplot.close( self.figure )
            # self.canvas.setAttribute( QtCore.Qt.WA_DeleteOnClose, True )
            # self.canvas.close()
            # self.canvas = None

        else:
            self.figure = Figure()
            self.axes = self.figure.add_subplot(111)
            if int(matplotlib.__version__.split('.')[0]) <= 2:
                self.axes.hold(True)
            self.canvas = FigureCanvas(self.figure)
            try:
                self.canvas.setParent(self)
            except TypeError as e:
                print("WARNING: The error might come from a mismatch between the matplotlib qt4 backend and the one used by soma.workflow "
                      + repr(QT_BACKEND))
                return
            self.canvas.updateGeometry()
            self.vlayout.addWidget(self.canvas)

            self._pick = self.canvas.mpl_connect('pick_event',
                                                 self._jobs_mouse_press)

        def key(j):
            if j.execution_date:
                return j.execution_date
            else:
                return datetime.max

        #self.jobs = sorted(self.jobs, key=self.sortkey)

        nb_jobs = 0
        x_min = datetime.max
        x_max = datetime.min
        n = 0
        #cols = matplotlib.rcParams['axes.prop_cycle']
        cols = [[0.0, 0.0, 1.0],
                [0.0, 0.0, 0.5],
                [0.0, 0.36470588235294116, 0.7725490196078432],
                [0.0, 0.18235294117647058, 0.3862745098039216],
                [0.0, 0.7098039215686275, 0.5529411764705883],
                [0.0, 0.35490196078431374, 0.27647058823529413],
                [0.0, 0.9803921568627451, 0.35294117647058826],
                [0.0, 0.49019607843137253, 0.17647058823529413],
                [0.23529411764705882, 1.0, 0.21568627450980393],
                [0.11764705882352941, 0.5, 0.10784313725490197],
                [0.5725490196078431, 1.0, 0.11764705882352941],
                [0.28627450980392155, 0.5, 0.058823529411764705],
                [0.8901960784313725, 1.0, 0.0392156862745098],
                [0.44509803921568625, 0.5, 0.0196078431372549],
                [1.0, 0.9882352941176471, 0.0],
                [0.5, 0.49411764705882355, 0.0],
                [1.0, 0.7294117647058823, 0.0],
                [0.5, 0.36470588235294116, 0.0],
                [1.0, 0.38823529411764707, 0.0],
                [0.5, 0.19411764705882353, 0.0],
                [1.0, 0.00392156862745098, 0.0],
                [0.5, 0.00196078431372549, 0.0]]
        # darken a bit
        cols = [[x*0.75 for x in c] for c in cols]

        for j in self.jobs:
            ncpu = 1
            if self.plot_type == 1 and j.parallel_job_info:
                ncpu = j.parallel_job_info.get('cpu_per_node', 1) \
                    * j.parallel_job_info.get('nodes_number', 1)
            if j.execution_date:
                nc = len(cols)
                #for c in cols[n % nc:(n % nc) + 1]:
                    #col = c['color']
                    #break
                col = cols[n % nc]
                n += 1
                nb_jobs = nb_jobs + 1
                if j.execution_date < x_min:
                    x_min = j.execution_date
                kwargs = {'picker': True, 'pickradius': 2}
                if j.ending_date:
                    if ncpu == 1:
                        self.axes.plot(
                            [j.execution_date, j.ending_date],
                            [nb_jobs, nb_jobs], color=col, **kwargs)
                        # link to job
                        self.axes.lines[-1].job = j.job_id
                    else:
                        self.axes.fill(
                            [j.execution_date, j.ending_date,
                             j.ending_date, j.execution_date],
                            [nb_jobs, nb_jobs, nb_jobs + ncpu - 1,
                             nb_jobs + ncpu - 1], color=col, **kwargs)
                        # link to job
                        self.axes.patches[-1].job = j.job_id
                    if j.ending_date > x_max:
                        x_max = j.ending_date
                else:
                    if ncpu == 1:
                        self.axes.plot(
                            [j.execution_date, datetime.now()],
                            [nb_jobs, nb_jobs], color=col, **kwargs)
                        # link to job
                        self.axes.lines[-1].job = j.job_id
                    else:
                        now = datetime.now()
                        self.axes.fill(
                            [j.execution_date, now, now, j.execution_date],
                            [nb_jobs, nb_jobs, nb_jobs + ncpu - 1,
                             nb_jobs + ncpu - 1], color=col, **kwargs)
                        # link to job
                        self.axes.patches[-1].job = j.job_id
                nb_jobs += ncpu - 1
        #print('njobs:', n, 'nb_jobs:', nb_jobs)

        if nb_jobs:
            self.axes.set_ylim(0, nb_jobs + 1)

        # self.axes.set_xlabel("Time")
        locator = mdates.AutoDateLocator(minticks=3)
        # locator = mdates.MinuteLocator(interval=2)
        self.axes.xaxis.set_major_locator(locator)
        self.axes.xaxis.set_major_formatter(mdates.ConciseDateFormatter(
            locator))
        if self.plot_type == 1:
            self.axes.set_ylabel("CPUs")
        else:
            self.axes.set_ylabel("Jobs")
        self.figure.autofmt_xdate(rotation=80)

        self.canvas.draw()

        self.update()

    def nbProcFctTime(self):

        if self.canvas:
            self.axes.clear()
            # self.canvas.hide()
            # self.vlayout.removeWidget(self.canvas)
            # self.canvas = None
        else:
            self.figure = Figure()
            self.axes = self.figure.add_subplot(111)
            if int(matplotlib.__version__.split('.')[0]) <= 2:
                self.axes.hold(True)
            self.canvas = FigureCanvas(self.figure)
            self.canvas.setParent(self)
            self.canvas.updateGeometry()
            self.vlayout.addWidget(self.canvas)

        dates = []
        nb_process_running = []
        infos = []  # sequence of tuple (job_item, start, date, ncpu)
                   # start is a bolean
                   # if start then date is the execution date
                   # else date is the ending date

        for job_item in self.jobs:
            ncpu = 1
            if self.plot_type == 3 and job_item.parallel_job_info:
                ncpu = job_item.parallel_job_info.get('cpu_per_node', 1) \
                    * job_item.parallel_job_info.get('nodes_number', 1)
            if job_item.execution_date:
                infos.append((job_item, True, job_item.execution_date, ncpu))
            if job_item.ending_date:
                infos.append((job_item, False, job_item.ending_date, ncpu))
            else:
                infos.append((job_item, False, datetime.now(), ncpu))

        infos = sorted(infos, key=lambda info_elem: info_elem[2])

        nb_process = 0
        previous = None
        for info_elem in infos:
            ncpu = info_elem[3]
            if previous and info_elem[2] == previous[2]:
                if info_elem[1]:
                    nb_process = nb_process + ncpu
                else:
                    nb_process = nb_process - ncpu
                nb_process_running[len(nb_process_running) - 1] = nb_process
            else:
                dates.append(info_elem[2])
                if info_elem[1]:
                    nb_process = nb_process + ncpu
                else:
                    nb_process = nb_process - ncpu
                nb_process_running.append(nb_process)
            previous = info_elem

        nb_proc_max = 0
        for i in range(1, len(nb_process_running)):
            self.axes.fill_between([dates[i - 1], dates[i]], [
                                   nb_process_running[i - 1], nb_process_running[i - 1]], y2=0, edgecolor='b')
            if nb_process_running[i] > nb_proc_max:
                nb_proc_max = nb_process_running[i]

        if nb_proc_max != 0:
            self.axes.set_ylim(0, nb_proc_max + 1)

        # self.axes.set_xlabel("Time")
        locator = mdates.AutoDateLocator(minticks=3)
        # locator = mdates.MinuteLocator() #interval=10)
        self.axes.xaxis.set_major_locator(locator)
        self.axes.xaxis.set_major_formatter(mdates.ConciseDateFormatter(
            locator))
        if self.plot_type == 3:
            self.axes.set_ylabel("Nb of CPU")
        else:
            self.axes.set_ylabel("Nb of jobs")
        self.figure.autofmt_xdate(rotation=80)

        self.canvas.draw()
        self.update()

    def _jobs_mouse_press(self, event):
        '''matplotlib callback for picker event
        '''
        fig = event.canvas.figure
        artist = event.artist
        job = getattr(artist, 'job', None)
        if job is not None:
          self.job_selected.emit(job)



class WorkflowGraphView(QtGui.QWidget):

    def __init__(self, model=None, parent=None):
        super(WorkflowGraphView, self).__init__(parent)
        self.ui = Ui_GraphWidget()
        self.ui.setupUi(self)

        self.workflow = None
        self.connection = None
        self.draw_enabled = False
        self._data_changed = False

        self.image_label = QtGui.QLabel(self)
        self.image_label.setBackgroundRole(QtGui.QPalette.Base)
        self.image_label.setSizePolicy(
            QtGui.QSizePolicy.Ignored, QtGui.QSizePolicy.Ignored)
        self.image_label.setScaledContents(True)

        self.ui.scrollArea.setBackgroundRole(QtGui.QPalette.Dark)
        # self.ui.scrollArea.setWidget(self.image_label)
        self.ui.scrollArea.setWidgetResizable(False)

        self.ui.zoom_slider.setRange(10, 200)
        self.ui.zoom_slider.sliderMoved.connect(self.zoomChanged)
        self.ui.zoom_slider.setValue(100)
        self.scale_factor = 1.0

        self.ui.adjust_size_checkBox.stateChanged.connect(
            self.adjustSizeChanged)
        self.ui.button_refresh.clicked.connect(self.refresh)

        self.model = model
        if model is not None:
            self.model.current_workflow_changed.connect(
                self.current_workflow_changed)
            self.model.current_connection_changed.connect(
                self.current_workflow_changed)

    @QtCore.Slot()
    def current_workflow_changed(self):
        if self.model is not None:
            gui_workflow = self.model.current_workflow()
            if gui_workflow is None:
                workflow = None
            else:
                workflow = gui_workflow.server_workflow
            self.setWorkflow(workflow,
                             self.model.current_connection)
            self.refresh()
        else:
            self.setWorkflow(None, None)

    def setWorkflow(self, workflow, connection):
        self.workflow = workflow
        self.connection = connection
        self.dataChanged()

    def clear(self):
        self.workflow = None
        self.dataChanged()

    @QtCore.Slot(int)
    def zoomChanged(self, percentage):
        self.scale_factor = percentage / 100.0
        if self.workflow:
            if self.image_label.pixmap():
                self.image_label.resize(
                    self.image_label.pixmap().size() * self.scale_factor)

    @QtCore.Slot(int)
    def adjustSizeChanged(self, state):
        if self.ui.adjust_size_checkBox.isChecked():
            pass
            # TBI

    @QtCore.Slot()
    def refresh(self):
        self.dataChanged(force=True)

    @QtCore.Slot()
    def dataChanged(self, force=False):
        if not self.draw_enabled:
            self._data_changed = True
            return
        if self.workflow and (force or
                              self.ui.checkbox_auto_update.isChecked()):
            self.rebuild_graph()
        else:
            self.ui.scrollArea.takeWidget()
        self._data_changed = False

    def rebuild_graph(self):
        image_file_path = self.printWorkflow()
        if image_file_path is not None:
            image = QtGui.QImage(image_file_path)
            pixmap = QtGui.QPixmap.fromImage(image)
            self.image_label.setPixmap(pixmap)
            self.ui.scrollArea.setWidget(self.image_label)
            self.image_label.resize(
                self.image_label.pixmap().size() * self.scale_factor)
        self._data_changed = False

    def graph_visibility_changed(self, visible):
        self.draw_enabled = visible
        if visible and self._data_changed:
            self.rebuild_graph()

    def printWorkflow(self):

        import tempfile
        from distutils.spawn import find_executable
        
        if not find_executable("dot"):
            print("Unable to print workflow because dot executable is not",
                  "available.", file = sys.stderr)
            return None
        
        output_dir = tempfile.gettempdir()

        GRAY = "\"#C8C8B4\""
        BLUE = "\"#00C8FF\""
        RED = "\"#FF6432\""
        GREEN = "\"#9BFF32\""
        LIGHT_BLUE = "\"#C8FFFF\""

        names = dict()
        current_id = 0

        dot_file_path = os.path.join(output_dir, "tmp.dot")
        graph_file_path = os.path.join(output_dir, "tmp.png")
        if dot_file_path and os.path.isfile(dot_file_path):
            os.remove(dot_file_path)
        file = open(dot_file_path, "w")
        print("digraph G {", file=file)
        for node in self.workflow.jobs:
            current_id = current_id + 1
            names[node] = ("node" + repr(current_id), "\"" + node.name + "\"")
        for ar in self.workflow.dependencies:
            print(names[ar[0]][0] + " -> " + names[ar[1]][0], file=file)
        for node in self.workflow.jobs:
            if isinstance(node, Job):
                if not hasattr(node, "job_id") \
                        or node.job_id == NOT_SUBMITTED_JOB_ID:
                    print(names[node][0] + "[shape=box label="
                          + names[node][1] + "];", file=file)
                else:
                    status = self.connection.job_status(node.job_id)
                    if status == constants.NOT_SUBMITTED:
                        print(names[node][0] + "[shape=box label="
                              + names[node][1] + ", style=filled, color="
                              + GRAY + "];", file=file)
                    elif status == constants.DONE:
                        exit_status, exit_value, term_signal, resource_usage \
                            = self.connection.job_termination_status(
                                node.job_id)
                        if exit_status == constants.FINISHED_REGULARLY \
                                and exit_value == 0:
                            print(names[node][0] + "[shape=box label="
                                  + names[node][1] + ", style=filled, color="
                                  + LIGHT_BLUE + "];", file=file)
                        else:
                            print(names[node][0] + "[shape=box label="
                                  + names[node][1] + ", style=filled, color="
                                  + RED + "];", file=file)
                    elif status == constants.FAILED:
                        print(names[node][0] + "[shape=box label="
                              + names[node][1] + ", style=filled, color="
                              + RED + "];", file=file)
                    else:
                        print(names[node][0] + "[shape=box label="
                              + names[node][1] + ", style=filled, color="
                              + GREEN + "];", file=file)
            if isinstance(node, FileTransfer):
                if not hasattr(node, "engine_path") or not node.engine_path:
                    print(names[node][0] + "[label=" + names[node][1] + "];",
                          file=file)
                else:
                    status = self.connection.transfer_status(
                        node.engine_path)[0]
                    if status == constants.FILES_DONT_EXIST:
                        print(names[node][0] + "[label="
                              + names[node][1] + ", style=filled, color="
                              + GRAY + "];", file=file)
                    elif status == constants.FILES_ON_CR \
                            or status == constants.FILES_ON_CLIENT_AND_CR \
                            or status == constants.FILES_ON_CLIENT:
                        print(names[node][0] + "[label="
                              + names[node][1] + ", style=filled, color="
                              + BLUE + "];", file=file)
                    elif status == constants.TRANSFERING_FROM_CLIENT_TO_CR or \
                            status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                        print(names[node][0] + "[label="
                              + names[node][1] + ", style=filled, color="
                              + GREEN + "];", file=file)
                    elif status == constants.FILES_UNDER_EDITION:
                        print(names[node][0] + "[label=" + names[node][1]
                              + ", style=filled, color=" + LIGHT_BLUE + "];",
                              file=file)

        print("}", file=file)
        file.close()

        command = ["dot", "-Tpng", dot_file_path, "-o", graph_file_path]
        subprocess.check_call(command)
        return graph_file_path


#
# MODEL FOR THE TREE VIEW   ###############
#

class WorkflowItemModel(QtCore.QAbstractItemModel):

    def __init__(self, gui_workflow, parent=None):
        super(WorkflowItemModel, self).__init__(parent)
        self.workflow = gui_workflow

        self.group_done_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/group_done.png"))
        self.group_failed_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/group_failed.png"))
        self.group_running_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/group_running.png"))
        self.group_no_status_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/group_no_status.png"))
        self.group_warning_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/group_warning.png"))

        self.running_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/running.png"))
        self.failed_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/failed.png"))
        self.done_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/success.png"))
        self.pending_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/pending.png"))
        self.queued_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/queued.png"))
        self.undetermined_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/undetermined.png"))
        self.warning_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/warning.png"))
        self.kill_delete_pending_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/kill_delete_pending.png"))
        self.no_status_icon = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/no_status.png"))

        self.transfer_files_dont_exit = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/transfer_files_dont_exist.png"))
        self.transfer_files_on_client = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/transfer_files_on_client.png"))
        self.transfer_files_on_both = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/transfer_files_on_both.png"))
        self.transfer_files_on_cr = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/transfer_files_on_cr.png"))
        self.transfering_from_client_to_cr = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/transfering_from_client_to_cr.png"))
        self.transfering_from_cr_to_client = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/transfering_from_cr_to_client.png"))
        self.files_under_edition = QtGui.QIcon(
            os.path.join(os.path.dirname(__file__), "icon/files_under_edition.png"))

    def index(self, row, column, parent=QtCore.QModelIndex()):
        if row < 0 or not column == 0:
            return QtCore.QModelIndex()

        if not parent.isValid():
            if row < len(self.workflow.root_item.children):
                return self.createIndex(row, column, self.workflow.items[self.workflow.root_item.children[row]])
        else:
            parent_item = parent.internalPointer()
            if row < len(parent_item.children):
                if parent_item.children[row] < len(self.workflow.items):
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
            if use_qvariant:
                return QtCore.QVariant()
            else:
                return None

        item = index.internalPointer()

        # if not item.initiated:
            # WIP

        # Groups ####
        if isinstance(item, GuiGroup):
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

        # Jobs ####
        if isinstance(item, GuiJob):
            if item.job_id == NOT_SUBMITTED_JOB_ID:
                if role == QtCore.Qt.DisplayRole:
                    return item.name
                if role == QtCore.Qt.DecorationRole:
                    return self.no_status_icon
            else:
                status = item.status
                # Done or Failed
                if status == constants.DONE or status == constants.FAILED:
                    exit_status, exit_value, term_signal, resource_usage = item.exit_info
                    if role == QtCore.Qt.DisplayRole:
                        return item.name  # + " status " + repr(exit_status) + " exit_value: " + repr(exit_value) + " signal " + repr(term_signal)
                    if role == QtCore.Qt.DecorationRole:
                        if status == constants.DONE and exit_status == constants.FINISHED_REGULARLY and exit_value == 0:
                            return self.done_icon
                        elif status == constants.DONE and exit_status == None:
                            return self.undetermined_icon
                        else:
                            return self.failed_icon

                if role == QtCore.Qt.DisplayRole:
                    return item.name  # + " " + status

                if role == QtCore.Qt.DecorationRole:
                    if status == constants.NOT_SUBMITTED:
                        return QtGui.QIcon()
                    if status == constants.UNDETERMINED:
                        return self.undetermined_icon
                    elif status == constants.QUEUED_ACTIVE:
                        return self.queued_icon
                    elif status == constants.RUNNING:
                        return self.running_icon
                    elif status == constants.DELETE_PENDING or status == constants.KILL_PENDING:
                        return self.kill_delete_pending_icon
                    elif status == constants.SUBMISSION_PENDING:
                        return self.pending_icon
                    elif status == constants.WARNING:
                        return self.warning_icon
                    else:  # SYSTEM_ON_HOLD USER_ON_HOLD USER_SYSTEM_ON_HOLD
                            # SYSTEM_SUSPENDED USER_SUSPENDED
                            # USER_SYSTEM_SUSPENDED
                        return self.warning_icon

        # FileTransfers ####
        if isinstance(item, GuiTransfer):
            if isinstance(item, GuiInputTransfer):
                # if role == QtCore.Qt.ForegroundRole:
                    # return QtGui.QBrush(RED)
                if item.transfer_status == constants.TRANSFERING_FROM_CLIENT_TO_CR or item.transfer_status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                    display = "in: " + item.name + " " + \
                        repr(item.percentage_achievement) + "%"
                else:
                    display = "in: " + item.name
            if isinstance(item, GuiOutputTransfer):
                # if role == QtCore.Qt.ForegroundRole:
                    # return QtGui.QBrush(BLUE)
                if item.transfer_status == constants.TRANSFERING_FROM_CLIENT_TO_CR or item.transfer_status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                    display = "out: " + item.name + " " + \
                        repr(item.percentage_achievement) + "%"
                else:
                    display = "out: " + item.name

            if not item.engine_path:
                if role == QtCore.Qt.DisplayRole:
                    return display
            else:
                status = item.transfer_status
                if role == QtCore.Qt.DisplayRole:
                    return display  # + " => " + status
                if status == constants.FILES_DO_NOT_EXIST:
                    if role == QtCore.Qt.DecorationRole:
                        return self.transfer_files_dont_exit
                if status == constants.FILES_ON_CLIENT:
                    if role == QtCore.Qt.DecorationRole:
                        return self.transfer_files_on_client
                if status == constants.FILES_ON_CR:
                    if role == QtCore.Qt.DecorationRole:
                        return self.transfer_files_on_cr
                if status == constants.FILES_ON_CLIENT_AND_CR:
                    if role == QtCore.Qt.DecorationRole:
                        return self.transfer_files_on_both
                if status == constants.TRANSFERING_FROM_CLIENT_TO_CR:
                    if role == QtCore.Qt.DecorationRole:
                        return self.transfering_from_client_to_cr
                if status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                    if role == QtCore.Qt.DecorationRole:
                        return self.transfering_from_cr_to_client
                if status == constants.FILES_UNDER_EDITION:
                    if role == QtCore.Qt.DecorationRole:
                        return self.files_under_edition

        if use_qvariant:
            return QtCore.QVariant()
        else:
            return None

#
# MODEL   ######################
#


class ComputingResourcePool(object):

    '''
    Holds the instances of soma_workflow.client.WorkflowController associated
    to a unique resource id.
    '''

    # dictonary: resource id -> soma_workflow.client.WorkflowController
    _connections = None

    # dictionary: resource id -> lock
    _connection_locks = None

    def __init__(self):
        self._connections = {}
        self._connection_locks = {}

    def __del__(self):
        self.delete_all()

    def add_default_connection(self):
        resource_id = socket.gethostname()
        if resource_id not in self._connections.keys():
            try:
                self.add_connection(resource_id, WorkflowController())
            except Exception:
                print('could not connect to the default connection %s'
                      % resource_id)

    def add_connection(self, resource_id, workflow_controller):
        self._connections[resource_id] = workflow_controller
        self._connection_locks[resource_id] = threading.RLock()

    def delete_connection(self, resource_id):
        print('ComputingResourcePool delete_connection:', resource_id)
        if resource_id in self._connections:
            print('del WFC')
            del self._connections[resource_id]
        if resource_id in self._connection_locks:
            del self._connection_locks[resource_id]

    def delete_all(self):
        resource_ids = list(six.iterkeys(self._connections))
        for resource_id in resource_ids:
            self.delete_connection(resource_id)
        try:
            import gc
        except ImportError:
            gc = None
        if gc:
            gc.collect()

    def reinit_connection(self, resource_id, workflow_controller):
        with self._connection_locks[resource_id]:
            self._connections[resource_id] = workflow_controller

    def connection(self, resource_id):
        return self._connections[resource_id]

    def lock(self, resource_id):
        return self._connection_locks[resource_id]

    def resource_exist(self, resource_id):
        return resource_id in self._connections.keys()

    def resource_ids(self):
        return list(six.iterkeys(self._connections))


class ApplicationModel(QtCore.QObject):

    '''
    Model for the application. This model was created to provide faster
    application minimizing communications with the server.
    The instances of this class hold the connections and the GuiWorkflow instances
    created in the current session of the application.
    The current workflow is periodically updated and the signal workflow_state_changed is
    emitted if necessary.
    '''

    # Computing resource pool
    resource_pool = None

    # gui_workflows
    # dictionary: resource_id => workflow_id => GuiWorkflow
    _workflows = None

    # dictionary: resource_id => workflow_ids => expiration_dates
    _expiration_dates = None

    # dictionary: resource_id => workflow_ids => workflow_names
    _workflow_names = None

    # dictionary: resource_id => workflow_ids => workflow_status
    _workflow_statuses = None

    current_connection = None

    current_resource_id = None

    _current_workflow = None

    current_wf_id = None

    workflow_exp_date = None

    workflow_name = None

    workflow_status = None

    tmp_stderrout_dir = None

    # dictionary: resource_id => boolean
    _hold = None

    _timer = None

    _timeout_duration = None

    # signals
    connection_closed_error = QtCore.Signal((), (str,))
    current_connection_changed = QtCore.Signal()
    workflow_state_changed = QtCore.Signal()
    current_workflow_about_to_change = QtCore.Signal()
    current_workflow_changed = QtCore.Signal()
    global_workflow_state_changed = QtCore.Signal()

    class UpdateThread(QtCore.QThread):

        def __init__(self, application_model, parent):
            super(ApplicationModel.UpdateThread, self).__init__(parent)
            self.application_model = application_model

        def run(self):
            self.application_model.update()

    def __init__(self, resource_pool=None, parent=None):
        '''
        **resource_pool**: *ComputingResourcePool*
        '''
        super(ApplicationModel, self).__init__(parent)

        home_dir = configuration.Configuration.get_home_dir()

        self.tmp_stderrout_dir = os.path.join(home_dir, ".soma-workflow")
        if not os.path.isdir(self.tmp_stderrout_dir) \
                and os.path.islink(self.tmp_stderrout_dir):
            # make ~/.soma-wodkflow as target of the dead link (if any)
            swf_dir = os.readlink(self.tmp_stderrout_dir)
            os.makedirs(swf_dir)
        if not os.path.isdir(self.tmp_stderrout_dir):
            os.makedirs(self.tmp_stderrout_dir)

        if resource_pool != None:
            self.resource_pool = resource_pool
        else:
            self.resource_pool = ComputingResourcePool()

        self._workflows = {}
        self._expiration_dates = {}
        self._workflow_names = {}
        self._workflow_statuses = {}

        self.current_connection = None
        self.current_resource_id = None

        self._current_workflow = None
        self.current_wf_id = None
        self.workflow_exp_date = None
        self.workflow_name = None
        self.workflow_status = None

        self._timeout_duration = {}
        self.update_interval = 3  # update period in seconds
        self.auto_update = True
        self._hold = {}

        self._lock = threading.RLock()

        for rid, connection_inst in six.iteritems(
                self.resource_pool._connections):
            self.add_connection(rid, connection_inst)

        self.update_thread = None

        self.global_workflow_state_changed.emit()
        self._timer = QtCore.QTimer(self)
        self._timer.setInterval(self.update_interval * 3000)
        self._timer.timeout.connect(self.threaded_update)
        self._timer.start()

        QtGui.QApplication.instance().lastWindowClosed.connect(
            self.wait_for_thread)

    @QtCore.Slot()
    def threaded_update(self):
        if self.update_thread != None:
                    # flush running update
            if not self.update_thread.isFinished():
                return  # still updating, we will do it again later
        if self.update_thread == None:
            self.update_thread = ApplicationModel.UpdateThread(
                application_model=self,
                parent=None)
        self.update_thread.start(QtCore.QThread.LowPriority)

    @QtCore.Slot()
    def wait_for_thread(self):
        if self.update_thread != None:
            if self.update_thread.isRunning():
                if not self.update_thread.wait(10000):
                    self.update_thread.terminate()
                    self.update_thread.wait()
            self.update_thread = None

    def update(self):
        with self._lock:
            if self.auto_update:
                if self.current_wf_id != None and not self._hold[self.current_resource_id]:
                    try:
                        if self.current_wf_id == NOT_SUBMITTED_WF_ID:
                            wf_status = None
                        elif self._current_workflow:
                            # print(" ==> communication with the server " + repr(self.wf_id))
                            # begining = datetime.now()

                            # wf_complete_status =
                            # self.current_connection.workflow_elements_status(self.current_wf_id)
                            wf_complete_status = self.connection_timeout(
                                WorkflowController.workflow_elements_status,
                                args=(
                                    self.current_connection, self.current_wf_id),
                                timeout_duration=self._timeout_duration[self.current_resource_id])
                            wf_status = wf_complete_status[2]
                            # end = datetime.now() - begining
                            # print(" <== end communication" + repr(self.wf_id)
                            # + " : " + repr(end.seconds))
                        else:
                            wf_status = self.connection_timeout(
                                WorkflowController.workflow_status,
                                args=(
                                    self.current_connection, self.current_wf_id),
                                timeout_duration=self._timeout_duration[self.current_resource_id])
                            # wf_status =
                            # self.current_connection.workflow_status(self.current_wf_id)
                    except ConnectionClosedError as e:
                        self.connection_closed_error[str].emit(
                            self.current_resource_id)
                        self._hold[self.current_resource_id] = True
                        return
                    except UnknownObjectError as e:
                        self.delete_workflow()
                        return
                    else:
                        if self._current_workflow and self.current_wf_id != NOT_SUBMITTED_WF_ID:
                            if self._current_workflow.updateState(
                                    wf_complete_status):
                                self.workflow_state_changed.emit()
                        if self.current_wf_id != NOT_SUBMITTED_WF_ID and self.workflow_status != wf_status:
                            self.workflow_status = wf_status
                            self._workflow_statuses[self.current_resource_id][
                                self.current_wf_id] = wf_status
                            self.workflow_state_changed.emit()
                            self.global_workflow_state_changed.emit()
                if True:
                    # update the status of every workflow
                    global_wf_state_changed = False
                    for rid in self.resource_pool.resource_ids():
                        if not self._hold[rid]:
                            for wfid in list(self._workflows[rid].keys()):
                                if wfid != self.current_wf_id:
                                    try:
                                        connection = self.resource_pool.connection(
                                            rid)
                                        wf_status = self.connection_timeout(
                                            WorkflowController.workflow_status,
                                            args=(
                                                connection, wfid),
                                            timeout_duration=self._timeout_duration[self.current_resource_id])

                                        # wf_status =
                                        # self.resource_pool.connection(rid).workflow_status(wfid)
                                    except ConnectionClosedError as e:
                                        self.connection_closed_error[str].emit(
                                            rid)
                                        self._hold[rid] = True
                                        break
                                    except UnknownObjectError as e:
                                        self.delete_workflow(wfid)
                                        continue
                                    else:
                                        if wf_status != self._workflow_statuses[rid][wfid]:
                                            global_wf_state_changed = True
                                            self._workflow_statuses[
                                                rid][wfid] = wf_status
                    if global_wf_state_changed:
                        self.global_workflow_state_changed.emit()

    def connection_timeout(self,
                           func,
                           args=(),
                           kwargs={},
                           timeout_duration=30,
                           default=None):
        """This function will spawn a thread and run the given function
        using the args, kwargs and return the given default value if the
        timeout_duration is exceeded.
        """
        class InterruptibleThread(QtCore.QThread):

            def __init__(self):
                super(InterruptibleThread, self).__init__(parent=None)
                self.result = default
                self.exception = None

            def run(self):
                try:
                    self.result = func(*args, **kwargs)
                except Exception as e:
                    self.exception = e
        it = InterruptibleThread()
        it.start()
        it.wait(msecs=timeout_duration * 1000)
        if it.isRunning():
            it.terminate()
            raise ConnectionClosedError("Connection time out")
        else:
            if it.exception != None:
                raise it.exception
            return it.result

    def list_workflow_names(self, resource_id):
        return list(six.itervalues(self._workflow_names[resource_id]))

    def list_workflow_status(self, resource_id):
        return list(six.itervalues(self._workflow_statuses[resource_id]))

    def get_workflow_status(self, resource_id, workflow_id):
        if workflow_id in self._workflow_statuses[resource_id]:
            return self._workflow_statuses[resource_id][workflow_id]
        else:
            return None

    def list_workflow_expiration_dates(self, resource_id):
        return list(six.itervalues(self._expiration_dates[resource_id]))

    def workflows(self, resource_id):
        result = {}
        for wf_id in self._workflows[resource_id].keys():
            result[wf_id] = (self._workflow_names[resource_id][wf_id],
                             self._expiration_dates[resource_id][wf_id])
        return result

    def add_connection(self, resource_id, connection):
        '''
        Adds a connection and use it as the current connection
        '''
        with self._lock:
            self.resource_pool.add_connection(resource_id, connection)
            self._workflows[resource_id] = {}
            self._expiration_dates[resource_id] = {}
            self._workflow_names[resource_id] = {}
            self._workflow_statuses[resource_id] = {}
            self.current_resource_id = resource_id
            self.current_connection = connection
            self._current_workflow = None
            self.current_wf_id = None
            self.workflow_exp_date = None
            self.workflow_status = None
            self.workflow_name = None
            if connection.config.get_mode() == configuration.REMOTE_MODE:
                self._timeout_duration[resource_id] = 40
            else:
                self._timeout_duration[resource_id] = 240
            self.current_connection_changed.emit()
            self.current_workflow_about_to_change.emit()
            self.current_workflow_changed.emit()
            self._hold[resource_id] = False

    def delete_connection(self, resource_id):
        '''
        Delete the connection.
        If the resource is the current connection:
        If any other connections exist the new current connection will be one of them.
        If not the current connection is set to None.
        '''
        with self._lock:
            #ref = self.resource_pool._connections[resource_id]
            self.resource_pool.delete_connection(resource_id)
            del self._workflows[resource_id]
            del self._expiration_dates[resource_id]
            del self._workflow_names[resource_id]
            del self._workflow_statuses[resource_id]
            del self._hold[resource_id]
            if resource_id == self.current_resource_id:
                self.current_resource_id = None
                self.current_connection = None
                self._current_workflow = None
                self.current_wf_id = None
                self.workflow_exp_date = None
                self.workflow_name = None
                resource_ids = self.resource_pool.resource_ids()
                if resource_ids:
                    self.current_resource_id \
                        = next(iter(self.resource_pool.resource_ids()))
                else:
                    self.current_resource_id = None
                if self.current_resource_id != None:
                    self.current_connection = self.resource_pool.connection(
                        self.current_resource_id)
                self.current_connection_changed.emit()
            self.global_workflow_state_changed.emit()

            #ref.disconnect() # should be done by WorkflowController.__del__
            #import objgraph
            #objgraph.show_backrefs(ref, refcounts=True, max_depth=10)

            # we need to use gc.collect() here to make sure the
            # WorkflowController actually gets destroyed. It seems that there
            # are some internal references (that I could not find) that prevent
            # automatic refcount deletion.
            import gc
            #print('ref:', gc.get_referrers(ref))
            #del ref
            gc.collect()

    def set_current_connection(self, resource_id):
        if resource_id != self.current_resource_id:
            with self._lock:
                print('resource_exist:', repr(resource_id), self.resource_pool.resource_exist(resource_id))
                assert(self.resource_pool.resource_exist(resource_id))
                self.current_resource_id = resource_id
                self.current_connection = self.resource_pool.connection(
                    resource_id)
                self.current_connection_changed.emit()

    def reinit_connection(self, resource_id, connection):
        with self._lock:
            self.current_resource_id = resource_id
            self.current_connection = connection
            self.resource_pool.reinit_connection(resource_id, connection)
            self.current_connection_changed.emit()
            self._hold[resource_id] = False

    def add_to_submitted_workflows(self,
                                   workflow_id,
                                   workflow_exp_date,
                                   workflow_name,
                                   workflow_status,
                                   workflow=None):
        '''
        Add a workflow without modifying the current workflow
        '''
        with self._lock:
            if workflow_id != NOT_SUBMITTED_WF_ID:
                if workflow:
                    self._workflows[self.current_resource_id][workflow_id] = GuiWorkflow(
                        workflow,
                        self.tmp_stderrout_dir)
                else:
                    self._workflows[self.current_resource_id][
                        workflow_id] = None
                self._expiration_dates[self.current_resource_id][
                    workflow_id] = workflow_exp_date
                self._workflow_names[self.current_resource_id][
                    workflow_id] = workflow_name
                self._workflow_statuses[self.current_resource_id][
                    workflow_id] = workflow_status
            self.global_workflow_state_changed.emit()

    def add_workflow(self,
                     workflow_id,
                     workflow_exp_date,
                     workflow_name,
                     workflow_status,
                     workflow=None):
        '''
        Build a GuiWorkflow from a soma_workflow.client.Worklfow and
        use it as the current workflow.
        '''
        with self._lock:
            self.current_workflow_about_to_change.emit()
            if workflow:
                self._current_workflow = GuiWorkflow(
                    workflow, self.tmp_stderrout_dir)
            else:
                self._current_workflow = None
            self.current_wf_id = workflow_id
            self.workflow_exp_date = workflow_exp_date
            self.workflow_name = workflow_name
            self.workflow_status = workflow_status
            if self.current_wf_id != NOT_SUBMITTED_WF_ID:

                self._workflows[self.current_resource_id][
                    workflow_id] = self._current_workflow
                self._expiration_dates[self.current_resource_id][
                    workflow_id] = self.workflow_exp_date
                self._workflow_names[self.current_resource_id][
                    workflow_id] = workflow_name
                self._workflow_statuses[self.current_resource_id][
                    workflow_id] = workflow_status
                if self._current_workflow != None:
                    try:
                        wf_status \
                            = self.current_connection.workflow_elements_status(
                                workflow_id)
                    except ConnectionClosedError as e:
                        self.connection_closed_error[()].emit()
                    else:
                        self._current_workflow.updateState(wf_status)
            self.current_workflow_changed.emit()
            self.global_workflow_state_changed.emit()

    def current_workflow(self):
        if self.current_wf_id == NOT_SUBMITTED_WF_ID or \
           self.current_wf_id == None or \
           self._current_workflow != None:
            return self._current_workflow

        with self._lock:
            try:
                workflow = self.current_connection.workflow(self.current_wf_id)
            except ConnectionClosedError as e:
                QtGui.QApplication.restoreOverrideCursor()
                self.connection_closed_error[()].emit()
            except UnknownObjectError as e:
                self.delete_workflow()
                return self._current_workflow
            else:
                try:
                    self._current_workflow = GuiWorkflow(
                        workflow, self.tmp_stderrout_dir)
                    self._workflows[self.current_resource_id][
                        self._current_workflow.wf_id] = self._current_workflow
                    wf_status \
                        = self.current_connection.workflow_elements_status(
                            self.current_wf_id)
                except ConnectionClosedError as e:
                    QtGui.QApplication.restoreOverrideCursor()
                    self.connection_closed_error[()].emit()
                except Exception:
                    import traceback
                    traceback.print_exc()
                    self._current_workflow = None
                else:
                    self._current_workflow.updateState(wf_status)

        return self._current_workflow

    def restart_current_workflow(self):
        self._current_workflow.restart()

    def delete_workflow(self, workflow_id=None):
        with self._lock:
            if workflow_id != None and (self._current_workflow == None or workflow_id != self._current_workflow.wf_id):
                del self._workflows[self.current_resource_id][workflow_id]
                del self._expiration_dates[
                    self.current_resource_id][workflow_id]
                del self._workflow_names[self.current_resource_id][workflow_id]
                del self._workflow_statuses[
                    self.current_resource_id][workflow_id]
            else:
                self.current_workflow_about_to_change.emit()
                if self._current_workflow and self._current_workflow.wf_id \
                        in self._workflows[self.current_resource_id].keys():
                    del self._workflows[self.current_resource_id][
                        self._current_workflow.wf_id]
                    del self._expiration_dates[self.current_resource_id][
                        self._current_workflow.wf_id]
                    del self._workflow_names[self.current_resource_id][
                        self._current_workflow.wf_id]
                    del self._workflow_statuses[self.current_resource_id][
                        self._current_workflow.wf_id]
                self._current_workflow = None
                self.current_wf_id = None
                self.workflow_exp_date = None  # datetime.now()
                self.workflow_status = None
                self.workflow_name = None
                self.current_workflow_changed.emit()
            self.global_workflow_state_changed.emit()

    def clear_current_workflow(self):
        with self._lock:
            if self._current_workflow != None or \
               self.current_wf_id != NOT_SUBMITTED_WF_ID:
                self.current_workflow_about_to_change.emit()
                self._current_workflow = None
                self.current_wf_id = None
                self.workflow_exp_date = None  # datetime.now()
                self.workflow_status = None
                self.workflow_name = None
                self.current_workflow_changed.emit()

    def set_current_workflow(self, wf_id):
        if wf_id != self.current_wf_id:
            with self._lock:
                assert(wf_id in self._workflows[
                       self.current_resource_id].keys())
                self.current_workflow_about_to_change.emit()
                self.current_wf_id = wf_id
                self._current_workflow = self._workflows[
                    self.current_resource_id][self.current_wf_id]
                self.workflow_exp_date = self._expiration_dates[
                    self.current_resource_id][self.current_wf_id]
                self.workflow_name = self._workflow_names[
                    self.current_resource_id][self.current_wf_id]
                self.workflow_status = self._workflow_statuses[
                    self.current_resource_id][self.current_wf_id]
                self.current_workflow_changed.emit()

    def set_no_current_workflow(self):
        with self._lock:
            self.current_workflow_about_to_change.emit()
            self._current_workflow = None
            self.current_wf_id = None
            self.workflow_exp_date = None  # datetime.now()
            self.workflow_status = None
            self.workflow_name = None
            self.current_workflow_changed.emit()

    def change_expiration_date(self, date):
        self.workflow_exp_date = date
        self._expiration_dates[self.current_resource_id][
            self._current_workflow.wf_id] = self.workflow_exp_date

    def is_loaded_workflow(self, wf_id):
        return wf_id in self._workflows[self.current_resource_id].keys()


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
    # dict: TemporaryPath id => sequence of gui item temporary id
    server_temporary = None
    # dict: Job id => gui item job id
    server_jobs = None

    queue = None

    def __init__(self, workflow, tmp_stderrout_dir):

        # print("wf " +repr(workflow))
        self.name = workflow.name

        if isinstance(workflow, EngineWorkflow):
            self.wf_id = workflow.wf_id
            self.queue = workflow.queue
        else:
            self.wf_id = NOT_SUBMITTED_WF_ID
            self.queue = None

        self.wf_status = None

        ids = {}  # workflow element => sequence of ids
        self.items = {}  # id => WorkflowItem
        self.root_item = None

        id_cnt = 0  # unique id for the items

        self.server_workflow = workflow
        self.server_jobs = {}
        self.server_file_transfers = {}
        self.server_temporary = {}

        # print(" ==> building the workflow ")
        # begining = datetime.now()
        # retrieving the set of job and the set of file transfers
        w_js = set()
        w_fts = set()

        for job in workflow.jobs:
            w_js.add(job)

        # Processing the Jobs to create the corresponding GuiJob instances
        for job in w_js:
            item_id = id_cnt
            id_cnt = id_cnt + 1
            if isinstance(workflow, EngineWorkflow):
                ejob = workflow.job_mapping[job]
                job_id = ejob.job_id
                command = ejob.plain_command()
            else:
                job_id = NOT_SUBMITTED_JOB_ID
                command = job.command

            gui_job = GuiJob(it_id=item_id,
                             command=command,
                             tmp_stderrout_dir=tmp_stderrout_dir,
                             parent=-1,
                             row=-1,
                             data=job,
                             children_nb=len(job.referenced_input_files) + len(
                                 job.referenced_output_files),
                             name=job.name,
                             job_id=job_id,
                             priority=job.priority,
                             parallel_job_info=job.parallel_job_info,
                             gui_workflow=self)
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
        # build maps to accelerate search
        rows = {data: row for row, data in enumerate(workflow.root_group)}
        g_rows = {}
        for group in workflow.groups:
            g_rows[group] = {data: row
                             for row, data in enumerate(group.elements)}
        for item in self.items.values():
            if isinstance(item, GuiGroup) or isinstance(item, GuiJob):
                row = rows.get(item.data)
                if row is not None:
                    item.parent = -1
                    item.row = row
                    self.root_item.children[item.row] = item.it_id
                for group in workflow.groups:
                    row = g_rows[group].get(item.data)
                    if row is not None:
                        item.parent = ids[group]
                        item.row = row
                        self.items[item.parent].children[item.row] = item.it_id

        # processing the file transfers
        def file_transfer_key(ft):
            if isinstance(ft, FileTransfer):
                return ft.name
            return ft

        for ft in w_fts:
            # print(" ft " + repr(ft))
            ids[ft] = []
            for job in w_js:
                ref_in = list(job.referenced_input_files)
                ref_in.sort(key=file_transfer_key)
                ref_out = list(job.referenced_output_files)
                ref_out.sort(key=file_transfer_key)
                if ft in ref_in:
                    item_id = id_cnt
                    id_cnt = id_cnt + 1
                    ids[ft].append(item_id)
                    row = ref_in.index(ft)
                    if isinstance(workflow, EngineWorkflow):
                        engine_id = workflow.transfer_mapping[ft].get_id()
                        engine_path = workflow.transfer_mapping[ft].engine_path
                    else:
                        engine_id = None
                        engine_path = None
                    gui_transfer = GuiInputTransfer(it_id=item_id,
                                                    parent=ids[job],
                                                    row=row,
                                                    data=ft,
                                                    name=ft.name,
                                                    engine_path=engine_path,
                                                    engine_id=engine_id)
                    self.items[item_id] = gui_transfer
                    if isinstance(ft, TemporaryPath):
                        self.server_temporary.setdefault(
                            gui_transfer.engine_id, []).append(item_id)
                    else:
                        self.server_file_transfers.setdefault(
                            gui_transfer.engine_id, []).append(item_id)
                    self.items[ids[job]].children[row] = item_id
                    # print(repr(job.name) + " " +
                    # repr(self.items[ids[job]].children))
                if ft in ref_out:
                    item_id = id_cnt
                    id_cnt = id_cnt + 1
                    ids[ft].append(item_id)
                    row = len(ref_in) + ref_out.index(ft)
                    if isinstance(workflow, EngineWorkflow):
                        engine_id = workflow.transfer_mapping[ft].get_id()
                        engine_path = workflow.transfer_mapping[ft].engine_path
                    else:
                        engine_id = None
                        engine_path = None
                    gui_ft = GuiOutputTransfer(it_id=item_id,
                                               parent=ids[job],
                                               row=row,
                                               data=ft,
                                               name=ft.name,
                                               engine_path=engine_path,
                                               engine_id=engine_id)
                    self.items[item_id] = gui_ft
                    if isinstance(ft, TemporaryPath):
                        self.server_temporary.setdefault(
                            gui_ft.engine_id, []).append(item_id)
                    else:
                        self.server_file_transfers.setdefault(
                            gui_ft.engine_id, []).append(item_id)

                    self.items[ids[job]].children[row] = item_id
                    # print(repr(job.name) + " " +
                    # repr(self.items[ids[job]].children))

        # for item in self.items.itervalues():
            # print(repr(item.children))

        # end = datetime.now() - begining
        # print(" <== end building workflow " + repr(end.seconds))

        # print model ####################
        # print("dependencies : " + repr(len(workflow.dependencies)))
        # if workflow.full_dependencies:
            # print("full_dependencies : " + repr(len(workflow.full_dependencies)))
        # for dep in workflow.dependencies:
            # print(dep[0].name + " -> " + dep[1].name)
        # for item in self.items.values():
            # print(repr(item.it_id) + " " + repr(item.parent) + " " + repr(item.row) + " " + repr(item.it_type) + " " + repr(item.name) + " " + repr(item.children))
        # raw_input()
        #

    def updateState(self, wf_status):
        if self.wf_id == NOT_SUBMITTED_WF_ID:
            return False
        data_changed = False

        self.queue = wf_status[3]

        if not wf_status:
            return False
        # updating jobs:
        for job_info in wf_status[0]:
            job_id, status, queue, exit_info, date_info, drmaa_id = job_info
            # date_info = (None, None, None) # (submission_date,
            # execution_date, ending_date)
            item = self.items[self.server_jobs[job_id]]
            data_changed = item.updateState(
                status, queue, exit_info, date_info, drmaa_id) or data_changed

        # end = datetime.now() - begining
        # print(" <== end updating jobs" + repr(self.wf_id) + " : " +
        # repr(end.seconds))

        # print(" ==> updating transfers " + repr(self.wf_id))
        # begining = datetime.now()

        # updating file transfer
        for transfer_info in wf_status[1]:
            transfer_id, complete_status = transfer_info
            for item_id in self.server_file_transfers[transfer_id]:
                item = self.items[item_id]
                data_changed = item.updateState(
                    complete_status) or data_changed

        # updating temp files
        for temp_info in wf_status[4]:
            temp_path_id, engine_file_path, status = temp_info
            complete_status = (status, None, engine_file_path, None, None)
            for item_id in self.server_temporary[temp_path_id]:
                item = self.items[item_id]
                data_changed = item.updateState(
                    complete_status) or data_changed

        # end = datetime.now() - begining
        # print(" <== end updating transfers" + repr(self.wf_id) + " : " +
        # repr(end.seconds) + " " + repr(data_changed))

        # updateing groups
        self.root_item.updateState()

        data_changed = data_changed or not self.wf_status == wf_status[2]
        self.wf_status = wf_status[2]

        return data_changed

    def restart(self):
        for item in six.itervalues(self.items):
            if isinstance(item, GuiJob):
                item.stdout = ""
                item.stderr = ""
                item.submission_date = None
                item.execution_date = None
                item.ending_date = None
                item.serial_duration = None


class GuiWorkflowItem(object):

    '''
    Abstract class for workflow items.
    '''

    def __init__(self,
                 it_id,
                 parent=-1,
                 row=-1,
                 data=None,
                 children_nb=0):

        self.it_id = it_id
        self.parent = parent  # parent_id
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
        self.pending = []
        self.queued = []
        self.warning = []

        self.job_count = 0

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
        self.pending = []
        self.queued = []
        self.warning = []

        no_status = False

        self.job_count = 0

        for child in self.children:
            item = self.gui_workflow.items[child]
            # TO DO : explore files
            if isinstance(item, GuiJob):
                self.job_count = self.job_count + 1
                if item.job_id == NOT_SUBMITTED_JOB_ID:
                    no_status = True
                    break
                if item.status == constants.WARNING:
                    self.warning.append(item)
                elif item.status == constants.NOT_SUBMITTED:
                    self.not_sub.append(item)
                elif item.status == constants.DONE or item.status == constants.FAILED:
                    exit_status, exit_value, term_signal, resource_usage = item.exit_info
                    if item.status == constants.DONE and exit_status == constants.FINISHED_REGULARLY and exit_value == 0:
                        self.done.append(item)
                    elif item.status == constants.DONE and exit_status == None:
                        self.running.append(item)
                    else:
                        self.failed.append(item)
                elif item.status == constants.SUBMISSION_PENDING:
                    self.pending.append(item)
                elif item.status == constants.QUEUED_ACTIVE:
                    self.queued.append(item)
                else:
                    self.running.append(item)
                if item.serial_duration:
                    self.theoretical_serial_time \
                        = self.theoretical_serial_time + item.serial_duration
                    if item.ending_date is not None \
                            and  item.ending_date > self.last_end_date:
                        self.last_end_date = item.ending_date
                if item.submission_date \
                        and item.submission_date < self.first_sub_date:
                    self.first_sub_date = item.submission_date

            if isinstance(item, GuiGroup):
                item.updateState()
                self.job_count = self.job_count + item.job_count
                self.not_sub.extend(item.not_sub)
                self.done.extend(item.done)
                self.failed.extend(item.failed)
                self.running.extend(item.running)
                self.pending.extend(item.pending)
                self.queued.extend(item.queued)
                self.warning.extend(item.warning)
                self.input_to_transfer.extend(item.input_to_transfer)
                self.input_transfer_ended.extend(item.input_transfer_ended)
                self.output_ready.extend(item.output_ready)
                self.output_transfer_ended.extend(item.output_transfer_ended)
                if item.first_sub_date and item.first_sub_date < self.first_sub_date:
                    self.first_sub_date = item.first_sub_date
                if item.last_end_date and item.last_end_date > self.last_end_date:
                    self.last_end_date = item.last_end_date
                self.theoretical_serial_time = self.theoretical_serial_time + \
                    item.theoretical_serial_time

        if no_status:
            new_status = GuiGroup.GP_NO_STATUS
        elif len(self.warning) > 0:
            new_status = GuiGroup.GP_WARNING
        elif len(self.failed) > 0:
            new_status = GuiGroup.GP_FAILED
        elif len(self.not_sub) == 0 and len(self.failed) == 0 and len(self.running) + len(self.pending) + len(self.queued) == 0:
            new_status = GuiGroup.GP_DONE
        elif len(self.running) + len(self.pending) + len(self.queued) == 0 and len(self.done) == 0 and len(self.failed) == 0:
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
                 tmp_stderrout_dir,
                 parent=-1,
                 row=-1,
                 it_type=None,
                 data=None,
                 children_nb=0,
                 name="no name",
                 job_id=NOT_SUBMITTED_JOB_ID,
                 priority=None,
                 parallel_job_info=None,
                 gui_workflow=None):
        super(GuiJob, self).__init__(it_id, parent, row, data, children_nb)

        self.status = "not submitted"
        self.exit_info = ("", "", "", "")
        self.stdout = ""
        self.stderr = ""
        self.submission_date = None
        self.execution_date = None
        self.ending_date = None
        self.priority = priority
        self.queue = None
        self.serial_duration = None

        self.name = name
        self.job_id = job_id
        self.drmaa_id = None

        self.tmp_stderrout_dir = tmp_stderrout_dir
        self.parallel_job_info = parallel_job_info
        if gui_workflow is not None:
            self.gui_workflow = weakref.ref(gui_workflow)
        else:
            self.gui_workflow = None

        cmd_seq = []
        for command_el in command:
            if isinstance(command_el, tuple) and isinstance(command_el[0], FileTransfer):
                cmd_seq.append(
                    "<FileTransfer " + command_el[0].client_path + " >")
            elif isinstance(command_el, FileTransfer):
                cmd_seq.append(
                    "<FileTransfer " + command_el.client_path + " >")
            elif isinstance(command_el, SharedResourcePath):
                cmd_seq.append("<SharedResourcePath " + command_el.namespace +
                               " " + command_el.uuid + " " + command_el.relative_path + " >")
            elif isinstance(command_el, TemporaryPath):
                cmd_seq.append("<TemporaryPath " + command_el.name + " >")
            elif isinstance(command_el, six.text_type):
                cmd_seq.append(command_el)
            elif isinstance(command_el, str):
                cmd_seq.append(command_el.decode('utf-8'))
            else:
                cmd_seq.append(repr(command_el))
        separator = u" "
        self.command = separator.join(cmd_seq)

    def updateState(self, status, queue, exit_info, date_info, drmaa_id):
        self.initiated = True
        state_changed = False
        state_changed = self.status != status or state_changed
        self.status = status
        state_changed = self.exit_info != exit_info or state_changed
        self.exit_info = exit_info
        self.submission_date = date_info[0]
        self.execution_date = date_info[1]
        self.ending_date = date_info[2]
        self.drmaa_id = drmaa_id
        state_changed = self.queue != queue or state_changed
        self.queue = queue
        if self.exit_info:
            exit_status, exit_value, term_signal, resource_usage = self.exit_info
            rud = {}
            if resource_usage:
                if six.PY3 and isinstance(resource_usage, bytes):
                    # in py3 RU is bytes, we want unicode/str
                    resource_usage = resource_usage.decode()
                ru = resource_usage.split()
                for ruel in ru:
                    ruel = ruel.split("=")
                    rud[ruel[0]] = ruel[1]
                    if ruel[0] == "start_time" and ruel[1] != "0":
                        t = time.localtime(float(ruel[1].replace(',', '.')))
                        self.execution_date = datetime(year=t[0], month=t[
                                                       1], day=t[2], hour=t[3], minute=t[4], second=t[5])
                    elif ruel[0] == "end_time" and ruel[1] != "0":
                        t = time.localtime(float(ruel[1].replace(',', '.')))
                        self.ending_date = datetime(year=t[0], month=t[
                                                    1], day=t[2], hour=t[3], minute=t[4], second=t[5])
                    elif ruel[0] == "submission_time" and ruel[1] != "0":
                        t = time.localtime(float(ruel[1].replace(',', '.')))
                        self.submission_date = datetime(year=t[0], month=t[
                                                        1], day=t[2], hour=t[3], minute=t[4], second=t[5])
            if self.ending_date:
                self.serial_duration \
                    = self.ending_date - self.execution_date
                if "cput" in rud:
                    tlist = [int(x) for x in rud["cput"].split(':')]
                    tlist = [0] * (6 - len(tlist)) + tlist + [0, 0, 0]
                    tlist[0] += 2000  # to avoid error about year range
                    t = time.struct_time(tlist)
                    t = datetime.fromtimestamp(time.mktime(t))
                    t0 = datetime.fromtimestamp(time.mktime(
                        time.struct_time([2000] + [0] * 8)))
                    #t = datetime.strptime(rud["cput"], "%H:%M:%S")
                    #t0 = datetime.strptime("00:00:00", "%H:%M:%S")
                    self.serial_duration = t - t0
                elif "cpupercent" in rud:
                    duration = self.serial_duration.total_seconds() \
                        * float(rud["cpupercent"]) / 100.
                    self.serial_duration = timedelta(seconds=duration)
                elif 'ncpus' in rud:
                    duration = self.serial_duration.total_seconds() \
                        * int(rud["ncpus"])
                    self.serial_duration = timedelta(seconds=duration)


        return state_changed

    def updateStdOutErr(self, connection):
        if self.data and self.job_id != NOT_SUBMITTED_JOB_ID:
            stdout_path = os.path.join(
                self.tmp_stderrout_dir, "tmp_stdout_file")
            stderr_path = os.path.join(
                self.tmp_stderrout_dir, "tmp_stderr_file")
            if os.path.exists(stdout_path):
                os.unlink(stdout_path)
            if os.path.exists(stderr_path):
                os.unlink(stderr_path)
            connection.retrieve_job_stdouterr(
                self.job_id, stdout_path, stderr_path)

            stdout = ""
            if os.path.exists(stdout_path):
                with open(stdout_path, "rt") as f:
                    line = f.readline()
                    while line:
                        stdout = stdout + line + "\n"
                        line = f.readline()
                os.chmod(stdout_path, 0o666)
            self.stdout = stdout

            stderr = ""
            if os.path.exists(stderr_path):
                with open(stderr_path, "rt") as f:
                    line = f.readline()
                    while line:
                        stderr = stderr + line + "\n"
                        line = f.readline()
                os.chmod(stderr_path, 0o666)
            self.stderr = stderr

    def update_job_command(self, connection):
        if self.job_id != NOT_SUBMITTED_JOB_ID:
            command = connection.get_job_command(self.job_id)
            self.command = command

    def update_job_params(self, connection):
        if self.job_id != NOT_SUBMITTED_JOB_ID:
            in_params = connection.updated_job_parameters(self.job_id)
            self.data.param_dict.update(in_params)
            if self.data.has_outputs:
                out_params = connection.get_job_output_params(self.job_id)
                self.output_params = out_params


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
                 engine_path=None,
                 engine_id=None):
        super(GuiTransfer, self).__init__(
              it_id, parent, row, data, children_nb)

        self.transfer_status = " "
        self.size = None
        self.transmitted = None
        self.elements_status = None

        self.percentage_achievement = 0
        self.transfer_type = None
        self.name = name
        self.engine_path = engine_path
        self.engine_id = engine_id

    def updateState(self, transfer_status_info):
        self.initiated = True
        state_changed = False
        transfer_status = transfer_status_info[0]
        engine_path = transfer_status_info[2]
        client_path = transfer_status_info[3]
        client_paths = transfer_status_info[4]

        if transfer_status_info[1]:
            if len(transfer_status_info[1]) == 2:
                self.transfer_type = GuiTransfer.FILE
                size, transmitted = transfer_status_info[1]
                elements_status = None
            elif len(transfer_status_info[1]) == 3:
                self.transfer_type = GuiTransfer.DIRECTORY
                (size, transmitted, elements_status) = transfer_status_info[1]
            if size != 0:
                self.percentage_achievement = int(
                    float(transmitted) / size * 100.0)
            else:
                self.percentage_achievement = 100
        else:
            (size, transmitted, elements_status) = (None, None, None)
            self.percentage_achievement = 0

        state_changed = state_changed or transfer_status != self.transfer_status
        state_changed = state_changed or size != self.size
        state_changed = state_changed or transmitted != self.transmitted
        state_changed = state_changed or elements_status != self.elements_status
        state_changed = state_changed or engine_path != self.engine_path
        if hasattr(self.data, 'client_path'):
            state_changed |= client_path != self.data.client_path
        if hasattr(self.data, 'client_paths'):
            state_changed |= client_paths != self.data.client_paths

        self.transfer_status = transfer_status
        self.size = size
        self.transmitted = transmitted
        self.elements_status = elements_status
        self.engine_path = engine_path
        if hasattr(self.data, 'client_path'):
            self.data.client_path = client_path
            self.data.client_paths = client_paths

        return state_changed


class GuiInputTransfer(GuiTransfer):

    def __init__(self,
                 it_id,
                 parent=-1,
                 row=-1,
                 data=None,
                 children_nb=0,
                 name="no name",
                 engine_path=None,
                 engine_id=None):
        super(GuiInputTransfer, self).__init__(it_id,
                                               parent,
                                               row,
                                               data,
                                               children_nb,
                                               name,
                                               engine_path,
                                               engine_id)


class GuiOutputTransfer(GuiTransfer):

    def __init__(self,
                 it_id,
                 parent=-1,
                 row=-1,
                 data=None,
                 children_nb=0,
                 name="no name",
                 engine_path=None,
                 engine_id=None):
        super(GuiOutputTransfer, self).__init__(it_id,
                                                parent,
                                                row,
                                                data,
                                                children_nb,
                                                name,
                                                engine_path,
                                                engine_id)
