cmake_minimum_required( VERSION 3.10 )
find_package( brainvisa-cmake REQUIRED )
BRAINVISA_PROJECT()
enable_testing()

BRAINVISA_COPY_PYTHON_DIRECTORY( "${CMAKE_CURRENT_SOURCE_DIR}/python"
                                 ${PROJECT_NAME} )

BRAINVISA_COPY_DIRECTORY( "${CMAKE_CURRENT_SOURCE_DIR}/bin"
                          bin
                          ${PROJECT_NAME} )

BRAINVISA_DEPENDENCY( RUN DEPENDS python RUN ">= 2.7; << 4.0" )
if( DESIRED_QT_VERSION EQUAL 4 )
  BRAINVISA_DEPENDENCY( RUN RECOMMENDS python-qt4 RUN )
else()
  BRAINVISA_DEPENDENCY( RUN RECOMMENDS python-qt5 RUN )
endif()
BRAINVISA_DEPENDENCY( RUN RECOMMENDS graphviz RUN )

find_package( LibDRMAA QUIET )
if( NOT LIBDRMAA_FOUND )
  set( LIBDRMAA_LIBRARIES drmaa )
endif()

# PySide-compatible .ui -> .py generation
find_program( PYUIC "pyside-uic" )

if( PYUIC )
  BRAINVISA_PYUIC( JobInfo.ui ui_job_info.py python/soma_workflow/gui )
  BRAINVISA_PYUIC( graphWidget.ui ui_graph_widget.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( PlotWidget.ui ui_plot_widget.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( TransferInfo.ui ui_transfer_info.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( GroupInfo.ui ui_group_info.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( connection_dlg.ui ui_connection_dlg.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( workflowExampleDlg.ui ui_workflow_example_dlg.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( submissionDlg.ui ui_submission_dlg.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( resource_wf_select.ui ui_resource_wf_select.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( main_window.ui ui_main_window.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( wf_status_name_date.ui ui_wf_status_name_date.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( sw_mini.ui ui_sw_mini_widget.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( search_widget.ui ui_search_widget.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( local_scheduler_widget.ui ui_local_scheduler_cfg_ctrl.py python/soma_workflow/gui ) 
  BRAINVISA_PYUIC( engine_controller_widget.ui ui_workflow_engine_cfg_ctrl.py python/soma_workflow/gui ) 
endif()

find_package( Sphinx )

BRAINVISA_GENERATE_SPHINX_DOC( "doc/source"
  "share/doc/soma-workflow-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sphinx" )

brainvisa_add_test(soma-workflow-api "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.job_tests.test_workflow_api --isolated)
brainvisa_add_test(soma-workflow-exception1 "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_exception1 --isolated)
brainvisa_add_test(soma-workflow-exception2 "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_exception2 --isolated)
brainvisa_add_test(soma-workflow-fake_pipeline "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_fake_pipeline --isolated)
brainvisa_add_test(soma-workflow-multiple "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_multiple --isolated)
brainvisa_add_test(soma-workflow-native_spec "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_native_spec --isolated)
brainvisa_add_test(soma-workflow-njobs "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_njobs --isolated)
brainvisa_add_test(soma-workflow-njobs_with_dependencies "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_njobs_with_dependencies --isolated)
brainvisa_add_test(soma-workflow-serial_jobs "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_serial_jobs --isolated)
brainvisa_add_test(soma-workflow-simple "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_simple --isolated)
brainvisa_add_test(soma-workflow-special_command "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_special_command --isolated)
brainvisa_add_test(soma-workflow-special_transfer "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_special_transfer --isolated)
brainvisa_add_test(soma-workflow-wrong_native_spec "${PYTHON_EXECUTABLE_NAME}" -m soma_workflow.test.workflow_tests.test_wrong_native_spec --isolated)

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
