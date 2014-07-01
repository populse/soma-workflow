cmake_minimum_required( VERSION 2.6 )
find_package( brainvisa-cmake REQUIRED )
BRAINVISA_PROJECT()

BRAINVISA_COPY_PYTHON_DIRECTORY( "${CMAKE_CURRENT_SOURCE_DIR}/python"
                                 ${PROJECT_NAME} )

BRAINVISA_COPY_DIRECTORY( "${CMAKE_CURRENT_SOURCE_DIR}/bin"
                          bin
                          ${PROJECT_NAME} )

BRAINVISA_DEPENDENCY( RUN DEPENDS python RUN ">= 2.5;<< 3.0" )
BRAINVISA_DEPENDENCY( RUN DEPENDS libqtcore4 RUN ">= 4.6" )
BRAINVISA_DEPENDENCY( RUN DEPENDS libqtgui4 RUN ">= 4.6" )
BRAINVISA_DEPENDENCY( RUN RECOMMENDS libqt4-designer RUN ">= 4.6" )

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

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
