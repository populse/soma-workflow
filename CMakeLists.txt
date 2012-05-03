cmake_minimum_required( VERSION 2.6 )
find_package( brainvisa-cmake REQUIRED )
BRAINVISA_PROJECT()

find_package( SIP )

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
else()
  add_subdirectory( src/drmaaJobs )
  if( SIP_FOUND )
    add_subdirectory( src/sip )
  else()
    message( "WARNING: python wrapping of drmaajobs not built because sip has not been found" )
  endif()
endif()

# PySide-compatible .ui -> .py generation
find_program( PYUIC "pyside-uic" )
if( NOT PYUIC )
  find_program( PYUIC "pyuic4" )
endif()
if( NOT PYUIC )
  find_program( PYUIC "pyuic" )
endif()
if( PYUIC )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_job_info.py JobInfo.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/JobInfo.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_graph_widget.py graphWidget.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/graphWidget.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_plot_widget.py PlotWidget.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/PlotWidget.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_transfer_info.py TransferInfo.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/TransferInfo.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_group_info.py GroupInfo.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/GroupInfo.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_connection_dlg.py connection_dlg.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/connection_dlg.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_workflow_example_dlg.py workflowExampleDlg.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/workflowExampleDlg.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_submission_dlg.py submissionDlg.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/submissionDlg.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_resource_wf_select.py resource_wf_select.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/resource_wf_select.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_main_window.py main_window.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/main_window.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_wf_status_name_date.py wf_status_name_date.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/wf_status_name_date.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_sw_mini_widget.py sw_mini.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/sw_mini.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_search_widget.py search_widget.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/search_widget.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_local_scheduler_cfg_ctrl.py local_scheduler_widget.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/local_scheduler_widget.ui"
  )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} ALL
    ${PYUIC} -o ui_workflow_engine_cfg_ctrl.py engine_controller_widget.ui
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/python/soma/workflow/gui"
    DEPENDS "${CMAKE_BINARY_DIR}/python/soma/workflow/gui/engine_controller_widget.ui"
  )

endif()

configure_file( "${CMAKE_CURRENT_SOURCE_DIR}/config/config.py.in" "${CMAKE_BINARY_DIR}/python/soma/workflow/version.py" @ONLY )
BRAINVISA_INSTALL( FILES "${CMAKE_BINARY_DIR}/python/soma/workflow/version.py"
                   DESTINATION "python/soma/workflow"
                   COMPONENT ${PROJECT_NAME} )



find_package( Sphinx )

BRAINVISA_GENERATE_SPHINX_DOC( "doc/source"
  "share/doc/soma-workflow-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sphinx" )

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
