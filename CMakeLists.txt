cmake_minimum_required( VERSION 2.6 )
find_package( brainvisa-cmake REQUIRED )
BRAINVISA_PROJECT()

BRAINVISA_COPY_PYTHON_DIRECTORY( "${CMAKE_CURRENT_SOURCE_DIR}/python"
                                 ${PROJECT_NAME} )
add_subdirectory( src/drmaaJobs )
BRAINVISA_FIND_PACKAGE( aims-free )
if( AIMS-FREE_FOUND )
  include_directories( "${SIP_INCLUDE_DIR}" )
  add_subdirectory( src/sip )
else()
  message( "WARNING: sip wrapping of soma-pipeline not built because aims-free has not been found" )
endif()

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
