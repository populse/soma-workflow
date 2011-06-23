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
  
add_subdirectory( src/drmaaJobs )
if( SIP_FOUND )
  add_subdirectory( src/sip )
else()
  message( "WARNING: python wrapping of drmaajobs not built because sip has not been found" )
endif()

find_package( Sphinx )

BRAINVISA_GENERATE_SPHINX_DOC( "doc/source"
  "share/doc/soma-workflow-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sphinx" )

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
