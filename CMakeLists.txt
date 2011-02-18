cmake_minimum_required( VERSION 2.6 )
find_package( brainvisa-cmake REQUIRED )
BRAINVISA_PROJECT()

find_package( SIP )

BRAINVISA_COPY_PYTHON_DIRECTORY( "${CMAKE_CURRENT_SOURCE_DIR}/python"
                                 ${PROJECT_NAME} )

BRAINVISA_COPY_DIRECTORY( "${CMAKE_CURRENT_SOURCE_DIR}/bin"
                          bin
                          ${PROJECT_NAME} )

add_subdirectory( src/drmaaJobs )
if( SIP_FOUND )
  add_subdirectory( src/sip )
else()
  message( "WARNING: python wrapping of drmaajobs not built because sip has not been found" )
endif()

find_package( Sphinx )
if( SPHINX_FOUND )
  set( outputDirectory "share/doc/soma-workflow-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}" )
  add_custom_command( COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/${outputDirectory}"
                      COMMAND "${SPHINXBUILD_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/doc/source" "${CMAKE_BINARY_DIR}/${outputDirectory}"
                      OUTPUT "${CMAKE_BINARY_DIR}/${outputDirectory}/index.html"
                    )
  BRAINVISA_GENERATE_TARGET_NAME( target )
  add_custom_target( ${target} DEPENDS "${CMAKE_BINARY_DIR}/${outputDirectory}/index.html" )
  add_dependencies( soma-workflow-devdoc ${target} )
endif()

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
