file( GLOB headers RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.h" )
file( GLOB sources RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc" )


set( headers ${headers} drmaa/drmaa.h )
BRAINVISA_COPY_AND_INSTALL_HEADERS( headers soma/workflow headersTarget )

add_library( drmaa SHARED drmaa/drmaa_empty.c )
set_property( TARGET drmaa PROPERTY LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}" )
add_dependencies( drmaa ${headersTarget} )

add_library( somadrmaajobs SHARED ${sources} )
add_dependencies( somadrmaajobs ${headersTarget} )
set_property( TARGET somadrmaajobs PROPERTY VERSION ${${PROJECT_NAME}_VERSION} )
set_property( TARGET somadrmaajobs PROPERTY SKIP_BUILD_RPATH TRUE )
#target_link_libraries( somadrmaajobs "${CMAKE_CURRENT_SOURCE_DIR}/drmaa/libcondordrmaa.so" ) #condor
#target_link_libraries( somadrmaajobs "${CMAKE_CURRENT_SOURCE_DIR}/drmaa/libdrmaa.so" ) #SGE
target_link_libraries( somadrmaajobs drmaa ) # built-in

BRAINVISA_INSTALL( TARGETS somadrmaajobs
                   DESTINATION lib
                   COMPONENT ${PROJECT_NAME} )
