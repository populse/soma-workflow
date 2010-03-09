file( GLOB headers RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.h" )
file( GLOB sources RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc" )

set( headers ${headers} drmaa/drmaa.h )

BRAINVISA_COPY_AND_INSTALL_HEADERS( headers soma/pipeline headersTarget )
add_library( somadrmaajobs SHARED ${sources} )
add_dependencies( somadrmaajobs ${headersTarget} )
set_property( TARGET somadrmaajobs PROPERTY VERSION ${${PROJECT_NAME}_VERSION} )
#target_link_libraries( somadrmaajobs "${CMAKE_CURRENT_SOURCE_DIR}/drmaa/libcondordrmaa.a" ) #condor
target_link_libraries( somadrmaajobs "${CMAKE_CURRENT_SOURCE_DIR}/drmaa/libdrmaa.so" ) #SGE

BRAINVISA_INSTALL( TARGETS somadrmaajobs
                   DESTINATION lib
                   COMPONENT ${PROJECT_NAME} )
