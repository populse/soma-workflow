file( GLOB headers RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.h" )
file( GLOB sources RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc" )

set( headers ${headers} drmaa/drmaa.h )

BRAINVISA_COPY_AND_INSTALL_HEADERS( headers soma/pipeline headersTarget )
add_library( somajobs SHARED ${sources} )
add_dependencies( somajobs ${headersTarget} )
set_property( TARGET somajobs PROPERTY VERSION ${${PROJECT_NAME}_VERSION} )
target_link_libraries( somajobs "${CMAKE_CURRENT_SOURCE_DIR}/drmaa/libcondordrmaa.a" )
BRAINVISA_INSTALL( TARGETS somajobs
                   DESTINATION lib
                   COMPONENT ${PROJECT_NAME} )
