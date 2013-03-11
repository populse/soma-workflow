cmake_minimum_required( VERSION 2.6 )

set ( PROJECT_NAME soma-workflow )
set (${PROJECT_NAME}_VERSION_MAJOR 2)
set (${PROJECT_NAME}_VERSION_MINOR 5)
set (${PROJECT_NAME}_VERSION_PATCH 0)
set (${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}.${${PROJECT_NAME}_VERSION_PATCH} )

project (${PROJECT_NAME})


set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMakeModules)
find_package( Sphinx )
find_package( SIP )
find_package( LibDRMAA QUIET )

include (share_func.cmake)

set ( CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR})


if( NOT LIBDRMAA_FOUND )
  message ( "LIBDRMAA NOT FOUND" )
#  set( LIBDRMAA_LIBRARIES drmaa )
else()
  message ( "LIBDRMAA FOUND" )
  add_subdirectory( src/drmaaJobs )
  if( SIP_FOUND )
    add_subdirectory( src/sip )
#  else()
#    message( "WARNING: python wrapping of drmaajobs not built because sip has not been found" )
  endif()
endif()

configure_file( "${CMAKE_CURRENT_SOURCE_DIR}/config/config.py.in" "${CMAKE_BINARY_DIR}/python/soma/workflow/version.py" @ONLY )

install ( FILES "${CMAKE_BINARY_DIR}/python/soma/workflow/version.py"
                   DESTINATION "python/soma/workflow"
                  )
