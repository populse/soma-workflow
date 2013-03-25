cmake_minimum_required( VERSION 2.6 )


set (HostNameGabriel gabriel.intra.cea.fr)
set (MyHostName $ENV{HOSTNAME})

if(MyHostName STREQUAL HostNameGabriel)
  message ("Setup configuration on ${HostNameGabriel}")
  #Configuration for gabriel
  set ( PYTHON_EXECUTABLE /i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/bin/python) 
  set ( PYTHON_INCLUDE_PATH /i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/include/python2.7 )
  set ( PYTHON_LIBRARY /i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/libpython2.7.so )  
  set ( PYTHON_MODULES_PATH /i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/python2.7 ) 
  set ( PYTHON_SHORT_VERSION 2.7 ) 
  set ( PYTHON_VERSION 2.7.3 ) 
  set ( LIBDRMAA_LIBRARIES /i2bm/brainvisa/CentOS-5.3-x86_64/pbs_drmaa-1.0.13/lib/libdrmaa.so )
  #end Configuration for gabriel
endif()

set ( PROJECT_NAME soma-workflow )
set (${PROJECT_NAME}_VERSION_MAJOR 2)
set (${PROJECT_NAME}_VERSION_MINOR 5)
set (${PROJECT_NAME}_VERSION_PATCH 0)
set (${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}.${${PROJECT_NAME}_VERSION_PATCH} )

project (${PROJECT_NAME})

message ("PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE}")

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMakeModules)
find_package( Sphinx )
find_package( SIP )
find_package( LibDRMAA )

include (share_func.cmake)

#set ( CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR})

if( NOT LIBDRMAA_FOUND )
#  message ( "LIBDRMAA NOT FOUND" )
#  set( LIBDRMAA_LIBRARIES drmaa )
else()
  message ( "LIBDRMAA FOUND:${LIBDRMAA_LIBRARIES}" )
  add_subdirectory( src/drmaaJobs )
  if( SIP_FOUND )
    add_subdirectory( src/sip )
#  else()
#    message( "WARNING: python wrapping of drmaajobs not built because sip has not been found" )
  endif()
endif()

configure_file( "${CMAKE_CURRENT_SOURCE_DIR}/config/config.py.in" "${CMAKE_CURRENT_SOURCE_DIR}/python/soma/workflow/version.py" @ONLY )

install ( FILES "${CMAKE_CURRENT_SOURCE_DIR}/python/soma/workflow/version.py"
                   DESTINATION "python/soma/workflow"
                  )
