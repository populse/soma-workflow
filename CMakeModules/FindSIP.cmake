# This file defines the following variables:
#
# SIP_EXECUTABLE - Path and filename of the SIP command line executable.
#
# SIP_INCLUDE_DIR - Directory holding the SIP C++ header file.
#
# SIP_INCLUDE_DIRS - All include directories necessary to compile sip generated files.
#
# SIP_VERSION - The version of SIP found expressed as a 6 digit hex number
#     suitable for comparision as a string.
#

# message( "SIP_VERSION: ${SIP_VERSION}" )
if( SIP_VERSION )
  # SIP is already found, do nothing
  set( SIP_INCLUDE_DIRS "${PYTHON_INCLUDE_PATH}" "${SIP_INCLUDE_DIR}" )
  set(SIP_FOUND TRUE)
else( SIP_VERSION )
  find_program( SIP_EXECUTABLE
    NAMES sip
    DOC "Path to sip executable" )
  
  if( SIP_EXECUTABLE )
    find_package( python REQUIRED )
    
    mark_as_advanced( SIP_EXECUTABLE )
    execute_process( COMMAND ${SIP_EXECUTABLE} -V OUTPUT_VARIABLE SIP_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE )
    set( SIP_VERSION "${SIP_VERSION}" CACHE STRING "Version of sip executable" )
    mark_as_advanced( SIP_VERSION )
    execute_process( COMMAND ${PYTHON_EXECUTABLE} -c "import sipconfig; print sipconfig.Configuration().sip_inc_dir"
      OUTPUT_VARIABLE SIP_INCLUDE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE )
    set( SIP_INCLUDE_DIR "${SIP_INCLUDE_DIR}" CACHE PATH "Path to sip include files" )
    mark_as_advanced( SIP_INCLUDE_DIR )
    set( SIP_INCLUDE_DIRS "${PYTHON_INCLUDE_PATH}" "${SIP_INCLUDE_DIR}" )
    set( SIP_FOUND TRUE )
    if( NOT SIP_FIND_QUIETLY )
      message( STATUS "Found SIP version: ${SIP_VERSION}" )
    endif(NOT SIP_FIND_QUIETLY)
  else( SIP_EXECUTABLE )
    set( SIP_FOUND FALSE )
    if( SIP_FIND_REQUIRED )
      message( FATAL_ERROR "SIP not found" )
    endif( SIP_FIND_REQUIRED )
  endif( SIP_EXECUTABLE )
endif( SIP_VERSION )

if( SIP_FOUND )
  if( ( ${SIP_VERSION} VERSION_GREATER 4.7.4 ) AND
      ( ${SIP_VERSION} VERSION_LESS 4.7.7 ) )
    # this flag is used in pyaims/pyanatomist to work around buggy throw
    # statements which make sip segfault
    set( SIP_FLAGS "-t" "SIPTHROW_BUG" CACHE STRING "options passed to SIP program" )
  else()
    set( SIP_FLAGS "" CACHE STRING "options passed to SIP program" )
  endif()
  if( NOT SIP4MAKE_EXECUTABLE )
    # find the sip4make.py wrapper script
    find_program( SIP4MAKE_EXECUTABLE
      NAMES bv_sip4make
      DOC "Path to bv_sip4make script" )
    if( NOT SIP4MAKE_EXECUTABLE )
      # not found: use the regular sip executable
      set( SIP4MAKE_EXECUTABLE "$SIP_EXECUTABLE" CACHE FILEPATH "Path to bv_sip4make script (or sip itself as fallback)" )
    endif()
  endif()
endif()

