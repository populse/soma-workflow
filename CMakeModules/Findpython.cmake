# - Find python interpreter, libraries, includes and modules
# This module calls find_package( PythonInterp ) to find Python
# executable. Then it calls Python in order to get other information.
# The following variables are set:
#
#  PYTHON_FOUND - Was Python found
#  PYTHON_EXECUTABLE  - path to the Python interpreter
#  PYTHON_MODULES_PATH - path to main Python modules
#  PYTHON_INCLUDE_PATH - path to Python header files
#  PYTHON_LIBRARY - path to Python dynamic library
#  PYTHON_VERSION - Python full version (e.g. "2.6.2")
#  PYTHON_SHORT_VERSION - Python short version (e.g. "2.6")

if ( PYTHON_VERSION )
  # Python already found, do nothing
  set( PYTHON_FOUND TRUE )
else()
  find_package( PythonInterp REQUIRED )
  include( CMakeFindFrameworks )
  # Search for the python framework on Apple.
  cmake_find_frameworks( Python )
 
  execute_process( COMMAND "${PYTHON_EXECUTABLE}" "-c" "import sys, os; print os.path.normpath( sys.prefix )"
    OUTPUT_VARIABLE _prefix OUTPUT_STRIP_TRAILING_WHITESPACE )
  FILE( TO_CMAKE_PATH "${_prefix}" _prefix )
  execute_process( COMMAND "${PYTHON_EXECUTABLE}" "-c" "import sys; print \".\".join( (str(i) for i in sys.version_info[ :2 ]) )"
    OUTPUT_VARIABLE _version OUTPUT_STRIP_TRAILING_WHITESPACE )
  execute_process( COMMAND "${PYTHON_EXECUTABLE}" "-c" "import sys; print \".\".join( (str(i) for i in sys.version_info[ :3 ]) )"
    OUTPUT_VARIABLE _fullVersion OUTPUT_STRIP_TRAILING_WHITESPACE )
  string(REPLACE "." "" _versionNoDot ${_version} )
  message( STATUS "Using python ${_fullVersion}: ${PYTHON_EXECUTABLE}" )
  
  set( PYTHON_VERSION "${_fullVersion}" CACHE STRING "Python full version (e.g. \"2.6.2\")" )
  set( PYTHON_SHORT_VERSION "${_version}" CACHE STRING "Python short version (e.g. \"2.6\")" )
  
  set( PYTHON_FRAMEWORK_INCLUDES )
  set( PYTHON_FRAMEWORK_LIBRARIES )
  if( Python_FRAMEWORKS AND NOT PYTHON_INCLUDE_PATH )
    foreach( _dir ${Python_FRAMEWORKS} )
      set( PYTHON_FRAMEWORK_INCLUDES ${PYTHON_FRAMEWORK_INCLUDES}
          "${_dir}/Versions/${_version}/include/python${_version}" )
      set( PYTHON_FRAMEWORK_LIBRARIES ${PYTHON_FRAMEWORK_LIBRARIES}
          "${_dir}/Versions/${_version}/lib" )
    endforeach()
  endif()
  find_path( PYTHON_INCLUDE_PATH
    NAMES Python.h
    PATHS
      "${_prefix}/include"
    PATH_SUFFIXES
      python${_version}
    NO_DEFAULT_PATH
  )
  find_path( PYTHON_INCLUDE_PATH
    NAMES Python.h
    PATHS
      ${PYTHON_FRAMEWORK_INCLUDES}
      [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_version}\\InstallPath]/include
    PATH_SUFFIXES
      python${_version}
  )
  mark_as_advanced( PYTHON_INCLUDE_PATH )
  
  find_path( PYTHON_MODULES_PATH
    NAMES os.py
    PATHS
      "${_prefix}/lib"
      ${PYTHON_FRAMEWORK_INCLUDES}
      [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_version}\\InstallPath]/include
    PATH_SUFFIXES
      python${_version}
  )
  mark_as_advanced( PYTHON_MODULES_PATH )
  
  find_library( PYTHON_LIBRARY
    NAMES python${_versionNoDot} python${_version} python
    PATHS
      "${_prefix}/lib"
      ${PYTHON_FRAMEWORK_LIBRARIES}
      [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_version}\\InstallPath]/libs
  #  PATH_SUFFIXES
  #    python${_version}/config
  #  # Avoid finding the .dll in the PATH.  We want the .lib.
  #  NO_SYSTEM_ENVIRONMENT_PATH
  )
  mark_as_advanced( PYTHON_LIBRARY )
  
  # handle the QUIETLY and REQUIRED arguments and set PYTHONINTERP_FOUND to TRUE if
  # all listed variables are TRUE
  INCLUDE(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(python DEFAULT_MSG PYTHON_INCLUDE_PATH PYTHON_INCLUDE_PATH PYTHON_LIBRARY)
endif()
