# This file defines the following variables:
#
# SPHINXBUILD_EXECUTABLE - Path and filename of the sphinx-build command line executable.
#
# SPHINX_VERSION - The version of sphinx found expressed as a 6 digit hex number
#     suitable for comparision as a string.
#

if( SPHINX_VERSION )
  # Sphinx is already found, do nothing
  set(SPHINX_FOUND TRUE)
else()
  find_program( SPHINXBUILD_EXECUTABLE
    NAMES sphinx-build
    DOC "Path to sphinx-build executable" )

  find_package( python REQUIRED )

  mark_as_advanced( SPHINXBUILD_EXECUTABLE )
  if(SPHINXBUILD_EXECUTABLE)
    execute_process( COMMAND ${PYTHON_EXECUTABLE} -c "import sphinx; ver = [ int(x) for x in sphinx.__version__.split( '.' ) ]; print '%x' % ( ver[0] * 0x10000 + ver[1] * 0x100 + ver[2] )"
    OUTPUT_VARIABLE SPHINX_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE )
    set( SPHINX_VERSION "${SPHINX_VERSION}" CACHE STRING "Version of sphinx module" )
    mark_as_advanced( SPHINX_VERSION )
  endif()
  
  if(SPHINXBUILD_EXECUTABLE AND SPHINX_VERSION)
    set( SPHINX_FOUND TRUE )
  else()
    set(SPHINX_FOUND FALSE)
    if( SPHINX_FIND_REQUIRED )
        message( SEND_ERROR "Sphinx was not found." )
    else()
      if(NOT SPHINX_FIND_QUIETLY)
        message(STATUS "Sphinx was not found.")
      endif()
    endif()

  endif()
endif()

