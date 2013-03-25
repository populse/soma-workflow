
function( BRAINVISA_GENERATE_TARGET_NAME _variableName )
  if( DEFINED ${PROJECT_NAME}_TARGET_COUNT )
    math( EXPR ${PROJECT_NAME}_TARGET_COUNT ${${PROJECT_NAME}_TARGET_COUNT}+1 )
    set( ${PROJECT_NAME}_TARGET_COUNT ${${PROJECT_NAME}_TARGET_COUNT} CACHE INTERNAL "Used to generate new targets" )
  else( DEFINED ${PROJECT_NAME}_TARGET_COUNT )
    set( ${PROJECT_NAME}_TARGET_COUNT 1 CACHE INTERNAL "Used to generate new targets" )
  endif( DEFINED ${PROJECT_NAME}_TARGET_COUNT )
  set( ${_variableName} ${PROJECT_NAME}_target_${${PROJECT_NAME}_TARGET_COUNT} PARENT_SCOPE )
endfunction( BRAINVISA_GENERATE_TARGET_NAME )

# BRAINVISA_COPY_FILES
#
# Usage:
#  BRAINVISA_COPY_FILES( <component> <source files> [SOURCE_DIRECTORY <directory>] DESTINATION <destination directory>  [IMMEDIATE] [GET_TARGET <target variable>] [TARGET <target name>] [GET_OUTPUT_FILES <target variable>] [NO_SYMLINKS] )
#
function( BRAINVISA_COPY_FILES component )
  set( _files "${ARGN}" )
  
  # Read GET_OUTPUT_FILES option
  list( FIND _files GET_OUTPUT_FILES result )
  if( result EQUAL -1 )
    set( outputVariable )
  else()
    list( REMOVE_AT _files ${result} )
    list( GET _files ${result} outputVariable )
    list( REMOVE_AT _files ${result} )
  endif()

  # Read GET_TARGET option
  list( FIND _files GET_TARGET result )
  if( result EQUAL -1 )

    # Read TARGET option
    list( FIND _files TARGET result )
    if( result EQUAL -1 )
      set( targetName )
    else()
      list( REMOVE_AT _files ${result} )
      list( GET _files ${result} targetName )
      list( REMOVE_AT _files ${result} )
    endif()

    set( targetVariable )
  else()
    list( REMOVE_AT _files ${result} )
    list( GET _files ${result} targetVariable )
    list( REMOVE_AT _files ${result} )
  endif()

  # Read DESTINATION option
  list( FIND _files DESTINATION result )
  if( result EQUAL -1 )
    message( FATAL_ERROR "DESTINATION argument is mandatory for BRAINVISA_COPY_FILES" )
  else()
    list( REMOVE_AT _files ${result} )
    list( GET _files ${result} _destination )
    list( REMOVE_AT _files ${result} )
  endif()

  # Read SOURCE_DIRECTORY option
  list( FIND _files SOURCE_DIRECTORY result )
  if( result EQUAL -1 )
    set( _sourceDirectory )
  else()
    list( REMOVE_AT _files ${result} )
    list( GET _files ${result} _sourceDirectory )
    list( REMOVE_AT _files ${result} )
  endif()

  # Read IMMEDIATE option
  list( FIND _files IMMEDIATE result )
  if( result EQUAL -1 )
    set( immediate FALSE )
  else()
    set( immediate TRUE )
    list( REMOVE_AT _files ${result} )
  endif()

  # Read NO_SYMLINKS option
  list( FIND _files NO_SYMLINKS result )
  if( result EQUAL -1 )
    set( symlinks TRUE )
  else()
    set( symlinks FALSE )
    list( REMOVE_AT _files ${result} )
  endif()


#     message( "=== copy: from ${_sourceDirectory} to ${_destination} : ${_files}" )
  # Create a custom target for the files installation. The install-component target depends on this custom target
  BRAINVISA_GENERATE_TARGET_NAME( installTarget )

  add_custom_target( ${installTarget} )
  ##add_dependencies( install-${component} ${installTarget} )
  set( _allOutputFiles )
  set( _targetDepends  )
  foreach( _file ${_files} )
    if( IS_ABSOLUTE "${_file}" )
      set( _absoluteFile "${_file}"  )
      set( _path )
      get_filename_component( _file "${_file}" NAME )
    elseif( _sourceDirectory )
      set( _absoluteFile "${_sourceDirectory}/${_file}"  )
      get_filename_component( _path "${_file}" PATH )
    else()
      set( _absoluteFile "${CMAKE_CURRENT_SOURCE_DIR}/${_file}"  )
      get_filename_component( _path "${_file}" PATH )
    endif()
    if( EXISTS "${_absoluteFile}" )
      # do not copy a file on itself
      if(NOT "${_absoluteFile}" STREQUAL "${CMAKE_BINARY_DIR}/${_destination}/${_file}" )
        if( immediate )
          configure_file( "${_absoluteFile}"
                          "${CMAKE_BINARY_DIR}/${_destination}/${_file}"
                          COPYONLY )
        else()
          if( symlinks AND ( UNIX OR APPLE ) )
            # Make a symlink instead of copying Python source allows to
            # execute code from the build tree and directly benefit from
            # modifications in the source tree (without typing make)
            get_filename_component( _path_file "${_file}" PATH )
            add_custom_command(
              OUTPUT "${CMAKE_BINARY_DIR}/${_destination}/${_file}"
              COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/${_destination}/${_path_file}"
              COMMAND "${CMAKE_COMMAND}" -E create_symlink "${_absoluteFile}" "${CMAKE_BINARY_DIR}/${_destination}/${_file}" )
          else()
            add_custom_command(
              OUTPUT "${CMAKE_BINARY_DIR}/${_destination}/${_file}"
              DEPENDS "${_absoluteFile}"
              COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${_absoluteFile}" "${CMAKE_BINARY_DIR}/${_destination}/${_file}" )
          endif()
        endif()
      endif()
      if( outputVariable )
        list( APPEND _allOutputFiles  "${CMAKE_BINARY_DIR}/${_destination}/${_file}" )
      endif()

      # custom command attached to the custom target created for the files installation
      add_custom_command(TARGET ${installTarget} PRE_BUILD
          COMMAND if [ -n \"$(CMAKE_INSTALL_PREFIX)\" ]\;then "${CMAKE_COMMAND}" -E copy "${_absoluteFile}" "$(CMAKE_INSTALL_PREFIX)/${_destination}/${_file}"\;else "${CMAKE_COMMAND}" -E copy "${_absoluteFile}" "${CMAKE_INSTALL_PREFIX}/${_destination}/${_file}"\;fi )

      set( _targetDepends ${_targetDepends} "${CMAKE_BINARY_DIR}/${_destination}/${_file}" )
    else()
      message( "Warning: file \"${_absoluteFile}\" does not exists" )
    endif()
  endforeach()

  if (targetName)
    #message("Set target name : ${targetName}")
    set(target ${targetName})
  else()
    BRAINVISA_GENERATE_TARGET_NAME( target )
  endif()

  add_custom_target( ${target} ALL
                     DEPENDS ${_targetDepends} )
  if( targetVariable )
    set( ${targetVariable} "${target}" PARENT_SCOPE )
  endif()
  if( outputVariable )
    set( ${outputVariable} ${_allOutputFiles} PARENT_SCOPE )
  endif()
endfunction()


#
# Usage:
#   BRAINVISA_ADD_SIP_PYTHON_MODULE( <module> <directory> <mainSipFile> [ SIP_SOURCES <file> ... ] [ SIP_INCLUDE <directory> ... ] [ SIP_INSTALL <directory> ] )
#
#
macro( BRAINVISA_ADD_SIP_PYTHON_MODULE _moduleName _modulePath _mainSipFile )
  # Parse parameters
  set( _argn "${ARGN}" )
  list( FIND _argn SIP_INSTALL result )
  if( result EQUAL -1 )
    set( _SIP_INSTALL "share/${PROJECT_NAME}-${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}/sip" )
  else()
    list( REMOVE_AT _argn ${result} )
    list( GET _argn ${result} _SIP_INSTALL )
    list( REMOVE_AT _argn ${result} )
  endif()
  set( _SIP_SOURCES )
  set( _SIP_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}" )
  set( _listVariable )
  foreach( _i ${_argn} )
    if( "${_i}" STREQUAL "SIP_SOURCES" OR
        "${_i}" STREQUAL "SIP_INCLUDE" )
      set( _listVariable "_${_i}" )
    else( "${_i}" STREQUAL "SIP_SOURCES" OR
        "${_i}" STREQUAL "SIP_INCLUDE" )
      if( _listVariable )
        set( ${_listVariable} ${${_listVariable}} "${_i}" )
      else( _listVariable )
        message( FATAL_ERROR "Invalid option for BRAINVISA_ADD_SIP_PYTHON_MODULE: ${_i}" )
      endif( _listVariable )
    endif( "${_i}" STREQUAL "SIP_SOURCES" OR
        "${_i}" STREQUAL "SIP_INCLUDE" )
  endforeach()

  list( FIND _SIP_SOURCES "${_mainSipFile}" result )
  if( NOT result EQUAL -1 )
    list( REMOVE_AT _SIP_SOURCES "${result}" )
  endif()
  
  # Build install rules for sip files
  BRAINVISA_COPY_FILES( ${PROJECT_NAME}-dev
    ${_mainSipFile} ${_SIP_SOURCES}
    DESTINATION "${_SIP_INSTALL}"
    GET_OUTPUT_FILES copied_sip_files
    )

  # Compute C++ file names that will be generated by sip.
  # This is only possible with -j option.
  set( _sipSplitGeneratedCode 8 )
  set(_sipOutputFiles )
  foreach( _i RANGE 0 ${_sipSplitGeneratedCode} )
    if( ${_i} LESS ${_sipSplitGeneratedCode} )
      set(_sipOutputFiles ${_sipOutputFiles} "${CMAKE_CURRENT_BINARY_DIR}/sip${_moduleName}part${_i}.cpp" )
    endif( ${_i} LESS ${_sipSplitGeneratedCode} )
  endforeach( _i RANGE 0 ${_sipSplitGeneratedCode} )

 
  # Build include options according to _SIP_INCLUDE
  set( _sipIncludeOptions )
  set( _sipDeps )
  foreach( _i ${_SIP_INCLUDE} )
    set( _sipIncludeOptions ${_sipIncludeOptions} -I "${_i}" )
    file( GLOB _j "${_i}/*.sip" )
    list( APPEND _sipDeps ${_j} )
  endforeach( _i ${_SIP_INCLUDE} )
  # this will be many many dependencies,
  # but we cannot know the exact correct ones.
  list( REMOVE_DUPLICATES _sipDeps )

  # Add rule to generate C++ code with sip
  if( DESIRED_QT_VERSION EQUAL 3 )
    set( _sipFlags "-t" "ALL" "-t" "WS_X11" "-t" "Qt_3_3_0" )
  else()
    string( REPLACE " " ";" _sipFlags "${PYQT4_SIP_FLAGS}" )
  endif()
  set( _sipFlags ${SIP_FLAGS} ${_sipFlags} )

  if( ${SIP4MAKE_EXECUTABLE} STREQUAL ${SIP_EXECUTABLE} )
   # use regular sip
    add_custom_command(
      OUTPUT ${_sipOutputFiles}
      # Sip can generate less files than requested. The touch
      # command make sure that all the files are created (necessary)
      # for dependencies).
      COMMAND "${CMAKE_COMMAND}" -E remove ${_sipOutputFiles}
      COMMAND "${SIP4MAKE_EXECUTABLE}"
              -j ${_sipSplitGeneratedCode}
              ${_sipIncludeOptions}
              -c "${CMAKE_CURRENT_BINARY_DIR}"
              -e
              ${_sipFlags}
              -x VendorID -x Qt_STYLE_WINDOWSXP -x Qt_STYLE_INTERLACE
              ${_mainSipFile}
      COMMAND "${CMAKE_COMMAND}" -E touch ${_sipOutputFiles}
      DEPENDS ${copied_sip_files}
      DEPENDS ${_sipDeps}
    )
  else()
   # use bv_sip4make, taking care of creating the expected number of files
    add_custom_command(
      OUTPUT ${_sipOutputFiles}
      COMMAND "${SIP4MAKE_EXECUTABLE}"
              -j ${_sipSplitGeneratedCode}
              ${_sipIncludeOptions}
              -c "${CMAKE_CURRENT_BINARY_DIR}"
              -e
              ${_sipFlags}
              -x VendorID -x Qt_STYLE_WINDOWSXP -x Qt_STYLE_INTERLACE
              ${_mainSipFile}
      DEPENDS ${copied_sip_files}
      DEPENDS ${_sipDeps}
    )
  endif()

  # Create library with sip generated files
  add_library( ${_moduleName} MODULE ${_sipOutputFiles} )
  set_target_properties( ${_moduleName} PROPERTIES
                LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python/${_modulePath}"
                PREFIX "" )
  if( WIN32 )
    set_target_properties( ${_moduleName} PROPERTIES SUFFIX ".pyd" )
  endif( WIN32 )
  install ( TARGETS ${_moduleName} 
            DESTINATION "python/${_modulePath}"
           )
endmacro( BRAINVISA_ADD_SIP_PYTHON_MODULE _moduleName _modulePath _installComponent _installComponentDevel _sipSplitGeneratedCode _mainSipFile )

