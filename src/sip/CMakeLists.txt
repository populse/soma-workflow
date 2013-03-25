
#BRAINVISA_COPY_FILES( ${PROJECT_NAME}-dev "${CMAKE_CURRENT_SOURCE_DIR}/jobs.sip"
#                      SOURCE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
#                      DESTINATION "share/soma-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sip" 
#                      IMMEDIATE )

include_directories( ${SIP_INCLUDE_DIRS} )

include_directories( ${PROJECT_SOURCE_DIR}/src/drmaaJobs ) 

BRAINVISA_ADD_SIP_PYTHON_MODULE( somadrmaajobssip 
  soma/workflow
  "${CMAKE_CURRENT_SOURCE_DIR}/jobs.sip"
  SIP_INSTALL "share/soma-${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}/sip" )

target_link_libraries( somadrmaajobssip "${PYTHON_LIBRARY}" somadrmaajobs )

set_property( TARGET somadrmaajobssip PROPERTY SKIP_BUILD_RPATH TRUE )

install ( TARGETS somadrmaajobssip 
          DESTINATION python/soma/workflow
         )


#message ("CMAKE_CURRENT_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/configure.py")
#message ("${PYTHON_EXECUTABLE}")
#message ("echo hello > ${CMAKE_CURRENT_SOURCE_DIR}/log.txt")


#add_custom_command (
#  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/sipsomadrmaajobssipcmodule.cpp ${CMAKE_CURRENT_SOURCE_DIR}/jobs.sbf ${CMAKE_CURRENT_SOURCE_DIR}/sipAPIsomadrmaajobssip.h ${CMAKE_CURRENT_SOURCE_DIR}/sipsomadrmaajobssipDrmaaJobs.cpp ${CMAKE_CURRENT_SOURCE_DIR}/Makefile
#  COMMAND sip -c ${CMAKE_CURRENT_SOURCE_DIR} -b ${CMAKE_CURRENT_SOURCE_DIR}/jobs.sbf ${CMAKE_CURRENT_SOURCE_DIR}/jobs.sip
#  )

#add_custom_target(run ALL 
#DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/Makefile)

