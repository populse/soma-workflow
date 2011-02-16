#BRAINVISA_COPY_FILES( ${PROJECT_NAME}-dev "${CMAKE_CURRENT_SOURCE_DIR}/jobs.sip"
#                      SOURCE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
#                      DESTINATION "share/soma-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sip" 
#                      IMMEDIATE )

include_directories( ${SIP_INCLUDE_DIRS} )
BRAINVISA_ADD_SIP_PYTHON_MODULE( somadrmaajobssip 
  soma/workflow
  "${CMAKE_CURRENT_SOURCE_DIR}/jobs.sip"
  SIP_INSTALL "share/soma-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sip" )
target_link_libraries( somadrmaajobssip "${PYTHON_LIBRARY}" somadrmaajobs )

BRAINVISA_INSTALL( TARGETS somadrmaajobssip 
                   DESTINATION python/soma/workflow
                   COMPONENT ${PROJECT_NAME} )
