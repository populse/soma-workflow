#BRAINVISA_COPY_FILES( ${PROJECT_NAME}-devel "${CMAKE_CURRENT_SOURCE_DIR}/jobs.sip"
#                      SOURCE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
#                      DESTINATION "share/soma-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sip" 
#                      IMMEDIATE )

BRAINVISA_ADD_SIP_PYTHON_MODULE( somadrmaajobssip 
  soma/pipeline
  "${CMAKE_CURRENT_SOURCE_DIR}/jobs.sip"
  SIP_INCLUDE  "${AIMS-FREE_SIP_DIRECTORY}"
  SIP_INSTALL "share/soma-${BRAINVISA_PACKAGE_VERSION_MAJOR}.${BRAINVISA_PACKAGE_VERSION_MINOR}/sip" )
target_link_libraries( somadrmaajobssip ${AIMS_PYAIMS_LIBRARIES} somadrmaajobs )

BRAINVISA_INSTALL( TARGETS somadrmaajobssip 
                   DESTINATION python/soma/pipeline
                   COMPONENT ${PROJECT_NAME} )
