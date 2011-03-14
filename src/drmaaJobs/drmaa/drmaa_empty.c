#include <soma/workflow/drmaa/drmaa.h>
#include <stdio.h>

#ifdef  __cplusplus
extern "C" {
#endif

int drmaa_get_next_attr_name(drmaa_attr_names_t* values, 
                             char *value,
                             size_t value_len) { 
  return DRMAA_NO_ERRNO; 
}

int drmaa_get_next_attr_value(drmaa_attr_values_t* values, 
                              char *value,
                              size_t value_len) { 
  return DRMAA_NO_ERRNO; 
}  
  
int drmaa_get_next_job_id(drmaa_job_ids_t* values, 
                          char *value,
                          size_t value_len) { 
  return DRMAA_NO_ERRNO; 
}

int drmaa_get_num_attr_names(drmaa_attr_names_t* values, 
                             int *size ) { 
  return DRMAA_NO_ERRNO; 
}

int drmaa_get_num_attr_values(drmaa_attr_values_t* values, 
                              int *size) { 
  return DRMAA_NO_ERRNO; 
}

int drmaa_get_num_job_ids(drmaa_job_ids_t* values, 
                           int *size) { 
  return DRMAA_NO_ERRNO; 
}

void drmaa_release_attr_names(drmaa_attr_names_t* values) {
}

void drmaa_release_attr_values(drmaa_attr_values_t* values) {
}

void drmaa_release_job_ids(drmaa_job_ids_t* values) {
}

int drmaa_init(const char *contact, 
               char *error_diagnosis,
               size_t error_diag_len) {
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_exit(char *error_diagnosis, 
               size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_allocate_job_template(drmaa_job_template_t **jt,
                                char *error_diagnosis, 
                                size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_delete_job_template(drmaa_job_template_t *jt, 
                              char *error_diagnosis,
                              size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_set_attribute(drmaa_job_template_t *jt, 
                        const char *name,
                        const char *value, 
                        char *error_diagnosis,
                        size_t error_diag_len) {
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO;
}
int drmaa_get_attribute(drmaa_job_template_t *jt, 
                        const char *name, char *value,
                        size_t value_len, 
                        char *error_diagnosis,
                        size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO;
}

int drmaa_set_vector_attribute(drmaa_job_template_t *jt, 
                               const char *name,
                               const char *value[], 
                               char *error_diagnosis,
                               size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO;
}
  
int drmaa_get_vector_attribute(drmaa_job_template_t *jt, 
                               const char *name,
                               drmaa_attr_values_t **values,
                               char *error_diagnosis, 
                               size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO;
}

int drmaa_get_attribute_names(drmaa_attr_names_t **values,
                              char *error_diagnosis, 
                              size_t error_diag_len)  { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_get_vector_attribute_names(drmaa_attr_names_t **values,
                                     char *error_diagnosis,
                                     size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_run_job(char *job_id, size_t job_id_len,
                  const drmaa_job_template_t *jt, 
                  char *error_diagnosis,
                  size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_run_bulk_jobs(drmaa_job_ids_t **jobids,
                        const drmaa_job_template_t *jt, 
                        int start, 
                        int end,
                        int incr, 
                        char *error_diagnosis, 
                        size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_control(const char *jobid, 
                  int action, 
                  char *error_diagnosis,
                  size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}
                  
int drmaa_synchronize(const char *job_ids[], 
                      signed long timeout, 
                      int dispose,
                      char *error_diagnosis, 
                      size_t error_diag_len) {  
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}
                      
int drmaa_wait(const char *job_id, 
               char *job_id_out, 
               size_t job_id_out_len,
               int *stat, 
               signed long timeout, 
               drmaa_attr_values_t **rusage, 
               char *error_diagnosis, 
               size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_wifexited(int *exited, int stat, 
                    char *error_diagnosis,
                    size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_wexitstatus(int *exit_status, 
                      int stat, 
                      char *error_diagnosis,
                      size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_wifsignaled(int *signaled, 
                      int stat, 
                      char *error_diagnosis,
                      size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_wtermsig(char *signal, 
                   size_t signal_len, 
                   int stat,
                   char *error_diagnosis, 
                   size_t error_diag_len)  { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_wcoredump(int *core_dumped, 
                    int stat, 
                    char *error_diagnosis,
                    size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_wifaborted(int *aborted, 
                     int stat, 
                     char *error_diagnosis,
                     size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_job_ps(const char *job_id, 
                 int *remote_ps, 
                 char *error_diagnosis,
                 size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

const char *drmaa_strerror(int drmaa_errno) { return( NULL ); }

int drmaa_get_contact(char *contact, 
                      size_t contact_len, 
                      char *error_diagnosis, 
                      size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_version(unsigned int *major, 
                  unsigned int *minor, 
                  char *error_diagnosis, 
                  size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_get_DRM_system(char *drm_system, 
                         size_t drm_system_len, 
                         char *error_diagnosis, 
                         size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

int drmaa_get_DRMAA_implementation(char *drmaa_impl, 
                                   size_t drmaa_impl_len, 
                                   char *error_diagnosis, 
                                   size_t error_diag_len) { 
  snprintf(error_diagnosis, 
           error_diag_len, 
           "Wrong DRMAA library ");
  return DRMAA_NO_ERRNO; 
}

#ifdef  __cplusplus
}
#endif
