#ifndef DRMAAMANIP_H
#define DRMAAMANIP_H

#include <stddef.h>
#include <soma/pipeline/drmaa.h>

class DrmaaManip
{
public:

    DrmaaManip();

    int getErrnum() const {
        return mErrnum;
    }

    const char* getError() const {
        return mError;
    }


protected:
    int mErrnum;
    char mError[DRMAA_ERROR_STRING_BUFFER];

};

#endif // DRMAAMANIP_H
