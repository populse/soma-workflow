# -*- coding: utf-8 -*-
"""
Created on Mon May 13 10:48:02 2013

@author: jinpeng

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}

"""

from __future__ import print_function

if __name__ == "__main__":
    try:
        import soma_workflow.scheduler
        from soma_workflow import configuration
        c = configuration.Configuration.load_from_file()
        if c.get_scheduler_type() == configuration.LOCAL_SCHEDULER:
            # local mode on a remote server
            print("True")
        elif soma_workflow.scheduler.DRMAA_LIB_FOUND:
            print("True")
        else:
            print("False")
    except:
        print("False")
