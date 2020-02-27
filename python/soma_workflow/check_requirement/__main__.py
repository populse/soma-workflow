# -*- coding: utf-8 -*-

from __future__ import absolute_import
if __name__ == '__main__':
    from soma_workflow import configuration
    import sys

    if len(sys.argv) >= 2:
        resource_id = sys.argv[1]
    else:
        resource_id = None
    config = configuration.Configuration.load_from_file(resource_id)
    if config is not None and config.get_scheduler_type() == 'drmaa':
        # import drmaa test
        from . import drmaa
        drmaa.test_drmaa()
