# -*- coding: utf-8 -*-

from __future__ import absolute_import
from soma_workflow import info

fullVersion = info.__version__
shortVersion = '%d.%d' % (info.version_major, info.version_minor)
