#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component

from nti.dataserver.interfaces import IDataserverClosedEvent

from nti.spark.interfaces import IHiveSparkInstance

logger = __import__('logging').getLogger(__name__)


@component.adapter(IDataserverClosedEvent)
def _closed_dataserver(unused_event):
    try:
        component.getUtility(IHiveSparkInstance).close()
    except Exception:  # pylint: disable=broad-except
        pass
