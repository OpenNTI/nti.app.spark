#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import interface

from zope.location.interfaces import IContained

from zope.traversing.interfaces import IPathAdapter

from nti.app.spark import SPARK_ADAPTER

#: Fetch the error of a spark job
SPARK_JOB_ERROR = 'SparkJobError'

#: Fetch the status of a spark job
SPARK_JOB_STATUS = 'SparkJobStatus'

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IPathAdapter, IContained)
class SparkPathAdapter(object):

    __name__ = SPARK_ADAPTER

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self.__parent__ = context
