#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyramid import httpexceptions as hexc

from zope import component
from zope import interface

from zope.location.interfaces import IContained

from zope.traversing.interfaces import IPathAdapter

from nti.app.spark import HIVE_ADAPTER
from nti.app.spark import SPARK_ADAPTER

from nti.spark.interfaces import IHiveTable

#: Fetch the error of a spark job
SPARK_JOB_ERROR = 'SparkJobError'

#: Fetch the status of a spark job
SPARK_JOB_STATUS = 'SparkJobStatus'

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IPathAdapter, IContained)
class HivePathAdapter(object):

    __name__ = HIVE_ADAPTER

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self.__parent__ = context

    def find_hive_table(self, key):
        for _, table in component.getUtilitiesFor(IHiveTable):
            if table.table_name.lower() == key.lower():
                return table
        
    def __getitem__(self, key):
        table = self.find_hive_table(key or '')
        if table is not None:
            return table
        raise KeyError(key) if key else hexc.HTTPNotFound()


@interface.implementer(IPathAdapter, IContained)
class SparkPathAdapter(object):

    __name__ = SPARK_ADAPTER

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self.__parent__ = context

    def __getitem__(self, key):
        if key == HIVE_ADAPTER:
            return HivePathAdapter(self, self.request)
        raise KeyError(key) if key else hexc.HTTPNotFound()
