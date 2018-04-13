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

from zope.cachedescriptors.property import Lazy

from zope.location.interfaces import IContained
from zope.location.interfaces import LocationError

from zope.traversing.interfaces import IPathAdapter
from zope.traversing.interfaces import ITraversable

from nti.app.spark import HIVE_ADAPTER
from nti.app.spark import SPARK_ADAPTER

from nti.app.spark.interfaces import RID_SPARK

from nti.dataserver.authorization import ROLE_ADMIN

from nti.dataserver.authorization_acl import ace_allowing
from nti.dataserver.authorization_acl import acl_from_aces

from nti.dataserver.interfaces import ALL_PERMISSIONS

from nti.spark.interfaces import IHiveTable

#: Fetch the error of a spark job
SPARK_JOB_ERROR = 'SparkJobError'

#: Fetch the status of a spark job
SPARK_JOB_STATUS = 'SparkJobStatus'

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IPathAdapter, IContained)
class AdapterMixin(object):
    
    def __init__(self, context, request):
        self.context = context
        self.request = request
        self.__parent__ = context

    @Lazy
    def __acl__(self):
        aces = [
            ace_allowing(ROLE_ADMIN, ALL_PERMISSIONS, type(self)),
            ace_allowing(RID_SPARK, ALL_PERMISSIONS, type(self)),
        ]
        acl = acl_from_aces(aces)
        return acl


@interface.implementer(ITraversable)
class HivePathAdapter(AdapterMixin):

    __name__ = HIVE_ADAPTER

    def find_hive_table(self, key):
        key = (key or '').lower()
        for table in component.getAllUtilitiesRegisteredFor(IHiveTable):
            if table.table_name.lower() == key:
                return table
    
    def traverse(self, subpath, unused_remaining):
        table = self.find_hive_table(subpath)
        if table is None:
            raise LocationError(subpath)
        return table


class SparkPathAdapter(AdapterMixin):
    __name__ = SPARK_ADAPTER
