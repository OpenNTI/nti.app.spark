#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from nti.app.spark import TABLE
from nti.app.spark import DATABASE
from nti.app.spark import EXTERNAL
from nti.app.spark import HIVE_TABLE_MIMETYPE

from nti.externalization.interfaces import IExternalObject
from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.spark.interfaces import IHiveTable

MIMETYPE = StandardExternalFields.MIMETYPE

logger = __import__('logging').getLogger(__name__)


@component.adapter(IHiveTable)
@interface.implementer(IExternalObject)
class _HiveTableExternal(object):

    def __init__(self, table):
        self.table = table

    def toExternalObject(self, **unused_kwargs):
        result = LocatedExternalDict()
        result[MIMETYPE] = HIVE_TABLE_MIMETYPE
        result[TABLE] = self.table.table_name
        result[DATABASE] = self.table.database
        result[EXTERNAL] = self.table.external
        return result
