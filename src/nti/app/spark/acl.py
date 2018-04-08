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

from zope.cachedescriptors.property import Lazy

from nti.app.spark.interfaces import RID_SPARK

from nti.dataserver.authorization import ROLE_ADMIN

from nti.dataserver.authorization_acl import ace_allowing
from nti.dataserver.authorization_acl import acl_from_aces

from nti.dataserver.interfaces import ALL_PERMISSIONS

from nti.dataserver.interfaces import IACLProvider

from nti.spark.interfaces import IHiveTable

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IACLProvider)
class _ACLProviderMixin(object):

    def __init__(self, context):
        self.context = context

    @Lazy
    def __acl__(self):
        aces = [
            ace_allowing(ROLE_ADMIN, ALL_PERMISSIONS, type(self)),
            ace_allowing(RID_SPARK, ALL_PERMISSIONS, type(self)),
        ]
        acl = acl_from_aces(aces)
        return acl


@component.adapter(IHiveTable)
class _HiveTableACLProvider(_ACLProviderMixin):
    pass
