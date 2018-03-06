#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from zope import component

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.spark.views import HivePathAdapter

from nti.dataserver import authorization as nauth

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.spark.interfaces import IHiveTable

TOTAL = StandardExternalFields.TOTAL
ITEMS = StandardExternalFields.ITEMS
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

logger = __import__('logging').getLogger(__name__)


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='GET',
             context=HivePathAdapter,
             permission=nauth.ACT_READ)
class HiveGetView(AbstractAuthenticatedView):

    def __call__(self):
        raise hexc.HTTPForbidden()


@view_config(name="tables")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=HivePathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class HiveTablesView(AbstractAuthenticatedView):

    def __call__(self):
        result = LocatedExternalDict()
        result[ITEMS] = items = []
        for _, table in component.getUtilitiesFor(IHiveTable):
            items.append(to_external_object(table))
        result[TOTAL] = result[ITEM_COUNT] = len(items)
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        return result
