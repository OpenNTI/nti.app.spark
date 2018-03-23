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

from pyramid.interfaces import IRequest

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IArchivableHiveTimeIndexed

LINKS = StandardExternalFields.LINKS

logger = __import__('logging').getLogger(__name__)

@component.adapter(IHiveTable, IRequest)
@interface.implementer(IExternalMappingDecorator)
class _HiveTableDecorator(AbstractAuthenticatedRequestAwareDecorator):
    """
    Decorate Hive table operations links onto hive table objects
    on externalization
    """

    def _do_decorate_external(self, context, result_map):
        from pdb import set_trace; set_trace()
        links = result_map.setdefault(LINKS, [])
