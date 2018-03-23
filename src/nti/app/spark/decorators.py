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
from nti.externalization.interfaces import IExternalObjectDecorator

from nti.links.links import Link

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IArchivableHiveTimeIndexed

LINKS = StandardExternalFields.LINKS

logger = __import__('logging').getLogger(__name__)

@component.adapter(IHiveTable, IRequest)
@interface.implementer(IExternalObjectDecorator)
class _HiveTableDecorator(AbstractAuthenticatedRequestAwareDecorator):
    """
    Decorate Hive table operations links onto hive table objects
    on externalization
    """

    ARCHIVE_LINKS = ('reset', 'archive')

    def _do_decorate_external(self, context, result_map):
        links = result_map.setdefault(LINKS, [])
        root_url = self.request.url
        if IArchivableHiveTimeIndexed.providedBy(context):
            for lnk in self.ARCHIVE_LINKS:
                links.append(Link(root_url, elements=('@@%s' % lnk,), 
                                        rel=lnk))
