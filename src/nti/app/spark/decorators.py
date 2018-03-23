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

from nti.appserver.pyramid_authorization import has_permission

from nti.dataserver.authorization import ACT_READ

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

    GENERAL_LINKS = ('upload',)
    ARCHIVE_LINKS = ('reset', 'archive')

    def _predicate(self, context, unused_result):
        # pylint: disable=too-many-function-args
        return bool(self.authenticated_userid) \
           and has_permission(ACT_READ, context, self.request)

    def _generate_links(self, names, links, root_url):
        for lnk in names or ():
            links.append(Link(root_url, elements=('@@%s' % lnk,),
                              rel=lnk))

    def _do_decorate_external(self, context, result):
        links = result.setdefault(LINKS, [])
        root_url = self.request.url
        self._generate_links(self.GENERAL_LINKS, links, root_url)
        if IArchivableHiveTimeIndexed.providedBy(context):
            self._generate_links(self.ARCHIVE_LINKS, links, root_url)
