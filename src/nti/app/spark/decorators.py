#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyramid.interfaces import IRequest

from zope import component
from zope import interface

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.app.spark.interfaces import ISparkJob

from nti.app.spark.utils import get_spark_href

from nti.app.spark.views import SPARK_JOB_ERROR
from nti.app.spark.views import SPARK_JOB_RESULT
from nti.app.spark.views import SPARK_JOB_STATUS

from nti.appserver.pyramid_authorization import has_permission

from nti.dataserver.authorization import ACT_READ

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalObjectDecorator

from nti.links.links import Link

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IArchivableHiveTimeIndexed
from nti.spark.interfaces import IArchivableHiveTimeIndexedHistorical


LINKS = StandardExternalFields.LINKS

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IExternalObjectDecorator)
class _TableDecoratorMixin(AbstractAuthenticatedRequestAwareDecorator):
    """
    Decorate Hive table operations links onto hive table objects
    on externalization
    """

    LINKS = ('upload',)

    def _predicate(self, context, unused_result):
        # pylint: disable=too-many-function-args
        return bool(self.authenticated_userid) \
           and has_permission(ACT_READ, context, self.request)

    def _generate_links(self, names, links, root_url):
        for lnk in names or ():
            links.append(Link(root_url, elements=('@@%s' % lnk,),
                              rel=lnk))

    def _do_decorate_external(self, unused_context, result):
        links = result.setdefault(LINKS, [])
        self._generate_links(self.LINKS, links, self.request.url)


@component.adapter(IHiveTable, IRequest)
class _HiveTableDecorator(_TableDecoratorMixin):
    """
    Decorate Hive table operations links onto hive table objects
    on externalization
    """
    LINKS = ('upload',)


@component.adapter(IArchivableHiveTimeIndexed, IRequest)
class _ArchivableHiveTableDecorator(_TableDecoratorMixin):
    """
    Decorate Hive table operations links onto archivable hive table objects
    on externalization
    """
    LINKS = ('reset', 'archive')


@component.adapter(IArchivableHiveTimeIndexedHistorical, IRequest)
class _ArchivableHiveTableHistoricalDecorator(_TableDecoratorMixin):
    """
    Decorate Hive table operations links onto historical archivable hive table objects
    on externalization
    """
    LINKS = ('unarchive',)


@component.adapter(ISparkJob, IRequest)
@interface.implementer(IExternalObjectDecorator)
class _SparkJobDecorator(AbstractAuthenticatedRequestAwareDecorator):
    """
    Decorate spark job links
    """

    LINKS = (SPARK_JOB_ERROR, SPARK_JOB_STATUS, SPARK_JOB_RESULT)

    def _predicate(self, context, unused_result):
        # pylint: disable=too-many-function-args
        return bool(self.authenticated_userid) \
           and has_permission(ACT_READ, context, self.request)

    def _generate_links(self, names, links, root_url):
        for lnk in names or ():
            links.append(Link(root_url, elements=('@@%s' % lnk,),
                              rel=lnk))

    def _do_decorate_external(self, context, result):
        spark_url = get_spark_href(self.request)
        links = result.setdefault(LINKS, [])
        params =  {'jobId': context.id}
        for name in self.LINKS:
            result = links.append(Link(spark_url, elements=(name,),
                                       params=params, method='GET'))
