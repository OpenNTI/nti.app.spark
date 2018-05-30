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

from nti.app.externalization.error import raise_json_error

from nti.app.spark import MessageFactory as _

from nti.app.spark.interfaces import SUCCESS

from nti.app.spark.jobs import create_table_empty_job
from nti.app.spark.jobs import create_table_reset_job
from nti.app.spark.jobs import create_table_archive_job
from nti.app.spark.jobs import create_table_timestamp_job
from nti.app.spark.jobs import create_generic_table_upload_job

from nti.app.spark.runner import get_job_result

from nti.app.spark.views import HivePathAdapter

from nti.app.spark.views.mixin_views import MonitorJobMixin
from nti.app.spark.views.mixin_views import AbstractHiveUploadView

from nti.dataserver import authorization as nauth

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IArchivableHiveTimeIndexed

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
        for table in component.getAllUtilitiesRegisteredFor(IHiveTable):
            items.append(to_external_object(table))
        result[TOTAL] = result[ITEM_COUNT] = len(items)
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        return result


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='GET',
             context=IHiveTable,
             permission=nauth.ACT_READ)
class HiveTableGetView(AbstractAuthenticatedView):

    def __call__(self):
        return to_external_object(self.context)


@view_config(name="reset")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=IArchivableHiveTimeIndexed,
               permission=nauth.ACT_READ)
class HiveTableResetView(AbstractAuthenticatedView):

    def __call__(self):
        # pylint: disable=no-member
        return create_table_reset_job(self.remoteUser.username,
                                      self.context.table_name)


@view_config(name="archive")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=IArchivableHiveTimeIndexed,
               permission=nauth.ACT_READ)
class HiveTableArchiveView(AbstractAuthenticatedView):

    def __call__(self):
        # pylint: disable=no-member
        return create_table_archive_job(self.remoteUser.username,
                                        self.context.table_name)


@view_config(name="upload")
@view_defaults(route_name="objects.generic.traversal",
               renderer="rest",
               request_method="POST",
               context=IHiveTable,
               permission=nauth.ACT_NTI_ADMIN)
class HiveTableUploadView(AbstractHiveUploadView):

    def create_upload_job(self, creator, target, unused_timestamp,
                          unused_archive, unused_strict):
        return create_generic_table_upload_job(creator, target, self.context)


@view_config(name="timestamp")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=IArchivableHiveTimeIndexed,
               permission=nauth.ACT_READ)
class HiveTableTimestampView(AbstractAuthenticatedView,
                             MonitorJobMixin):

    def __call__(self):
        # pylint: disable=no-member
        job = create_table_timestamp_job(self.remoteUser.username,
                                         self.context.table_name)
        result = self.monitor(job.jobId)
        if result == SUCCESS:
            return get_job_result(job.jobId) or hexc.HTTPNoContent()
        raise_json_error(self.request,
                         hexc.HTTPUnprocessableEntity,
                         {
                             'message': _(u"Cannot get timestamp"),
                             'code': 'CannotGetTimeStamp',
                         },
                         None)


@view_config(name="empty")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=IArchivableHiveTimeIndexed,
               permission=nauth.ACT_READ)
class HiveTableEmptyView(AbstractAuthenticatedView,
                         MonitorJobMixin):

    def __call__(self):
        # pylint: disable=no-member
        job = create_table_empty_job(self.remoteUser.username,
                                     self.context.table_name)
        result = self.monitor(job.jobId)
        if result == SUCCESS:
            return get_job_result(job.jobId) or hexc.HTTPNoContent()
        raise_json_error(self.request,
                         hexc.HTTPUnprocessableEntity,
                         {
                             'message': _(u"Cannot access table state"),
                             'code': 'CannotAccessTableState',
                         },
                         None)
