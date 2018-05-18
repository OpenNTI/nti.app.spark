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

from requests.structures import CaseInsensitiveDict

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.spark import MessageFactory as _

from nti.app.spark.common import parse_timestamp

from nti.app.spark.interfaces import SUCCESS

from nti.app.spark.jobs import create_drop_partition_job
from nti.app.spark.jobs import create_table_unarchive_job
from nti.app.spark.jobs import create_table_timestamps_job

from nti.app.spark.runner import get_job_result

from nti.app.spark.views.mixin_views import MonitorJobMixin

from nti.common.string import is_true

from nti.dataserver import authorization as nauth

from nti.spark.hive import get_timestamp

from nti.spark.interfaces import IArchivableHiveTimeIndexedHistorical


logger = __import__('logging').getLogger(__name__)


@view_config(name="unarchive")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=IArchivableHiveTimeIndexedHistorical,
               permission=nauth.ACT_READ)
class HiveTableHistoricalUnarchiveView(AbstractAuthenticatedView,
                                       ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):
        result = None
        if self.request.body:
            result = super(HiveTableHistoricalUnarchiveView, self).readInput(value)
        return CaseInsensitiveDict(result or {})

    def __call__(self):
        # pylint: disable=no-member
        values = self.readInput()
        archive = is_true(values.get('archive'))
        timestamp = parse_timestamp(values.get('timestamp'))
        timestamp = get_timestamp(timestamp)
        return create_table_unarchive_job(self.remoteUser.username,
                                          self.context.table_name,
                                          timestamp,
                                          archive)


@view_config(name="dropPartition")
@view_config(name="drop_partition")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=IArchivableHiveTimeIndexedHistorical,
               permission=nauth.ACT_READ)
class HiveTableHistoricalDropPartitionView(AbstractAuthenticatedView,
                                           ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):
        result = None
        if self.request.body:
            result = super(HiveTableHistoricalDropPartitionView, self).readInput(value)
        return CaseInsensitiveDict(result or {})

    def __call__(self):
        # pylint: disable=no-member
        values = self.readInput()
        timestamp = values.get('timestamp')
        if not timestamp:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                 'message': _(u"Must provide a timestamp"),
                                 'field': 'timestamp',
                             },
                             None)
        timestamp = parse_timestamp(timestamp)
        timestamp = get_timestamp(timestamp)
        return create_drop_partition_job(self.remoteUser.username,
                                         self.context.table_name,
                                         timestamp)


@view_config(name="timestamps")
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=IArchivableHiveTimeIndexedHistorical,
               permission=nauth.ACT_READ)
class HiveTableHistoricalTimestampsView(AbstractAuthenticatedView,
                                        MonitorJobMixin):

    def __call__(self):
        # pylint: disable=no-member
        job = create_table_timestamps_job(self.remoteUser.username,
                                          self.context.table_name)
        result = self.monitor(job.jobId)
        if result == SUCCESS:
            return get_job_result(job.jobId) or ()
        raise_json_error(self.request,
                         hexc.HTTPUnprocessableEntity,
                         {
                             'message': _(u"Cannot get timestamps"),
                             'code': 'CannotGetTimeStamps',
                         },
                         None)
