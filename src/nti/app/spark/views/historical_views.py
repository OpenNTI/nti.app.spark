#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import time

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from requests.structures import CaseInsensitiveDict

from zope.component import getUtility
from zope.component import getAllUtilitiesRegisteredFor

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.spark import MessageFactory as _

from nti.app.spark._table_utils import make_specific_table
from nti.app.spark._table_utils import HiveTimeIndexedHistoricalTable

from nti.app.spark.common import parse_timestamp

from nti.app.spark.views import HivePathAdapter

from nti.common.string import is_true

from nti.dataserver import authorization as nauth

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IHiveSparkInstance 
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
        timestamp = time.mktime(timestamp.timetuple())
        self.context.unarchive(timestamp, archive)
        return hexc.HTTPNoContent()


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
        timestamp = parse_timestamp(values.get('timestamp'))
        timestamp = time.mktime(timestamp.timetuple())
        spark = getUtility(IHiveSparkInstance)
        spark.drop_partition(self.context.table_name, timestamp)
        return hexc.HTTPNoContent()


@view_config(context=HivePathAdapter)
@view_defaults(route_name="objects.generic.traversal",
               renderer="templates/historical.pt",
               name="historical",
               request_method="GET",
               permission=nauth.ACT_READ)
class HiveTimeIndexedHistoricalTableView(AbstractAuthenticatedView):

    def get_table(self):
        result = {}
        for catalog in getAllUtilitiesRegisteredFor(IHiveTable):
            if IArchivableHiveTimeIndexedHistorical.providedBy(catalog):
                result[catalog.table_name] = catalog
        return result

    def __call__(self):
        data = self.get_table()
        table = make_specific_table(HiveTimeIndexedHistoricalTable,
                                    data, self.request)
        result = {
            'table': table,
        }
        return result
