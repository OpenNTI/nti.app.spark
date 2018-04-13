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

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.spark.common import parse_timestamp

from nti.dataserver import authorization as nauth

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
        timestamp = parse_timestamp(values.get('timestamp'))
        self.context.unarchive(timestamp)
        return hexc.HTTPNoContent()
