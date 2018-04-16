#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

import fudge

from zope import component
from zope import interface

from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.spark.interfaces import IArchivableHiveTimeIndexedHistorical


@interface.implementer(IArchivableHiveTimeIndexedHistorical)
class FakeHistorical(object):

    timestamps = (10, 11)
    __name__ = table_name = 'fake_historical'


class TestHistoricalViews(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch("nti.app.spark.views.historical_views.getUtility")
    def test_spark_tables(self, mock_gu):
        fake_historical = FakeHistorical()
        # fake spark/hive
        hive = fudge.Fake()
        hive.provides("drop_partition").returns_fake()
        mock_gu.is_callable().returns(hive)
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                'fake_historical')

            self.testapp.post('/dataserver2/spark/hive/fake_historical/@@drop_partition',
                              status=422)

            self.testapp.post_json('/dataserver2/spark/hive/fake_historical/@@drop_partition',
                                   {
                                       "timestamp": "2018-04-02",
                                   },
                                   status=204)
        finally:
            gsm.unregisterUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                  'fake_historical')
