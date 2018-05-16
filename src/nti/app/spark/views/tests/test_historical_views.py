#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

import fudge

from zope import component
from zope import interface

from nti.app.spark.interfaces import SUCCESS, FAILED

from nti.app.spark.tests import NoOpCM
from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.spark.interfaces import IArchivableHiveTimeIndexedHistorical


@interface.implementer(IArchivableHiveTimeIndexedHistorical)
class FakeHistorical(object):

    timestamps = (10, 11)

    database = 'fake_database'
    __name__ = table_name = 'fake_historical'

    def unarchive(self, *args, **kwargs):
        pass
    

class TestHistoricalViews(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch("nti.app.spark.views.historical_views.create_drop_partition_job")
    def test_drop_partition(self, mock_gu):
        fake_historical = FakeHistorical()
        mock_gu.is_callable().returns('jobid')
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
                                   status=200)
        finally:
            gsm.unregisterUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                  'fake_historical')

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_unarchive(self):
        fake_historical = FakeHistorical()
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                'fake_historical')

            self.testapp.post_json('/dataserver2/spark/hive/fake_historical/@@unarchive',
                                   {
                                       "timestamp": "2018-04-02",
                                   },
                                   status=200)
        finally:
            gsm.unregisterUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                  'fake_historical')
            
    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.jobs.get_redis_lock',
                 'nti.app.spark.views.historical_views.HiveTableHistoricalTimestampsView.monitor')
    def test_timestamps(self, mock_grl, mock_mon):
        fake_historical = FakeHistorical()
        mock_grl.is_callable().returns(NoOpCM())
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                'fake_historical')
            
            mock_mon.is_callable().returns(SUCCESS)
            self.testapp.get('/dataserver2/spark/hive/fake_historical/@@timestamps',
                             status=200)
            
            mock_mon.is_callable().returns(FAILED)
            self.testapp.get('/dataserver2/spark/hive/fake_historical/@@timestamps',
                             status=422)
        finally:
            gsm.unregisterUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                  'fake_historical')
