#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import has_length
from hamcrest import has_entries
from hamcrest import assert_that
from hamcrest import greater_than

import fudge

from zope import component
from zope import interface

from nti.app.spark.interfaces import FAILED
from nti.app.spark.interfaces import SUCCESS

from nti.app.spark.tests import NoOpCM
from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.cabinet.mixins import SourceFile

from nti.dataserver.tests import mock_dataserver

from nti.spark.interfaces import IArchivableHiveTimeIndexed
from nti.spark.interfaces import IArchivableHiveTimeIndexedHistorical


@interface.implementer(IArchivableHiveTimeIndexed)
class FakeTable(object):

    rows = None
    timestamp = 10
    external = True
    database = 'fake'

    __name__ = table_name = 'fake_table'

    def reset(self):
        pass

    def archive(self):
        pass


@interface.implementer(IArchivableHiveTimeIndexedHistorical)
class FakeHistorical(FakeTable):

    timestamps = (10, 11)
    __name__ = table_name = 'fake_historical'

    def unarchive(self, *args):
        pass


class TestHiveViews(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_tables(self):
        fake_table = FakeTable()
        fake_historical = FakeHistorical()
        # fake spark/hive
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_table, IArchivableHiveTimeIndexed,
                                'fake_table')
            gsm.registerUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                'fake_historical')
            res = self.testapp.get('/dataserver2/spark/hive/@@tables',
                                   status=200)
            assert_that(res.json_body,
                        has_entries('Items', has_length(greater_than(1)),
                                    'Total', greater_than(1)))

            self.testapp.get('/dataserver2/spark/hive', status=403)
            self.testapp.get('/dataserver2/spark/hive/notfound', status=404)

            res = self.testapp.get('/dataserver2/spark/hive/fake_table',
                                   status=200)
            assert_that(res.json_body,
                        has_entries('database', 'fake',
                                    'table', 'fake_table'))

            res = self.testapp.get('/dataserver2/spark/hive/fake_historical',
                                   status=200)
            assert_that(res.json_body,
                        has_entries('database', 'fake',
                                    'table', 'fake_historical'))
        finally:
            gsm.unregisterUtility(fake_table, IArchivableHiveTimeIndexed,
                                  'fake_table')
            gsm.unregisterUtility(fake_historical, IArchivableHiveTimeIndexedHistorical,
                                  'fake_historical')

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.jobs.get_redis_lock')
    def test_table_archive_reset(self, mock_grl):
        fake_table = FakeTable()
        mock_grl.is_callable().returns(NoOpCM())
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_table, IArchivableHiveTimeIndexed,
                                'fake_table')
            for name in ('archive', 'reset'):
                self.testapp.post('/dataserver2/spark/hive/fake_table/@@' + name,
                                  status=200)
        finally:
            gsm.unregisterUtility(fake_table, IArchivableHiveTimeIndexed,
                                  'fake_table')

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.views.hive_views.create_generic_table_upload_job',
                 'nti.app.spark.views.mixin_views.get_all_sources')
    def test_table_upload(self, mock_tuj, mock_gas):
        name = u"test.csv"
        source = SourceFile(name=name,
                            data=b'data',
                            contentType='application/csv')
        with mock_dataserver.mock_db_trans(self.ds):
            self._create_user("pgreazy")

        unauthed_environ = self._make_extra_environ(username="pgreazy")
        self.testapp.post('/dataserver2/spark/hive/OU.orgsync_recommendations/@@upload',
                          extra_environ=unauthed_environ,
                          status=403)

        mock_gas.is_callable().with_args().returns({
            name: source
        })

        mock_tuj.is_callable().with_args().returns({'jobId': 'myjob'})

        res = self.testapp.post_json('/dataserver2/spark/hive/OU.orgsync_recommendations/@@upload',
                                     {},
                                     status=200)

        assert_that(res.json_body,
                    has_entries('jobId', 'myjob'))

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.jobs.get_redis_lock',
                 'nti.app.spark.views.hive_views.HiveTableTimestampView.monitor')
    def test_timestamp(self, mock_grl, mock_mon):
        fake_table = FakeTable()
        mock_grl.is_callable().returns(NoOpCM())
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_table, IArchivableHiveTimeIndexed,
                                'fake_table')
            mock_mon.is_callable().returns(SUCCESS)
            self.testapp.get('/dataserver2/spark/hive/fake_table/@@timestamp',
                             status=200)

            mock_mon.is_callable().returns(FAILED)
            self.testapp.get('/dataserver2/spark/hive/fake_table/@@timestamp',
                             status=422)
        finally:
            gsm.unregisterUtility(fake_table, IArchivableHiveTimeIndexed,
                                  'fake_table')

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.jobs.get_redis_lock',
                 'nti.app.spark.views.hive_views.HiveTableEmptyView.monitor')
    def test_empty(self, mock_grl, mock_mon):
        fake_table = FakeTable()
        mock_grl.is_callable().returns(NoOpCM())
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_table, IArchivableHiveTimeIndexed,
                                'fake_table')
            mock_mon.is_callable().returns(SUCCESS)
            self.testapp.get('/dataserver2/spark/hive/fake_table/@@empty',
                             status=200)

            mock_mon.is_callable().returns(FAILED)
            self.testapp.get('/dataserver2/spark/hive/fake_table/@@empty',
                             status=422)

        finally:
            gsm.unregisterUtility(fake_table, IArchivableHiveTimeIndexed,
                                  'fake_table')
