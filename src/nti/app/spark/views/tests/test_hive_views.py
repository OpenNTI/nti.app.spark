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

from zope import component
from zope import interface

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.spark.hive import HiveTable

from nti.spark.interfaces import IHiveTable


@interface.implementer(IHiveTable)
class FakeTable(HiveTable):

    def __init__(self):
        HiveTable.__init__(self, 'fake', 'fake')


class TestSparkViews(ApplicationLayerTest):

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_403(self):
        self.testapp.get('/dataserver2/spark', status=403)

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_tables(self):
        fake = FakeTable()
        gsm = component.getGlobalSiteManager()
        try:
            gsm.registerUtility(fake, IHiveTable, '__fake__')
            res = self.testapp.get('/dataserver2/spark/hive/@@tables',
                                   status=200)
            assert_that(res.json_body,
                        has_entries('Items', has_length(greater_than(0)),
                                    'Total', greater_than(0)))
        finally:
            gsm.unregisterUtility(fake, IHiveTable, '__fake__')
