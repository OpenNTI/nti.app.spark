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

from nti.spark.interfaces import IArchivableHiveTimeIndexed


@interface.implementer(IArchivableHiveTimeIndexed)
class FakeTable(HiveTable):

    timestamp = 10

    def __init__(self):
        HiveTable.__init__(self, 'fake', 'fake')

    def reset(self):
        pass

    def archive(self):
        pass


class TestHiveViews(ApplicationLayerTest):

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_tables(self):
        fake = FakeTable()
        gsm = component.getGlobalSiteManager()
        try:
            gsm.registerUtility(fake, IArchivableHiveTimeIndexed, '__fake__')
            res = self.testapp.get('/dataserver2/spark/hive/@@tables',
                                   status=200)
            assert_that(res.json_body,
                        has_entries('Items', has_length(greater_than(0)),
                                    'Total', greater_than(0)))

            self.testapp.get('/dataserver2/spark/hive', status=403)
            self.testapp.get('/dataserver2/spark/hive/notfound', status=404)

            res = self.testapp.get('/dataserver2/spark/hive/fake',
                                   status=200)
            assert_that(res.json_body,
                        has_entries('database', 'fake',
                                    'table', 'fake',
                                    'timestamp', 10))

            self.testapp.post('/dataserver2/spark/hive/fake/@@reset',
                              status=204)

            self.testapp.post('/dataserver2/spark/hive/fake/@@archive',
                              status=204)
        finally:
            gsm.unregisterUtility(fake, IArchivableHiveTimeIndexed, '__fake__')
