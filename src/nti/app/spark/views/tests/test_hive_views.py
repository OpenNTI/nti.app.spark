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

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS


class TestSparkViews(ApplicationLayerTest):

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_403(self):
        self.testapp.get('/dataserver2/spark', status=403)

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_tables(self):
        res = self.testapp.get('/dataserver2/spark/hive/@@tables',
                               status=200)
        assert_that(res.json_body,
                    has_entries('Items', has_length(greater_than(0)),
                                'Total', greater_than(0)))
