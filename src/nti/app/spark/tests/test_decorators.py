#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import has_item
from hamcrest import has_entry
from hamcrest import has_entries
from hamcrest import assert_that

from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.dataserver.tests import mock_dataserver

class TestHiveDecorators(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_table_decoration(self):
        res = self.testapp.get('/dataserver2/spark/hive/OU.orgsync_recommendations',
                            status=200)
        
        assert_that(res.json_body, 
                    has_entries('Links',
                                has_item(has_entry('rel', 'archive'))))
        assert_that(res.json_body,
                    has_entries('Links',
                                has_item(has_entry('rel', 'reset'))))
