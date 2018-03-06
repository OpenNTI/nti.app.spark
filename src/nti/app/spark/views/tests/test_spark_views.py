#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ


from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS


class TestSparkViews(ApplicationLayerTest):

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_403(self):
        self.testapp.get('/dataserver2/spark', status=403)

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_spark_404(self):
        self.testapp.get('/dataserver2/spark/unknown', status=404)
