#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import none
from hamcrest import is_not
from hamcrest import assert_that

from nti.testing.matchers import verifiably_provides

from zope import component

from nti.app.spark import SPARK_JOBS_QUEUE

from nti.app.spark.interfaces import ISparkJobQueueFactory

import nti.testing.base

ZCML_STRING = u"""
<configure xmlns="http://namespaces.zope.org/zope"
    xmlns:zcml="http://namespaces.zope.org/zcml"
    xmlns:runner="http://nextthought.com/ntp/sparkjobrunner"
    i18n_domain='nti.dataserver'>

    <include package="zope.component" />
    
    <include package="." file="meta.zcml" />

    <runner:registerProcessingQueue  />

</configure>
"""


class TestZcml(nti.testing.base.ConfiguringTestBase):

    def test_runner(self):
        self.configure_string(ZCML_STRING)
        queue = component.getUtility(ISparkJobQueueFactory)
        assert_that(queue, verifiably_provides(ISparkJobQueueFactory))
        assert_that(queue.get_queue(SPARK_JOBS_QUEUE), is_not(none()))
