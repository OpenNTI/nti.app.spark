#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import none
from hamcrest import is_not
from hamcrest import raises
from hamcrest import calling
from hamcrest import assert_that

import fudge

from nti.app.spark.processing import get_job_site

from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.dataserver.tests import mock_dataserver


class TestProcessing(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    @WithSharedApplicationMockDS
    @fudge.patch('nti.app.spark.processing.get_site_for_site_names')
    def test_get_job_site(self, mock_gs):
        assert_that(get_job_site(None), is_not(none()))
        mock_gs.is_callable().returns(None)
        with mock_dataserver.mock_db_trans():
            assert_that(calling(get_job_site).with_args('foo.com'),
                        raises(ValueError))
