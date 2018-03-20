#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import has_entry
from hamcrest import assert_that

import unittest

from nti.app.spark.interfaces import FAILED
from nti.app.spark.interfaces import SUCCESS

from nti.app.spark.runner import queue_job
from nti.app.spark.runner import job_runner
from nti.app.spark.runner import get_job_error
from nti.app.spark.runner import get_job_status

from nti.app.spark.tests import SharedConfiguringTestLayer

from nti.coremetadata.interfaces import SYSTEM_USER_ID

from nti.dataserver.tests import mock_dataserver


def good_job():
    pass


def failed_job():
    raise Exception()


class TestRunner(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    @mock_dataserver.WithMockDS
    def test_good_job(self):
        # run job
        with mock_dataserver.mock_db_trans():
            job = queue_job(SYSTEM_USER_ID, good_job)
            assert_that(job, is_not(none))
        # test status
        job_id = job.job_id
        assert_that(get_job_status(job_id), is_(SUCCESS))

    @mock_dataserver.WithMockDS
    def test_failed_job(self):
        # run job
        with mock_dataserver.mock_db_trans():
            job = queue_job(SYSTEM_USER_ID, failed_job, site="dataserver2")
            assert_that(job, is_not(none))
        # test status
        job_id = job.job_id
        assert_that(get_job_status(job_id), is_(FAILED))
        assert_that(get_job_error(job_id), is_not(none()))

    @mock_dataserver.WithMockDS
    def test_missig_job(self):
        job_runner('missing')
        status = get_job_error('missing')
        assert_that(status, has_entry('message', 'Job is missing'))
