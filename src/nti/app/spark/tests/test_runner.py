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

import fudge

from nti.app.spark.interfaces import FAILED
from nti.app.spark.interfaces import SUCCESS

from nti.app.spark.runner import queue_job
from nti.app.spark.runner import job_runner
from nti.app.spark.runner import get_job_error
from nti.app.spark.runner import get_job_status
from nti.app.spark.runner import get_job_result
from nti.app.spark.runner import create_generic_table_upload_job

from nti.app.spark.tests import NoOpCM
from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.coremetadata.interfaces import SYSTEM_USER_ID

from nti.cabinet.mixins import SourceFile

from nti.dataserver.tests import mock_dataserver


def good_job():
    return b'data'


def failed_job():
    raise Exception()


class FakeTable(object):
    database = 'fake'
    table_name = 'fake'

    def update(self, *args, **kwargs):
        pass


class TestRunner(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    @WithSharedApplicationMockDS
    def test_good_job(self):
        # run job
        with mock_dataserver.mock_db_trans():
            job = queue_job(SYSTEM_USER_ID, good_job)
            assert_that(job, is_not(none))
        # test status
        job_id = job.job_id
        assert_that(get_job_status(job_id), is_(SUCCESS))
        assert_that(get_job_result(job_id), is_(b'data'))

    @WithSharedApplicationMockDS
    def test_failed_job(self):
        # run job
        with mock_dataserver.mock_db_trans():
            job = queue_job(SYSTEM_USER_ID, failed_job, site="dataserver2")
            assert_that(job, is_not(none))
        # test status
        job_id = job.job_id
        assert_that(get_job_status(job_id), is_(FAILED))
        assert_that(get_job_error(job_id), is_not(none()))

    @WithSharedApplicationMockDS
    def test_missig_job(self):
        job_runner('missing')
        status = get_job_error('missing')
        assert_that(status, has_entry('message', 'Job is missing'))

    @WithSharedApplicationMockDS
    @fudge.patch('nti.app.spark.runner.do_table_upload',
                 'nti.app.spark.runner.get_redis_lock')
    def test_upload(self, mock_load, mock_grl):
        name = u"test.csv"
        source = SourceFile(name=name,
                            data=b'data',
                            contentType='application/csv')
        mock_load.is_callable().returns_fake()
        mock_grl.is_callable().returns(NoOpCM())
        job = create_generic_table_upload_job("pgreazy", source, FakeTable())
        assert_that(job, is_not(none()))
