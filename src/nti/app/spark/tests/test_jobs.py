#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import none
from hamcrest import is_not
from hamcrest import assert_that

import os

import fudge

from nti.app.spark.jobs import create_generic_table_upload_job

from nti.app.spark.tests import NoOpCM
from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.cabinet.mixins import SourceFile


class FakeTable(object):
    database = 'fake'
    table_name = 'fake'

    def update(self, *args, **kwargs):
        pass


class TestJobs(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    @property
    def students_file(self):
        path = os.path.join(os.path.dirname(__file__),
                            "data", "students.csv")
        return path

    @property
    def students_data(self):
        with open(self.students_file, "r") as fp:
            return fp.read()

    @WithSharedApplicationMockDS
    @fudge.patch('nti.app.spark.jobs.get_redis_lock')
    def test_create_job(self, mock_grl):
        name = u"test.csv"
        source = SourceFile(name=name,
                            data=self.students_data,
                            contentType='application/csv')
        mock_grl.is_callable().returns(NoOpCM())
        job = create_generic_table_upload_job("pgreazy", source, FakeTable())
        assert_that(job, is_not(none()))
