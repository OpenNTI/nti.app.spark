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

from zope import component

from nti.app.spark.jobs import create_table_archive_job
from nti.app.spark.jobs import create_drop_partition_job
from nti.app.spark.jobs import create_generic_table_upload_job

from nti.app.spark.tests import NoOpCM
from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.cabinet.mixins import SourceFile

from nti.spark.interfaces import IHiveTable


class FakeTable(object):
    database = 'fake'
    table_name = 'fake'

    def archive(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def unarchive(self, *args, **kwargs):
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
    def test_create_upload_job(self, mock_grl):
        name = u"test.csv"
        source = SourceFile(name=name,
                            data=self.students_data,
                            contentType='application/csv')
        mock_grl.is_callable().returns(NoOpCM())
        job = create_generic_table_upload_job("pgreazy", source, FakeTable())
        assert_that(job, is_not(none()))

    @WithSharedApplicationMockDS
    @fudge.patch('nti.app.spark.jobs.get_redis_lock',
                 'nti.spark.spark.HiveSparkInstance.drop_partition')
    def test_create_drop_partition_job(self, mock_grl, mock_dp):
        fake_table = FakeTable()
        mock_dp.is_callable().returns_fake()
        mock_grl.is_callable().returns(NoOpCM())
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_table, IHiveTable, 'fake_table')
            job = create_drop_partition_job("pgreazy", 'fake_table', 10)
            assert_that(job, is_not(none()))
        finally:
            gsm.unregisterUtility(fake_table, IHiveTable, 'fake_table')

    @WithSharedApplicationMockDS
    @fudge.patch('nti.app.spark.jobs.get_redis_lock')
    def test_create_archive_job(self, mock_grl):
        fake_table = FakeTable()
        mock_grl.is_callable().returns(NoOpCM())
        try:
            gsm = component.getGlobalSiteManager()
            gsm.registerUtility(fake_table, IHiveTable, 'fake_table')
            job = create_table_archive_job("pgreazy", 'fake_table')
            assert_that(job, is_not(none()))
        finally:
            gsm.unregisterUtility(fake_table, IHiveTable, 'fake_table')
