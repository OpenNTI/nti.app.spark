#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import none
from hamcrest import is_not
from hamcrest import has_length
from hamcrest import assert_that

import unittest

import fudge

from nti.app.spark.scripts.nti_spark_runner import Constructor


class TestRunner(unittest.TestCase):

    @fudge.patch('nti.app.spark.scripts.nti_spark_runner.create_context')
    def test_create_context(self, mock_cc):
        mock_cc.is_callable().returns_fake()
        c = Constructor()
        assert_that(c.create_context('.'), is_not(none()))

    def test_conf_packages(self):
        c = Constructor()
        assert_that(c.conf_packages(), has_length(3))

    @fudge.patch('nti.app.spark.scripts.nti_spark_runner.Processor.process_args')
    def test_process_args(self, mock_pa):
        mock_pa.is_callable().returns_fake()
        class args(object):
            pass
        Constructor().process_args(args())
