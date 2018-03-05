#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import assert_that
from hamcrest import has_entries
from hamcrest import has_property

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

import unittest

from nti.app.spark.interfaces import ISparkJob

from nti.app.spark.model import SparkJob

from nti.app.spark.tests import SharedConfiguringTestLayer

from nti.base.interfaces import ILastModified

from nti.externalization.testing import externalizes


class TestModel(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_job(self):
        job = SparkJob()
        job.jobId = 'foo'
        job.state = 'Failed'
        job.creator = 'bar'
        job.callable = lambda x: x
        job.updateLastMod()

        assert_that(job, validly_provides(ILastModified))
        assert_that(job, validly_provides(ISparkJob))
        assert_that(job, verifiably_provides(ISparkJob))

        assert_that(str(job), is_not(none()))
        assert_that(job, has_property('creator', 'bar'))

        assert_that(job,
                    externalizes(has_entries('JobId', 'foo',
                                             'State', 'Failed')))

        assert_that(job.is_pending(), is_(False))
        assert_that(job.is_finished(), is_(True))
        assert_that(job.is_success(), is_(False))
        assert_that(job.is_failed(), is_(True))

        job.update_to_success_state()
        assert_that(job.is_success(), is_(True))

        job.update_to_failed_state('Exception')
        assert_that(job.is_failed(), is_(True))

        another = SparkJob()
        another.updateLastMod()
        assert_that(sorted([another, job]), is_([job, another]))
