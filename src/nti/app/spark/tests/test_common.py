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

import time
import unittest
from datetime import datetime

import fudge

from nti.app.spark.common import get_site
from nti.app.spark.common import get_redis_lock
from nti.app.spark.common import parse_timestamp

from nti.app.spark.tests import SharedConfiguringTestLayer


class TestCommon(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_get_site(self):
        assert_that(get_site(None, object()), is_(none()))

    def test_parse_timestamp(self):
        assert_that(parse_timestamp(time.time()), is_(datetime))
        assert_that(parse_timestamp('2017-11-30'), is_(datetime))

    @fudge.patch('nti.app.spark.common.RedisLock')
    def test_get_redis_lock(self, mock_rl):
        mock_rl.is_callable().returns_fake()
        assert_that(get_redis_lock('foo'), is_not(none()))
