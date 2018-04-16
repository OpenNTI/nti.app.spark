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

import os
import time
import shutil
import tempfile
from datetime import datetime

import fudge

from redis_lock import AlreadyAcquired

from nti.app.spark.common import get_site
from nti.app.spark.common import save_source
from nti.app.spark.common import get_redis_lock
from nti.app.spark.common import is_locked_held
from nti.app.spark.common import parse_timestamp

from nti.app.spark.tests import SparkApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.cabinet.mixins import SourceFile


class TestCommon(ApplicationLayerTest):

    layer = SparkApplicationTestLayer

    def test_get_site(self):
        assert_that(get_site(None, object()), is_(none()))

    def test_parse_timestamp(self):
        assert_that(parse_timestamp(time.time()), is_(datetime))
        assert_that(parse_timestamp('2017-11-30'), is_(datetime))

    @fudge.patch('nti.app.spark.common.RedisLock')
    def test_get_redis_lock(self, mock_rl):
        mock_rl.is_callable().returns_fake()
        assert_that(get_redis_lock('foo'), is_not(none()))

    def test_save_source(self):
        tmpdir = tempfile.mkdtemp()
        try:
            name = u'source.txt'
            source = SourceFile(name=name,
                                data=b'data',
                                contentType="application/csv")
            name = save_source(source, tmpdir)
            assert_that(os.path.exists(name), is_(True))
        finally:
            shutil.rmtree(tmpdir)

    @fudge.patch('nti.app.spark.common.RedisLock.acquire',
                 'nti.app.spark.common.RedisLock.release')
    def test_is_locked_held(self, mock_ac, mockl_rel):
        mock_ac.is_callable().returns(True)
        mockl_rel.is_callable().returns_fake()
        assert_that(is_locked_held('foo'), is_(False))

        mock_ac.is_callable().returns(False)
        assert_that(is_locked_held('foo'), is_(True))

        mock_ac.is_callable().raises(AlreadyAcquired)
        assert_that(is_locked_held('foo'), is_(True))