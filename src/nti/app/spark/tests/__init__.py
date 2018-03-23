#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

import os
import shutil

from zope import component

from zope.component.hooks import setHooks

from nti.app.testing.application_webtest import ApplicationTestLayer

from nti.spark.interfaces import IHiveSparkInstance

from nti.testing.layers import GCLayerMixin
from nti.testing.layers import ZopeComponentLayer
from nti.testing.layers import ConfiguringLayerMixin

import zope.testing.cleanup


class NoOpCM(object):

    def __enter__(self):
        pass

    def __exit__(self, t, v, tb):
        pass


class SharedConfiguringTestLayer(ZopeComponentLayer,
                                 GCLayerMixin,
                                 ConfiguringLayerMixin):

    set_up_packages = ('nti.dataserver', 'nti.app.spark',)

    @classmethod
    def setUp(cls):
        setHooks()
        cls.setUpPackages()

    @classmethod
    def tearDown(cls):
        cls.tearDownPackages()
        zope.testing.cleanup.cleanUp()

    @classmethod
    def testSetUp(cls, unused_test=None):
        setHooks()

    @classmethod
    def testTearDown(cls):
        pass


class SparkApplicationTestLayer(ApplicationTestLayer):

    @classmethod
    def clean_up(cls):
        shutil.rmtree(os.path.join(os.getcwd(), 'home'), True)
        shutil.rmtree(os.path.join(os.getcwd(), 'metastore_db'), True)
        shutil.rmtree(os.path.join(os.getcwd(), 'spark-warehouse'), True)

    @classmethod
    def setUp(cls):
        pass

    @classmethod
    def tearDown(cls):
        component.getUtility(IHiveSparkInstance).close()
        cls.clean_up()

    @classmethod
    def testSetUp(cls, test=None):
        pass

    @classmethod
    def testTearDown(cls, test=None):
        pass
