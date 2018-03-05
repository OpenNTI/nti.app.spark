#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component

from nti.app.spark.interfaces import ISparkJobQueueFactory

#: Spark job NTIID Type
SPARK_JOB = u'SparkJob'

#: Spark jobs redis queue name
SPARK_JOBS_QUEUE = '++etc++spark++queue++jobs'

QUEUE_NAMES = (SPARK_JOBS_QUEUE,)

logger = __import__('logging').getLogger(__name__)


def get_factory():
    return component.getUtility(ISparkJobQueueFactory)
