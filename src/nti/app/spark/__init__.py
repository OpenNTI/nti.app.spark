#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import zope.i18nmessageid
MessageFactory = zope.i18nmessageid.MessageFactory(__name__)

from zope import component

from nti.app.spark.interfaces import ISparkJobQueueFactory

#: Spark Adapter
SPARK_ADAPTER = u'spark'

#: Spark job NTIID Type
SPARK_JOB = u'SparkJob'

#: Spark jobs redis queue name
SPARK_JOBS_QUEUE = '++etc++spark++queue++jobs'

QUEUE_NAMES = (SPARK_JOBS_QUEUE,)

logger = __import__('logging').getLogger(__name__)


def get_factory():
    return component.getUtility(ISparkJobQueueFactory)
