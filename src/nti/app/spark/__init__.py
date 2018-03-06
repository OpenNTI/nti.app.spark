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

# Adapters

#: Hive Adapter
HIVE_ADAPTER = u'hive'

#: Spark Adapter
SPARK_ADAPTER = u'spark'

# Jobs

#: Spark job NTIID Type
SPARK_JOB = u'SparkJob'

#: Spark jobs redis queue name
SPARK_JOBS_QUEUE = '++etc++spark++queue++jobs'

QUEUE_NAMES = (SPARK_JOBS_QUEUE,)

# Tables

#: Hive table mimetype
HIVE_TABLE_MIMETYPE = 'application/vnd.nextthought.hive.table'

#: Database column
DATABASE = 'database'

#: Table column
TABLE = 'table'

#: External column
EXTERNAL = 'external'

logger = __import__('logging').getLogger(__name__)


def get_factory():
    return component.getUtility(ISparkJobQueueFactory)
