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

#: Fetch the error of a spark job
SPARK_JOB_ERROR = 'SparkJobError'

#: Fetch the status of a spark job
SPARK_JOB_STATUS = 'SparkJobStatus'

#: Fetch the result of a spark job
SPARK_JOB_RESULT = 'SparkJobResult'

# Tables

#: Hive table mimetype
HIVE_TABLE_MIMETYPE = 'application/vnd.nextthought.hive.table'

#: Database column
DATABASE = 'database'

#: Table column
TABLE = 'table'

#: Schema column
SCHEMA = 'schema'

#: External column
EXTERNAL = 'external'

#: Timestamp column
TIMESTAMP = 'timestamp'

#: Timestamps column
TIMESTAMPS = 'timestamps'

logger = __import__('logging').getLogger(__name__)


def get_factory():
    return component.getUtility(ISparkJobQueueFactory)
