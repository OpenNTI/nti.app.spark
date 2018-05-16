#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import shutil
import tempfile

from zope import component

from nti.app.spark.common import save_source
from nti.app.spark.common import get_redis_lock

from nti.app.spark.runner import queue_job

from nti.spark import TIMESTAMP

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.utils import csv_mode
from nti.spark.utils import get_timestamp

logger = __import__('logging').getLogger(__name__)

# generic upload job


LOCK = "++etc++%s++%s++lock"


def do_table_upload(table, source, overwrite=True, strict=True, spark=None):
    spark = component.getUtility(IHiveSparkInstance) if spark is None else spark
    # Read file blind of schema - allow the
    # job to fail if a bad format is given
    data_frame = spark.hive.read.csv(
        source, header=True, mode=csv_mode(strict),
    )
    table.update(data_frame, overwrite=overwrite)
    return data_frame


def generic_upload_job(context, source, overwrite, strict=False):
    # This should match the locks if uploading from specifig
    # database URL
    table_lock = LOCK % (context.database, context.table_name)
    with get_redis_lock(table_lock):
        tmpdir = tempfile.mkdtemp()
        try:
            source_file = save_source(source, tmpdir)
            do_table_upload(context, source_file, overwrite, strict)
        finally:
            shutil.rmtree(tmpdir, True)


def create_generic_table_upload_job(creator, source, context,
                                    overwrite=True, strict=False):
    return queue_job(creator, generic_upload_job,
                     args=(context, source, overwrite, strict))


def reset_table_job(name):
    context = component.getUtility(IHiveTable, name)
    table_lock = LOCK % (context.database, context.table_name)
    with get_redis_lock(table_lock):
        context.reset()


def create_table_reset_job(creator, name):
    return queue_job(creator, generic_upload_job,
                     args=(name,))


def archive_table_job(name):
    context = component.getUtility(IHiveTable, name)
    table_lock = LOCK % (context.database, context.table_name)
    with get_redis_lock(table_lock):
        context.archive()


def create_table_archive_job(creator, name):
    return queue_job(creator, archive_table_job,
                     args=(name,))


def drop_partition_job(name, timestamp):
    context = component.getUtility(IHiveTable, name)
    table_lock = LOCK % (context.database, context.table_name)
    with get_redis_lock(table_lock):
        tstamp = get_timestamp(timestamp)
        spark = component.getUtility(IHiveSparkInstance)
        spark.drop_partition(context.table_name,
                             {TIMESTAMP: tstamp})


def create_drop_partition_job(creator, name, timestamp):
    return queue_job(creator, drop_partition_job,
                     args=(name, timestamp))


def unarchive_table_job(name, timestamp, archive):
    context = component.getUtility(IHiveTable, name)
    table_lock = LOCK % (context.database, context.table_name)
    with get_redis_lock(table_lock):
        timestamp = get_timestamp(timestamp)
        context.unarchive(timestamp, archive)


def create_table_unarchive_job(creator, name, timestamp, archive):
    return queue_job(creator, unarchive_table_job,
                     args=(name, timestamp, archive))


def timestamp_table_job(name):
    context = component.getUtility(IHiveTable, name)
    table_lock = LOCK % (context.database, context.table_name)
    with get_redis_lock(table_lock):
        return context.timestamp


def create_table_timestamp_job(creator, name):
    return queue_job(creator, timestamp_table_job,
                     args=(name,))


def timestamps_table_job(name):
    context = component.getUtility(IHiveTable, name)
    table_lock = LOCK % (context.database, context.table_name)
    with get_redis_lock(table_lock):
        return context.timestamps


def create_table_timestamps_job(creator, name):
    return queue_job(creator, timestamps_table_job,
                     args=(name,))
