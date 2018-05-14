#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import six
import sys
import time
import zlib
import shutil
import tempfile
import simplejson

import transaction

from zope import component
from zope import exceptions
from zope import lifecycleevent

from nti.app.spark import SPARK_JOB
from nti.app.spark import SPARK_JOBS_QUEUE

from nti.app.spark.common import unpickle
from nti.app.spark.common import get_site
from nti.app.spark.common import pickle_dump
from nti.app.spark.common import get_creator
from nti.app.spark.common import save_source
from nti.app.spark.common import redis_client
from nti.app.spark.common import get_redis_lock

from nti.app.spark.interfaces import FAILED
from nti.app.spark.interfaces import PENDING
from nti.app.spark.interfaces import RUNNING
from nti.app.spark.interfaces import SUCCESS

from nti.app.spark.model import SparkJob

from nti.app.spark.processing import put_generic_job

from nti.base._compat import text_

from nti.coremetadata.interfaces import SYSTEM_USER_ID

from nti.ntiids.ntiids import make_ntiid
from nti.ntiids.ntiids import make_specific_safe

from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.utils import csv_mode

from nti.zodb.containers import time_to_64bit_int

# common

EXPIRY_TIME = 172800  # 48hrs

logger = __import__('logging').getLogger(__name__)


def prepare_json_text(s):
    result = s.decode('utf-8') if isinstance(s, bytes) else s
    return result


def format_exception(e):
    result = dict()
    exc_type, exc_value, exc_traceback = sys.exc_info()
    result['message'] = str(e)
    result['code'] = e.__class__.__name__
    result['traceback'] = repr(exceptions.format_exception(exc_type,
                                                           exc_value,
                                                           exc_traceback,
                                                           with_filenames=True))
    del exc_traceback
    return simplejson.dumps(result, indent='\t')


# jobs


def generate_job_id(creator=None):
    creator = get_creator(creator) or SYSTEM_USER_ID
    current_time = time_to_64bit_int(time.time())
    specific = u"%s_%s" % (creator, current_time)
    specific = make_specific_safe(specific)
    return make_ntiid(nttype=SPARK_JOB, specific=specific)


def job_id_status(job_id):
    return "%s=status" % job_id


def job_id_error(job_id):
    return "%s=error" % job_id

def job_id_result(job_id):
    return "%s=result" % job_id


def get_job_status(job_id):
    redis = redis_client()
    if redis is not None:
        key = job_id_status(job_id)
        return redis.get(key)


def get_job_error(job_id):
    redis = redis_client()
    if redis is not None:
        key = job_id_error(job_id)
        result = redis.get(key)
        result = simplejson.loads(prepare_json_text(result)) if result else None
        if isinstance(result, six.string_types):
            result = {
                'message': result,
                'code': 'AssertionError'
            }
        return result


def get_job_result(job_id):
    redis = redis_client()
    if redis is not None:
        key = job_id_result(job_id)
        data = redis.get(key)
        data = zlib.decompress(data) if data is not None else None
        return data

def update_job_status(job_id, status, expiry=EXPIRY_TIME):
    redis = redis_client()
    if redis is not None:
        key = job_id_status(job_id)
        redis.setex(key, value=status, time=expiry)
        return key


def update_job_error(job_id, error, expiry=EXPIRY_TIME):
    redis = redis_client()
    if redis is not None:
        key = job_id_error(job_id)
        redis.setex(key, value=error, time=expiry)
        return key


def update_job_result(job_id, data, expiry=EXPIRY_TIME):
    redis = redis_client()
    if redis is not None and isinstance(data, bytes):
        key = job_id_result(job_id)
        redis.setex(key, value=zlib.compress(data), time=expiry)
        return key


def create_spark_job(creator, func, args=(), kws=None):
    result = SparkJob()
    result.creator = creator
    result.callable = func
    result.callable_args = args
    result.callable_kwargs = kws
    result.job_id = generate_job_id(creator)
    return result


def store_job(job, expiry=EXPIRY_TIME):
    redis = redis_client()
    if redis is not None:
        redis.setex(job.job_id, value=pickle_dump(job), time=expiry)
        return job


def load_job(job_id):
    redis = redis_client()
    if redis is not None:
        pipe = redis.pipeline()
        result = pipe.get(job_id).delete(job_id).execute()
        job = result[0] if result else None
        job = unpickle(job) if job is not None else None
        return job


def run_job(job):
    logger.info('Running job (%s)', job.job_id)
    job_id = job.job_id
    try:
        # 1. update status
        update_job_status(job_id, RUNNING)
        # 2. run callable
        func = job.callable
        args = job.callable_args or ()
        kws = job.callable_kwargs or {}
        result = func(*args, **kws)
        # 3. clean on commit
        def after_commit_or_abort(success=False):
            if success:
                update_job_status(job_id, SUCCESS)
                update_job_result(job_id, result)
        transaction.get().addAfterCommitHook(after_commit_or_abort)
    except Exception as e:  # pylint: disable=broad-except
        logger.exception('Job %s failed', job_id)
        traceback_msg = text_(format_exception(e))
        job.update_to_failed_state(traceback_msg)
        update_job_status(job_id, FAILED)
        update_job_error(job_id, traceback_msg)
    else:
        logger.info('(%s) completed', job_id)
        job.update_to_success_state()
    finally:
        lifecycleevent.modified(job)
    return job


def job_runner(job_id):
    job = load_job(job_id)
    if job is None:
        update_job_status(job_id, FAILED)
        update_job_error(job_id,
                         simplejson.dumps("Job is missing"))
        logger.error("Job %s is missing", job_id)
    else:
        run_job(job)


def queue_job(creator, func, args=(), kws=None, site=None):
    site_name = get_site(site)
    # 1. create job
    job = create_spark_job(get_creator(creator), func, args, kws)
    # 2. store job
    store_job(job)
    # 3. update status
    update_job_status(job.job_id, PENDING)
    # 4. queue job
    put_generic_job(SPARK_JOBS_QUEUE,
                    job_runner,
                    job_id=job.job_id,
                    site_name=site_name)
    return job


# generic upload job


LOCK = "++etc++ou++%s++%s++lock"


def do_table_upload(table, source, overwrite=True, strict=True, spark=None):  # pragma: no cover
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
