#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import zlib
import tempfile
from io import BytesIO
from datetime import date
from datetime import datetime
from six.moves import cPickle as pickle

from redis_lock import AlreadyAcquired
from redis_lock import Lock as RedisLock

from zope import component

from zope.component.hooks import getSite

from nti.common.io import extract_all

from nti.coremetadata.interfaces import IRedisClient

from nti.site.interfaces import IHostPolicyFolder

from nti.spark.utils import parse_date

from nti.traversal.location import find_interface

#: Lock expire time 1.5(hr)
DEFAULT_LOCK_EXPIRY_TIME = 5400

logger = __import__('logging').getLogger(__name__)


def redis_client():
    return component.queryUtility(IRedisClient)


def get_site(site_name=None, context=None):
    if not site_name and context is not None:
        folder = find_interface(context, IHostPolicyFolder)
        site_name = getattr(folder, '__name__', None)
    if not site_name:
        site = getSite()
        site_name = site.__name__ if site is not None else None
    return site_name


def get_creator(context):
    result = getattr(context, 'creator', context)
    result = getattr(result, 'username', result)
    result = getattr(result, 'id', result)  # check 4 principal
    return result


def pickle_dump(context):
    bio = BytesIO()
    pickle.dump(context, bio)
    bio.seek(0)
    result = zlib.compress(bio.read())
    return result


def unpickle(data):
    data = zlib.decompress(data)
    bio = BytesIO(data)
    bio.seek(0)
    result = pickle.load(bio)
    return result


def get_redis_lock(name, expire=DEFAULT_LOCK_EXPIRY_TIME, strict=False):
    return RedisLock(redis_client(), name, expire, strict=strict)


def is_locked_held(name):
    result = True
    lock = get_redis_lock(name)
    try:
        if lock.acquire(False):
            lock.release()
            result = False
    except AlreadyAcquired:
        pass
    return result


def parse_timestamp(timestamp):
    """
    return a datetime object from the specified timestamp
    """
    result = parse_date(timestamp) if timestamp is not None else None
    result = datetime.combine(date.today(), datetime.min.time()) if not result else result
    return result


def save_source(source, path=None):
    # get source filename
    filename = getattr(source, 'filename', None)
    filename = filename or getattr(source, 'name', None) or 'data.dat'
    # get source data
    source = source.data if hasattr(source, 'data') else source
    source = source.read() if hasattr(source, 'read') else source
    # set output path
    path = path or tempfile.mkdtemp()
    name = os.path.split(filename)[1]
    name = os.path.join(path, name)
    with open(name, "w") as fp:
        fp.write(source)
    return extract_all(name)
