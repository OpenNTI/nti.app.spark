#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyramid.threadlocal import get_current_request

from six.moves import urllib_parse

from nti.app.spark import HIVE_ADAPTER
from nti.app.spark import SPARK_ADAPTER

logger = __import__('logging').getLogger(__name__)


def get_ds2(request=None):
    request = request if request else get_current_request()
    try:
        result = request.path_info_peek() if request else None
    except AttributeError:  # in unit test we may see this
        result = None
    return result or "dataserver2"


def get_spark_href(request=None):
    ds2 = get_ds2(request)
    href = '/%s/%s' % (ds2, SPARK_ADAPTER)
    return href


def get_table_href(context, request=None):
    ds2 = get_ds2(request)
    href = '/%s/%s/%s/%s' % (ds2, SPARK_ADAPTER, HIVE_ADAPTER,
                             urllib_parse.quote(context.table_name))
    return href


def get_table_url(context, request=None):
    request = request if request else get_current_request()
    href = get_table_href(context, request=request)
    result = urllib_parse.urljoin(request.host_url, href) if href else href
    return result
