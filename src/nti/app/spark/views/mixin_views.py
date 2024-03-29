#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import gevent

from requests.structures import CaseInsensitiveDict

from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import get_all_sources
from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.spark import MessageFactory as _

from nti.app.spark.common import parse_timestamp

from nti.app.spark.runner import get_job_status

from nti.app.spark.interfaces import FAILED
from nti.app.spark.interfaces import PENDING
from nti.app.spark.interfaces import RUNNING
from nti.app.spark.interfaces import SUCCESS

from nti.cabinet.mixins import NamedSource

from nti.common.string import is_true

from nti.coremetadata.interfaces import SYSTEM_USER_ID

#: Default max source size
DEFAULT_MAX_SOURCE_SIZE = 209715200  # 200mb


class AbstractHiveUploadView(AbstractAuthenticatedView,
                             ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):  # pragma: no cover
        result = None
        if self.request.body:
            result = super(AbstractHiveUploadView, self).readInput(value)
        return CaseInsensitiveDict(result or {})

    def max_file_length(self):
        return DEFAULT_MAX_SOURCE_SIZE

    def create_upload_job(self, creator, target, timestamp, archive, strict):
        raise NotImplementedError()

    def get_timestamp(self, values):
        return parse_timestamp(values.get('timestamp'))

    def validate(self, values):
        """
        Validate this request
        """
        pass

    def do_call(self, creator, timestamp, archive, strict=False):
        sources = get_all_sources(self.request)
        name, source = next(iter(sources.items()))
        if source.length >= self.max_file_length():
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                 'message': _(u"Max file size exceeded"),
                                 'code': 'MaxFileSizeExceeded',
                             },
                             None)
        filename = getattr(source, 'filename', None) or name
        target = NamedSource(name=filename, data=source.data)
        result = self.create_upload_job(creator, target,
                                        timestamp, archive, strict)
        return result

    def __call__(self):  # pragma: no cover
        data = self.readInput()
        self.validate(data)
        # pylint: disable=no-member
        creator = getattr(self.remoteUser, 'username', None) or SYSTEM_USER_ID
        # get parameters
        timestamp = self.get_timestamp(data)
        strict = is_true(data.get('strict', False))
        archive = is_true(data.get('archive', True))
        return self.do_call(creator, timestamp, archive, strict)


class MonitorJobMixin(object):

    DEFAULT_WAIT_TIME = 1
    MAX_ITERATIONS = 3600

    def monitor(self, job_id, wait_time=DEFAULT_WAIT_TIME, iterations=MAX_ITERATIONS):
        count = 0
        status = get_job_status(job_id)
        while status in (PENDING, RUNNING, None) and count <=iterations:
            if status is None:
                raise_json_error(self.request,
                                 hexc.HTTPUnprocessableEntity,
                                 {
                                     'message': _(u"Invalid jobId"),
                                     'code': 'Invalid job id',
                                 },
                                 None)
            if status not in (FAILED, SUCCESS):
                gevent.sleep(wait_time)
            count += 1
            status = get_job_status(job_id)
        return status
