#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from io import BytesIO

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from requests.structures import CaseInsensitiveDict

from zope import interface

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.renderers.interfaces import INoHrefInResponse

from nti.app.spark import MessageFactory as _

from nti.app.spark.runner import get_job_error
from nti.app.spark.runner import get_job_status
from nti.app.spark.runner import get_job_result

from nti.app.spark.views import SPARK_JOB_ERROR
from nti.app.spark.views import SPARK_JOB_RESULT
from nti.app.spark.views import SPARK_JOB_STATUS

from nti.app.spark.views import SparkPathAdapter

from nti.dataserver import authorization as nauth

from nti.externalization.interfaces import LocatedExternalDict

logger = __import__('logging').getLogger(__name__)


@view_config(name=SPARK_JOB_STATUS)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=SparkPathAdapter,
               permission=nauth.ACT_READ)
class SparkJobStatusView(AbstractAuthenticatedView):

    def __call__(self):
        data = CaseInsensitiveDict(self.request.params)
        job_id = data.get('jobId') or data.get('job_id')
        if not job_id:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                 'message': _(u"Must provide a job identifier"),
                                 'field': 'jobId',
                                 'code': 'InvalidJobID',
                             },
                             None)
        status = get_job_status(job_id)
        if status is None:
            raise hexc.HTTPNotFound()
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        result['jobId'] = job_id
        result['status'] = status
        interface.alsoProvides(result, INoHrefInResponse)
        return result


@view_config(name=SPARK_JOB_ERROR)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=SparkPathAdapter,
               permission=nauth.ACT_READ)
class SparkJobErrorView(AbstractAuthenticatedView):

    def __call__(self):
        data = CaseInsensitiveDict(self.request.params)
        job_id = data.get('jobId') or data.get('job_id')
        if not job_id:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                 'message': _(u"Must provide a job identifier"),
                                 'field': 'jobId',
                                 'code': 'InvalidJobID',
                             },
                             None)
        error = get_job_error(job_id)
        if error is None:
            raise hexc.HTTPNotFound()
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        result['jobId'] = job_id
        result.update(error)
        interface.alsoProvides(result, INoHrefInResponse)
        return result


@view_config(name=SPARK_JOB_RESULT)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=SparkPathAdapter,
               permission=nauth.ACT_READ)
class SparkJobResultView(AbstractAuthenticatedView):

    def prepare_response(self, data):
        data = BytesIO(data)
        data.seek(0)
        response = self.request.response
        response.content_encoding = "identity"
        response.content_disposition = 'attachment; filename=data.dat'
        response.body_file = data
        return response

    def __call__(self):
        data = CaseInsensitiveDict(self.request.params)
        job_id = data.get('jobId') or data.get('job_id')
        if not job_id:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                 'message': _(u"Must provide a job identifier"),
                                 'field': 'jobId',
                                 'code': 'InvalidJobID',
                             },
                             None)
        data = get_job_result(job_id)
        if data is None:
            raise hexc.HTTPNotFound()
        response = self.prepare_response(data)
        return response
