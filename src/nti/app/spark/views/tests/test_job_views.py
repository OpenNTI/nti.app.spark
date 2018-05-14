#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import is_
from hamcrest import has_entries
from hamcrest import assert_that

import fudge

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS


class TestJobViews(ApplicationLayerTest):

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.views.job_views.get_job_status')
    def test_get_job_status(self, mock_gjs):
        self.testapp.get('/dataserver2/spark/@@SparkJobStatus',
                         status=422)

        mock_gjs.is_callable().with_args().returns(None)
        self.testapp.get('/dataserver2/spark/@@SparkJobStatus?jobId=job',
                         status=404)

        mock_gjs.is_callable().with_args().returns('Failed')
        res = self.testapp.get('/dataserver2/spark/@@SparkJobStatus?jobId=job',
                               status=200)
        assert_that(res.json_body,
                    has_entries('jobId', is_('job'),
                                'status', is_('Failed')))

    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.views.job_views.get_job_error')
    def test_get_job_error(self, mock_gje):
        self.testapp.get('/dataserver2/spark/@@SparkJobError',
                         status=422)

        mock_gje.is_callable().with_args().returns(None)
        self.testapp.get('/dataserver2/spark/@@SparkJobError?jobId=job',
                         status=404)

        mock_gje.is_callable().with_args().returns({'Error': 'NPE'})
        res = self.testapp.get('/dataserver2/spark/@@SparkJobError?jobId=job',
                               status=200)
        assert_that(res.json_body,
                    has_entries('jobId', is_('job'),
                                'Error', is_('NPE')))
        
    @WithSharedApplicationMockDS(testapp=True, users=True)
    @fudge.patch('nti.app.spark.views.job_views.get_job_result')
    def test_get_job_result(self, mock_gje):
        self.testapp.get('/dataserver2/spark/@@SparkJobResult',
                         status=422)

        mock_gje.is_callable().with_args().returns(None)
        self.testapp.get('/dataserver2/spark/@@SparkJobResult?jobId=job',
                         status=404)

        mock_gje.is_callable().with_args().returns(b'data')
        self.testapp.get('/dataserver2/spark/@@SparkJobResult?jobId=job',
                         status=200)
