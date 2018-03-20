#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import is_
from hamcrest import none
from hamcrest import assert_that
from hamcrest import has_entries

import unittest

import fudge

from pyramid import httpexceptions as hexc

from nti.app.spark.views.mixin_views import DEFAULT_MAX_SOURCE_SIZE

from nti.app.spark.views.mixin_views import AbstractHiveUploadView

from nti.cabinet.mixins import SourceFile


class TestMixinViews(unittest.TestCase):

    @fudge.patch('nti.app.spark.views.mixin_views.get_all_sources',
                 'nti.app.spark.views.mixin_views.AbstractHiveUploadView.create_upload_job',
                 'nti.app.spark.views.mixin_views.AbstractHiveUploadView.max_file_length')
    def test_upload_source(self, mock_gas, mock_upj, mock_mfl):
        name = u'source.txt'
        source = SourceFile(name=name,
                            data=b'data',
                            contentType="application/csv")
        mock_gas.is_callable().with_args().returns({
            name: source
        })
        mock_mfl.is_callable().returns(0)
        mock_upj.is_callable().with_args().returns('job')

        request = fudge.Fake()
        request.has_attr(view_name="name").has_attr(context=None)
        view = AbstractHiveUploadView(request)
        with self.assertRaises(hexc.HTTPUnprocessableEntity):
            view.do_call("creator", 100, True)

        mock_mfl.is_callable().returns(100)
        result = view.do_call("creator",  100, True)
        assert_that(result,
                    has_entries('Items', has_entries(name, 'job')))

    def test_coverage(self):
        view = AbstractHiveUploadView(None)
        assert_that(view.max_file_length(), is_(DEFAULT_MAX_SOURCE_SIZE))
        assert_that(view.create_upload_job("creator", "target", 100, True),
                    is_(none()))
