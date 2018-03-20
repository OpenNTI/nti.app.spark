#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from requests.structures import CaseInsensitiveDict

from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import get_all_sources
from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.ou.mathplacement import MessageFactory as _

from nti.app.spark.common import parse_timestamp

from nti.cabinet.mixins import NamedSource

from nti.common.string import is_true

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

TOTAL = StandardExternalFields.TOTAL
ITEMS = StandardExternalFields.ITEMS
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

#: Default max source size
DEFAULT_MAX_SOURCE_SIZE = 209715200  # 200mb

class AbstractHiveUploadView(AbstractAuthenticatedView,
                             ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):
        result = super(AbstractHiveUploadView, self).readInput(value)
        return CaseInsensitiveDict(result)

    def max_file_length(self):
        return DEFAULT_MAX_SOURCE_SIZE

    def __call__(self):
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        result[ITEMS] = items = {}
        data = self.readInput()
        # pylint: disable=no-member
        creator = self.remoteUser.username
        # parse timestamp
        timestamp = parse_timestamp(data.get('timestamp'))
        archive = is_true(data.get('archive', True))
        sources = get_all_sources(self.request)
        for name, source in sources.items():
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
            job = self.create_upload_job(creator, target,
                                         timestamp, archive)
            items[filename] = job
        result[ITEM_COUNT] = result[TOTAL] = len(items)
        return result