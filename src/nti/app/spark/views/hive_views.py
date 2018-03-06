#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyramid import httpexceptions as hexc

from pyramid.view import view_config

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.spark.views import HivePathAdapter

from nti.dataserver import authorization as nauth

logger = __import__('logging').getLogger(__name__)


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='GET',
             context=HivePathAdapter,
             permission=nauth.ACT_READ)
class HiveGetView(AbstractAuthenticatedView):

    def __call__(self):
        raise hexc.HTTPForbidden()
