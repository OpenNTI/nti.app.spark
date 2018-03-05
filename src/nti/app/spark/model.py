#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from functools import total_ordering

from zope import interface

from zope.cachedescriptors.property import readproperty

from zope.container.contained import Contained

from zope.mimetype.interfaces import IContentTypeAware

from nti.base._compat import text_

from nti.app.spark.interfaces import FAILED
from nti.app.spark.interfaces import PENDING
from nti.app.spark.interfaces import SUCCESS

from nti.app.spark.interfaces import ISparkJob

from nti.coremetadata.interfaces import SYSTEM_USER_ID

from nti.dublincore.datastructures import PersistentCreatedModDateTrackingObject

from nti.property.property import alias

from nti.schema.eqhash import EqHash

from nti.schema.fieldproperty import createDirectFieldProperties

from nti.schema.schema import SchemaConfigured

logger = __import__('logging').getLogger(__name__)


@EqHash('JobId')
@total_ordering
@interface.implementer(ISparkJob, IContentTypeAware)
class SparkJob(SchemaConfigured,
               PersistentCreatedModDateTrackingObject,
               Contained):
    createDirectFieldProperties(ISparkJob)

    __external_class_name__ = "SparkJob"
    mime_type = mimeType = 'application/vnd.nextthought.spark.job'

    id = alias('__name__')
    state = alias('State')
    jobId = job_id = alias('JobId')

    OutputRoot = None

    parameters = {}

    def __init__(self, *args, **kwargs):
        SchemaConfigured.__init__(self, **kwargs)
        PersistentCreatedModDateTrackingObject.__init__(self, *args, **kwargs)

    def __str__(self, *unused_args, **unused_kwargs):
        return '%s (%s)' % (self.JobId, self.State)
    __repr__ = __str__

    def __lt__(self, other):
        try:
            return self.lastModified < other.lastModified
        except AttributeError:  # pragma: no cover
            return NotImplemented

    @readproperty
    def creator(self):  # pylint: disable=method-hidden
        return SYSTEM_USER_ID

    def is_finished(self):
        """
        Returns whether the job is finished or has failed.
        """
        return self.State in (SUCCESS, FAILED)

    def is_pending(self):
        """
        Returns whether the job has not yet been run.
        """
        return self.State == PENDING

    def is_success(self):
        """
        Returns whether the job has succeeded.
        """
        return self.State == SUCCESS

    def is_failed(self):
        """
        Returns whether the job has failed.
        """
        return self.State == FAILED
    has_failed = is_failed

    def update_to_failed_state(self, reason=None):
        """
        Mark this job as failing.
        """
        # pylint: disable=attribute-defined-outside-init
        self.updateLastMod()
        self.State = FAILED
        self.Error = text_(reason)

    def update_to_success_state(self):
        """
        Mark this job as successful.
        """
        # pylint: disable=attribute-defined-outside-init
        self.updateLastMod()
        self.State = SUCCESS
