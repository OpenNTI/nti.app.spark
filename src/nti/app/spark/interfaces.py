#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=inherit-non-class,no-value-for-parameter

from zope import interface

from zope.location.interfaces import IContained as IZContained

from zope.security.permission import Permission

from zope.schema.vocabulary import SimpleTerm
from zope.schema.vocabulary import SimpleVocabulary

from nti.base.interfaces import ICreated
from nti.base.interfaces import ILastModified

from nti.dataserver.authorization import ROLE_PREFIX

from nti.dataserver.authorization import StringRole

from nti.schema.field import Text
from nti.schema.field import Choice
from nti.schema.field import DecodingValidTextLine as ValidTextLine

#: The ID of a role for spark
RID_SPARK_PREFIX = ROLE_PREFIX + 'nti.dataserver.spark'
RID_SPARK = StringRole(RID_SPARK_PREFIX)

#: Spark admin permission
ACT_SPARK_ADMIN = Permission('nti.actions.spark.admin')

#: Spark jobs permission
ACT_SPARK_JOBS = Permission('nti.actions.spark.jobs')

FAILED = u'Failed'
PENDING = u'Pending'
RUNNING = u'Running'
SUCCESS = u'Success'
RENDER_STATES = (SUCCESS, PENDING, FAILED, RUNNING)
RENDER_STATE_VOCABULARY = SimpleVocabulary(
    [SimpleTerm(x) for x in RENDER_STATES]
)


class ISparkJob(ILastModified, ICreated, IZContained):
    """
    Contains status on a specific spark job
    """

    JobId = ValidTextLine(title=u"The unique job identifier.")

    State = Choice(vocabulary=RENDER_STATE_VOCABULARY,
                   title=u"The state for this render job",
                   required=False,
                   default=PENDING)

    Error = Text(title=u"Rendering error.",
                 required=False)

    callable = interface.Attribute("Job callable")
    callable.setTaggedValue('_ext_excluded_out', True)

    callable_args = interface.Attribute("Job callable arguments")
    callable_args.setTaggedValue('_ext_excluded_out', True)

    callable_kwargs = interface.Attribute("Job callable key word arguments")
    callable_kwargs.setTaggedValue('_ext_excluded_out', True)

    def is_finished():
        """
        Returns whether the job is finished or has failed.
        """

    def is_pending():
        """
        Returns whether the job has not yet been run.
        """

    def is_success():
        """
        Returns whether the job has succeeded.
        """

    def is_failed():
        """
        Returns whether the job has failed.
        """
    has_failed = is_failed

    def update_to_failed_state(reason=None):
        """
        Mark this job as failing.
        """

    def update_to_success_state():
        """
        Mark this job as successful.
        """


class ISparkJobQueueFactory(interface.Interface):
    """
    A factory for spark job queues.
    """
