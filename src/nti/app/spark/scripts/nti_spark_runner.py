#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import atexit

from zope import component

from nti.app.asynchronous.processor import Processor

from nti.app.spark import QUEUE_NAMES

from nti.dataserver.utils.base_script import create_context

from nti.spark.interfaces import IHiveSparkInstance

logger = __import__('logging').getLogger(__name__)


def close_at_exit():
    try:
        component.queryUtility(IHiveSparkInstance).close()
    except Exception:  # pylint: disable=broad-except
        pass
atexit.register(close_at_exit)


class Constructor(Processor):

    def create_context(self, env_dir, unused_args=None):
        context = create_context(env_dir,
                                 with_library=False,
                                 plugins=True,
                                 slugs=True)
        return context

    def conf_packages(self):
        return (self.conf_package, 'nti.app.spark', 'nti.asynchronous')

    def process_args(self, args):
        setattr(args, 'redis', True)
        setattr(args, 'library', True)
        setattr(args, 'priority', True)
        setattr(args, 'trx_retries', 9)
        setattr(args, 'queue_names', QUEUE_NAMES)
        Processor.process_args(self, args)


def main():  # pragma: no cover
    return Constructor()()


if __name__ == '__main__':  # pragma: no cover
    main()
