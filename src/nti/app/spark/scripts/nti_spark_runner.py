#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from nti.app.asynchronous.processor import Processor

from nti.app.spark import QUEUE_NAMES

from nti.dataserver.utils.base_script import create_context

logger = __import__('logging').getLogger(__name__)


class Constructor(Processor):

    def create_context(self, env_dir, unused_args=None):
        context = create_context(env_dir,
                                 with_library=False,
                                 plugins=True,
                                 slugs=True,
                                 slugs_files=("*spark.zcml", "*features.zcml"))
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
