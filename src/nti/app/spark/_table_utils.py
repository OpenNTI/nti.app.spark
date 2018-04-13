#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from datetime import datetime

import isodate

from z3c.table import table
from z3c.table import column

from zope.publisher.interfaces.browser import IBrowserRequest

from nti.app.spark.utils import get_table_url

logger = __import__('logging').getLogger(__name__)


class BatchTableMixin(table.Table):

    cssClasses = {
        'table': 'table table-hover',
        'thead': 'thead',
        'tbody': 'tbody',
        'th': 'th',
        'tr': 'tr',
        'td': 'td'
    }

    batchSize = 25
    startBatchingAt = 25

    def batchRows(self):  # pragma: no cover
        try:
            super(BatchTableMixin, self).batchRows()
        except IndexError:
            self.batchStart = len(self.rows) - self.getBatchSize()
            super(BatchTableMixin, self).batchRows()


def make_specific_table(table_clazz, container, request):
    the_table = table_clazz(container, IBrowserRequest(request))
    try:
        the_table.update()
    except IndexError:
        the_table.batchStart = len(the_table.rows) - the_table.getBatchSize()
        the_table.update()
    return the_table


class HiveTimeIndexedTable(BatchTableMixin):
    pass


class TableNameColumn(column.Column):

    weight = 1
    header = u'Name'

    def renderCell(self, item):  # pragma: no cover
        return item.table_name


class TimestampColumn(column.Column):

    weight = 2
    header = u'Timestamp'

    def renderCell(self, item):  # pragma: no cover
        timestamp = item.timestamp
        if timestamp is not None:
            tdt = datetime.fromtimestamp(timestamp) 
            return isodate.datetime_isoformat(tdt, isodate.DATE_EXT_COMPLETE)


class ArchiveColumn(column.Column):

    weight = 3

    def _archive_button(self, item):
        url = get_table_url(item, self.request) + '/@@archive'
        result = """
            <button type="button" class="btn btn-default btn-sm archiveButton" target_title="" action_url="%s" data-toggle="modal" data-target="#archiveModal">
            %s</button>
        """ % (url, 'Archive')
        return result

    def renderCell(self, item):
        return self._archive_button(item)
