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


class HiveTimeIndexedHistoricalTable(BatchTableMixin):
    pass


class TableNameColumn(column.Column):

    weight = 1
    header = u'Name'

    def renderCell(self, item):  # pragma: no cover
        return item.table_name


class TimestampColumn(column.Column):

    weight = 2
    header = u'Timestamp'

    def formatTimestamp(self, timestamp):
        tdt = datetime.fromtimestamp(timestamp) 
        return isodate.datetime_isoformat(tdt, isodate.DATE_EXT_COMPLETE)

    def renderCell(self, item):  # pragma: no cover
        timestamp = item.timestamp
        if timestamp is not None:
            return self.formatTimestamp(timestamp)


class TimestampsColumn(TimestampColumn):

    weight = 2
    header = u'Timestamps'

    def renderCell(self, item):  # pragma: no cover        
        timestamps = item.timestamps
        result = ['<select class="cb cb-sm comboBox" id="tms-%s">' % item.__name__]
        for timestamp in timestamps or ():
            result.append('<option value="%s">%s</option>' %
                          (timestamp, self.formatTimestamp(timestamp)))
        result.append('</select>')
        return ''.join(result) if timestamps else ''


class ArchiveColumn(column.Column):

    weight = 3

    def _archive_button(self, item):
        url = get_table_url(item, self.request) + '/@@archive'
        result = """
            <button type="button" class="btn btn-default btn-sm archiveButton" id="arc-%s" 
                    action_url="%s" data-toggle="modal" data-target="#archiveModal">
            %s</button>
        """ % (item.__name__, url, 'Archive')
        return result

    def renderCell(self, item):
        return self._archive_button(item) if item.timestamp is not None else ''


class UnarchiveColumn(column.Column):

    weight = 3

    def _unarchive_button(self, item):
        url = get_table_url(item, self.request) + '/@@unarchive'
        result = """
            <button type="button" class="btn btn-default btn-sm unArchiveButton" id="uar-%s"
                    action_url="%s" data-toggle="modal" data-target="#unArchiveModal">
            %s</button>
        """ % (item.__name__, url, 'Unarchive')
        return result

    def renderCell(self, item):
        return self._unarchive_button(item) if item.timestamps else ''


class ResetColumn(column.Column):

    weight = 4

    def _reset_button(self, item):
        url = get_table_url(item, self.request) + '/@@reset'
        result = """
            <button type="button" class="btn btn-default btn-sm resetButton" id="rst-%s"
                    action_url="%s" data-toggle="modal" data-target="#resetModal">
            %s</button>
        """ % (item.__name__, url, 'Reset')
        return result

    def renderCell(self, item):
        return self._reset_button(item) if item.timestamp is not None else ''
