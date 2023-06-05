#
# Alexandria3k Crossref bibliographic metadata processing
# Copyright (C) 2022-2023  Diomidis Spinellis
# SPDX-License-Identifier: GPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
"""Populate subject, journal, funder, OA data tables"""

import codecs
import csv
import sqlite3

from alexandria3k.common import (
    data_from_uri_provider,
    ensure_table_exists,
    get_string_resource,
)
from alexandria3k.data_source import SINGLE_PARTITION_INDEX
from alexandria3k import perf
from alexandria3k.virtual_db import ColumnMeta, TableMeta, StreamingTable


# Method names coming from apsw start with uppercase
# pylint: disable=invalid-name


class VTSource:
    """Virtual table data source for a single file.
    This gets registered with the apsw Connection through createmodule
    in order to instantiate the virtual table."""

    def __init__(self, table, data_source, sample):
        self.data_source = data_source
        self.sample = sample
        self.table_dict = {table.get_name(): table}

    def get_container_iterator(self):
        """Return an iterator over the int identifiers of all data files"""
        return [SINGLE_PARTITION_INDEX]

    def get_container_name(self, _fid):
        """Return the name of the file corresponding to the specified fid"""
        return self.data_source

    def Create(self, _db, _module_name, _db_name, table_name):
        """Create the specified virtual table by creating its schema
        and an apsw table (a StramingTable instance) streaming over its data"""
        table = self.table_dict[table_name]
        return table.table_schema(), StreamingTable(
            table, self.table_dict, self.data_source
        )

    Connect = Create


class CsvCursor:
    """A virtual table cursor over CSV data."""

    def __init__(self, table):
        """Not part of the apsw VTCursor interface.
        The table agument is a StreamingTable object"""
        self.table = table
        # Initialized in Filter()
        self.eof = False
        self.item_index = -1
        # Set in Next
        self.row_value = None
        # Set in Filter
        self.raw_input = None
        self.reader = None

    def Eof(self):
        """Return True when the end of the table's records has been reached."""
        return self.eof

    def Rowid(self):
        """Return a unique id of the row along all records"""
        return self.item_index

    def Column(self, col):
        """Return the value of the column with ordinal col"""
        if col == -1:
            return self.Rowid()

        if col == 0:  # id
            return self.Rowid()

        extract_function = self.table.get_value_extractor_by_ordinal(col)
        if extract_function:
            return extract_function(self.row_value)
        return self.row_value[col - 1]

    def Filter(self, _index_number, _index_name, _constraint_args):
        """Always called first to initialize an iteration to the first row
        of the table according to the index"""
        # print("FILTER", index_number, constraint_args)
        self.eof = False
        self.item_index = -1
        self.raw_input = data_from_uri_provider(self.table.data_source)
        self.reader = csv.reader(
            codecs.iterdecode(self.raw_input, "utf-8"),
            delimiter=self.table.get_table_meta().delimiter,
        )
        next(self.reader, None)  # Skip header row
        self.Next()  # Move to first row

    def Next(self):
        """Advance to the next item."""
        self.row_value = next(self.reader, None)
        if not self.row_value:
            self.eof = True
        else:
            self.item_index += 1

    def Close(self):
        """Cursor's destructor, used for cleanup"""
        self.raw_input.close()


works_asjcs_table = TableMeta(
    "works_asjcs",
    columns=[
        ColumnMeta("work_id"),
        ColumnMeta("asjc_id"),
    ],
)

tables = [
    works_asjcs_table,
]


def link_works_asjcs(database_path):
    """
    Create a many to many table linking works with Scopus
    All Science Journal Classification Codes — ASJCs.

    :param database_path: The path specifying the SQLite database
        to populate.
    :type database_path: str
    """
    connection = sqlite3.connect(database_path)
    ensure_table_exists(connection, "work_subjects")
    ensure_table_exists(connection, "asjcs")
    script = get_string_resource("sql/link-works-asjcs.sql")
    cursor = connection.cursor()
    cursor.executescript(script)
    perf.log("link_works_asjcs")
