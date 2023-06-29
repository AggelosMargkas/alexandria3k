#
# Alexandria3k Patent grant bibliographic metadata processing
# Copyright (C) 2023  Aggelos Margkas
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
"""Patent grant bibliographic (front page) text data (JAN 1976 - present)"""

import zipfile
import os
import re

from alexandria3k.data_source import (
    CONTAINER_INDEX,
    ROWID_INDEX,
    DataSource,
    ElementsCursor,
    StreamingCachedContainerTable,
)

from alexandria3k.xml import getter, agetter, all_getter
from alexandria3k.file_xml_cache import get_file_cache
from alexandria3k.db_schema import ColumnMeta, TableMeta


# Bulk data can be found here. https://bulkdata.uspto.gov
# Patent Grant Bibliographic (Front Page) Text Data (JAN 1976 - PRESENT)
DEFAULT_SOURCE = (
    "path/to/tests/data/April 2023 Patent Grant Bibliographic Data"
)

# Delimiter for extracting concatenated XML files.
XML_DELIMITER = '<?xml version="1.0" encoding="UTF-8"?>'

# Dataset Description — Patent grant full-text data (no images)
# JAN 1976 — present Automated Patent System (APS)
# Contains the full text of each patent grant issued weekly
# Tuesdays from January 1, 1976, to present excludes images/drawings and reexaminations.
# https://developer.uspto.gov/product/patent-grant-bibliographic-dataxml


class ErrorElement:
    """A class used for representing error elements"""

    def find(self, _path):
        """Placeholder to the actual XML tree element."""
        return None

    def findall(self, _path):
        """Placeholder to the actual XML tree element."""
        return None


class ZipFiles:
    """The source of the compressed XML data files"""

    def __init__(self, directory, sample_container):
        # Collect the names of all available data files
        self.file_path = []
        self.unique_patent_xml_files = []
        self.patent_file_name = []
        self.sample = sample_container
        pattern = r'file="([^"]+)"'

        # Read through the directory
        for file_name in os.listdir(directory):
            path = os.path.join(directory, file_name)
            # print(path)
            if not os.path.isfile(path):
                continue
            if not sample_container(path):
                continue

            # Collect the names of all available XML zip data files
            self.file_path.append(path)

        # pylint: disable-next=consider-using-with
        for uspto_record in self.file_path:
            # perf.log("Reading USPTO zip files...")
            # Open every US patent XML zip file
            with zipfile.ZipFile(uspto_record, "r") as zip_ref:
                # Pick only the XML file inside zip
                xml_file = [
                    file
                    for file in zip_ref.namelist()
                    if file.endswith(".xml")
                ]
                (self.file_name,) = xml_file

                # Get chunks of concatenated XML files
                with zip_ref.open(self.file_name, "r") as uspto_weekly_file:
                    # perf.log("Decoding and splitting weekly XML into chunks...")

                    xml_content = uspto_weekly_file.read().decode("utf-8")
                    xml_files = xml_content.split(XML_DELIMITER)

                    for self.xml_info in xml_files:
                        if len(self.xml_info) == 0:
                            continue

                        # Get container name through regex
                        match = re.search(pattern, self.xml_info[:200])
                        extracted_id = match.group(1)

                        self.patent_file_name.append(extracted_id)
                        self.unique_patent_xml_files.append(self.xml_info)

    def get_xml_files_array(self):
        """Return the array of data files"""
        return self.unique_patent_xml_files

    def get_container_iterator(self):
        """Return an iterator over the int identifiers of all data files"""
        return range(0, len(self.unique_patent_xml_files))

    def get_container_name(self, fid):
        """Return the name of the file corresponding to the specified fid"""
        return self.patent_file_name[fid]


class VTSource:
    """Virtual table data source.  This gets registered with the apsw
    Connection through createmodule in order to instantiate the virtual
    tables."""

    def __init__(self, data_directory, sample):
        self.data_files = ZipFiles(data_directory, sample)
        self.table_dict = {t.get_name(): t for t in tables}
        self.sample = sample

    def Create(self, _db, _module_name, _db_name, table_name):
        """Create the specified virtual table
        Return the tuple required by the apsw.Source.Create method:
        the table's schema and the virtual table class."""
        table = self.table_dict[table_name]
        # print(self.data_files)
        return table.table_schema(), StreamingCachedContainerTable(
            table,
            self.table_dict,
            self.data_files.get_xml_files_array(),
            self.sample,
        )

    Connect = Create

    def get_container_iterator(self):
        """Return an iterator over the data files identifiers"""
        return self.data_files.get_container_iterator()

    def get_container_name(self, fid):
        """Return the name of the file corresponding to the specified fid"""
        return self.data_files.get_container_name(fid)


class XMLChunkCursor:
    """A virtual table cursor over patents data.
    If it is used through data_source partitioned data access
    within the context of a ZipFiles iterator,
    it shall return the single element of the ZipFiles iterator.
    Otherwise it shall iterate over all elements."""

    def __init__(self, table):
        """Not part of the apsw VTCursor interface.
        The table argument is a StreamingTable object"""
        self.table = table
        self.eof = False
        # The following get initialized in Filter()
        self.file_index = None
        self.single_file = None
        self.file_read = None
        self.element_tree = None

    def Filter(self, index_number, _index_name, constraint_args):
        """Always called first to initialize an iteration to the first
        (possibly constrained) row of the table"""
        # print(f"Filter n={index_number} c={constraint_args}")
        if index_number == 0:
            # No index; iterate through all the files
            self.file_index = -1
            self.single_file = False
        elif index_number & CONTAINER_INDEX:
            self.single_file = True
            self.file_read = False
            self.file_index = constraint_args[0] - 1
        self.Next()

    def Next(self):
        """Advance reading to the next available file. Files are assumed to be
        non-empty."""
        if self.single_file and self.file_read:
            self.eof = True
            return
        if self.file_index + 1 >= len(self.table.data_source):
            print("xml chunks end. What happend now?")
            self.eof = True
            return
        self.file_index += 1
        # Get element tree of XML chunk
        # Use file_index to cache
        self.element_tree = get_file_cache().read(
            self.table.data_source[self.file_index], self.file_index
        )
        self.eof = False
        # The single file has been read. Set EOF in next Next call
        self.file_read = True

    def Rowid(self):
        """Return a unique id of the row along all records"""
        return self.file_index

    def current_row_value(self):
        """Return the current row. Not part of the apsw API."""
        return self.element_tree

    def Eof(self):
        """Return True when the end of the table's records has been reached."""
        return self.eof

    def Close(self):
        """Cursor's destructor, used for cleanup"""
        self.element_tree = None


class PatentsElementsCursor(ElementsCursor):
    """A cursor over USPTO elements."""

    def Next(self):
        """Advance to the next item."""
        while True:
            if self.parent_cursor.Eof():
                self.eof = True
                return
            if not self.elements:
                self.elements = self.extract_multiple(
                    self.parent_cursor.current_row_value()
                )
                self.element_index = -1
            if not self.elements:
                self.parent_cursor.Next()
                self.elements = None
                continue
            if self.element_index + 1 < len(self.elements):
                self.element_index += 1
                self.eof = False
                return
            self.parent_cursor.Next()
            self.elements = None


class PatentsCursor(PatentsElementsCursor):
    """A virtual table cursor over patents data.
    If it is used through data_source partitioned data access
    within the context of a ZipFiles iterator,
    it shall return the single element of the ZipFiles iterator.
    Otherwise it shall iterate over all elements."""

    def __init__(self, table):
        """Not part of the apsw VTCursor interface.
        The table argument is a StreamingTable object"""
        self.table = table
        self.files_cursor = XMLChunkCursor(table)
        # Initialized in Filter()
        self.eof = False
        self.item_index = -1
        self.single_file = None
        self.file_read = None
        self.iterator = None

    def Eof(self):
        """Return True when the end of the table's records has been reached."""
        return self.eof

    def container_id(self):
        """Return the id of the container containing the data being fetched.
        Not part of the apsw API."""
        return self.files_cursor.Rowid()

    def Rowid(self):
        """Return a unique id of the row along all records"""
        return self.item_index

    def Column(self, col):
        """Return the value of the column with ordinal col"""
        # print(f"Column {col}")
        if col == -1:
            return self.Rowid()

        if col == 0:  # id
            return self.files_cursor.Rowid()

        if col == 1:
            return self.files_cursor.Rowid()

        extract_function = self.table.get_value_extractor_by_ordinal(col)
        return extract_function(self.files_cursor.element_tree)

    def Filter(self, index_number, _index_name, constraint_args):
        """Always called first to initialize an iteration to the first row
        of the table according to the index"""
        self.files_cursor.Filter(index_number, _index_name, constraint_args)
        self.eof = self.files_cursor.Eof()
        if index_number & ROWID_INDEX:
            # This has never happened, so this is untested
            self.item_index = constraint_args[1]
        else:
            self.item_index = 0

    def Next(self):
        """Advance to the next item."""
        self.item_index = 0
        self.files_cursor.Next()
        self.eof = self.files_cursor.eof

    def current_row_value(self):
        """Return the current row. Not part of the apsw API."""
        return self.files_cursor.element_tree

    def Close(self):
        """Cursor's destructor, used for cleanup"""
        self.files_cursor.Close()


class PatentsIpcrCurcor(PatentsElementsCursor):
    """A cursor over any of a patent's details data."""

    def __init__(self, table, parent_cursor):
        """Not part of the apsw VTCursor interface.
        The table agument is a StreamingTable object"""
        super().__init__(table, parent_cursor)
        self.extract_multiple = table.get_table_meta().get_extract_multiple()

    def Rowid(self):
        """Return a unique id of the row along all records.
        This allows for 16k elements."""
        return (self.parent_cursor.Rowid() << 14) | self.element_index

    def Column(self, col):
        """Return the value of the column with ordinal col"""
        if col == 0:  # id
            return self.record_id()

        if col == 2:  # patent_id
            return self.parent_cursor.container_id()

        return super().Column(col)


tables = [
    TableMeta(
        "us_patents",
        cursor_class=PatentsCursor,
        columns=[
            ColumnMeta("id"),
            ColumnMeta("container_id"),
            ColumnMeta(
                "language",
                agetter("lang"),
                description="Fixed EN for publishing.",
            ),
            ColumnMeta(
                "status",
                agetter("status"),
                description="Not used for publishing.",
            ),
            ColumnMeta("country", agetter("country"), description="Fixed US."),
            ColumnMeta(
                "filename",
                agetter("file"),
                description="Filename for the specific date.",
            ),
            ColumnMeta("date_produced", agetter("date-produced")),
            ColumnMeta(
                "date_published",
                agetter("date-publ"),
            ),
            ColumnMeta(
                "type",
                agetter(
                    "appl-type",
                    "us-bibliographic-data-grant/application-reference",
                ),
            ),
            ColumnMeta(
                "series_code",
                getter(
                    "us-bibliographic-data-grant/us-application-series-code"
                ),
            ),
            ColumnMeta(
                "invention_title",
                getter("us-bibliographic-data-grant/invention-title"),
            ),
            ColumnMeta(
                "botanic_name",
                getter("us-bibliographic-data-grant/us-botanic/latin-name"),
            ),
            ColumnMeta(
                "botanic_variety",
                getter("us-bibliographic-data-grant/us-botanic/variety"),
            ),
            ColumnMeta(
                "claims_number",
                getter("us-bibliographic-data-grant/number-of-claims"),
            ),
            ColumnMeta(
                "figures_number",
                getter(
                    "us-bibliographic-data-grant/figures/number-of-figures"
                ),
                description="Excluded element figures-to-publish.",
            ),
            ColumnMeta(
                "drawings_number",
                getter(
                    "us-bibliographic-data-grant/figures/number-of-drawing-sheets"
                ),
            ),
            ColumnMeta(
                "microform_number",
                getter("lang"),
                description="UNCOMPLETEDOptical microform appendix.",
            ),
            ColumnMeta(
                "primary_examiner_firstname",
                getter(
                    "us-bibliographic-data-grant/examiners/primary-examiner/first-name"
                ),
            ),
            ColumnMeta(
                "primary_examiner_lastname",
                getter(
                    "us-bibliographic-data-grant/examiners/primary-examiner/last-name"
                ),
            ),
            ColumnMeta(
                "assistant_examiner_firstname",
                getter(
                    "us-bibliographic-data-grant/examiners/assistant-examiner/first-name"
                ),
            ),
            ColumnMeta(
                "assistant_examiner_lastname",
                getter(
                    "us-bibliographic-data-grant/examiners/assistant-examiner/last-name"
                ),
            ),
            ColumnMeta(
                "authorized_officer_firstname",
                getter(
                    "us-bibliographic-data-grant/authorized-officer/first-name"
                ),
            ),
            ColumnMeta(
                "authorized_officer_lastname",
                getter(
                    "us-bibliographic-data-grant/authorized-officer/last-name"
                ),
            ),
            ColumnMeta(
                "hague_filing_date",
                getter(
                    "us-bibliographic-data-grant/hague-agreement-data/international-filing-date"
                ),
            ),
            ColumnMeta(
                "hague_reg_pub_date",
                getter(
                    "us-bibliographic-data-grant/hague-agreement-data/"
                    + "international-registration-publication-date"
                ),
            ),
            ColumnMeta(
                "hague_reg_date",
                getter(
                    "us-bibliographic-data-grant/hague-agreement-data/"
                    + "international-registration-publication-date"
                ),
            ),
            ColumnMeta(
                "hague_reg_num",
                getter(
                    "us-bibliographic-data-grant/hague-agreement-data/"
                    + "international-registration-number"
                ),
            ),
            ColumnMeta(
                "sir_flag",
                agetter("sir-text", "us-bibliographic-data-grant/us-sir-flag"),
                description="Statutory invention registration flag.",
            ),
            ColumnMeta(
                "cpa_flag",
                agetter(
                    "grant-cpa-text",
                    "us-bibliographic-data-grant/us-issued-on-continued-prosecution-application",
                ),
                description="Continued prosecution application flag.",
            ),
            ColumnMeta(
                "rule47_flag",
                getter("us-bibliographic-data-grant/rule-47-flag"),
                description="Refused to execute the application.",
            ),
        ],
    ),
    TableMeta(
        "icpr_classifications",
        foreign_key="patent_id",
        parent_name="us_patents",
        primary_key="id",
        cursor_class=PatentsIpcrCurcor,
        extract_multiple=all_getter(
            "us-bibliographic-data-grant/classifications-ipcr/classification-ipcr"
        ),
        columns=[
            ColumnMeta("id"),
            ColumnMeta("container_id"),
            ColumnMeta("patent_id"),
            ColumnMeta(
                "ipc_date",
                getter("ipc-version-indicator/date"),
            ),
            ColumnMeta(
                "class_level",
                getter("classification-level"),
            ),
            ColumnMeta("section", getter("section")),
            ColumnMeta("class", getter("class")),
            ColumnMeta("subclass", getter("subclass")),
            ColumnMeta(
                "main_group",
                getter("main-group"),
            ),
            ColumnMeta("subgroup", getter("subgroup")),
            ColumnMeta("symbol_position", getter("symbol-position")),
            ColumnMeta("class_value", getter("classification-value")),
            ColumnMeta("action_date", getter("action-date/date")),
            ColumnMeta(
                "generating_office", getter("generating-office/country")
            ),
            ColumnMeta("class_status", getter("classification-status")),
            ColumnMeta("class_source", getter("classification-data-source")),
        ],
    ),
]


class Uspto(DataSource):
    """
    Create an object containing USPTO meta-data that supports queries over
    its (virtual) tables and the population of an SQLite database with its
    data.

    :param uspto_directory: The directory path where the USPTO
        data files are located
    :type uspto_directory: str

    :param sample: A callable to control container sampling, defaults
        to `lambda n: True`.
        The population or query method will call this argument
        for each USPTO container file with each container's file
        name as its argument.  When the callable returns `True` the
        container file will get processed, when it returns `False` the
        container will get skipped.
    :type sample: callable, optional

    :param attach_databases: A list of colon-joined tuples specifying
        a database name and its path, defaults to `None`.
        The specified databases are attached and made available to the
        query and the population condition through the specified database
        name.
    :type attach_databases: list, optional

    """

    def __init__(
        self,
        uspto_directory,
        sample=lambda n: True,
        attach_databases=None,
    ):
        super().__init__(
            VTSource(uspto_directory, sample),
            tables,
            attach_databases,
        )
