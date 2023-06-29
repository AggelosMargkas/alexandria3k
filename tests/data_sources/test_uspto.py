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
"""USPTO import integration tests"""

import os
import sqlite3

from ..test_dir import add_src_dir, td

add_src_dir()

from ..common import PopulateQueries
from alexandria3k.data_sources import uspto
from alexandria3k.file_xml_cache import FileCache
from alexandria3k import debug


DATABASE_PATH = td("tmp/uspto.db")


class TestUsptoPopulate(PopulateQueries):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(DATABASE_PATH):
            os.unlink(DATABASE_PATH)

        FileCache.file_reads = 0
        cls.uspto = uspto.Uspto(
            td("data/April 2023 Patent Grant Bibliographic Data")
        )
        cls.uspto.populate(DATABASE_PATH)
        cls.con = sqlite3.connect(DATABASE_PATH)
        cls.cursor = cls.con.cursor()

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        os.unlink(DATABASE_PATH)

    def test_import(
        self,
    ):
        result = TestUsptoPopulate.cursor.execute(
            f"SELECT Count(*) from us_patents"
        )
        (count,) = result.fetchone()
        self.assertEqual(count, 10)

    def test_type(
        self,
    ):
        result = TestUsptoPopulate.cursor.execute(
            f"""SELECT type FROM us_patents
            WHERE filename='USPP034694-20221025.XML'"""
        )
        (type,) = result.fetchone()
        self.assertEqual(type, "plant")

    def test_primary_examiner(
        self,
    ):
        result = TestUsptoPopulate.cursor.execute(
            f"""SELECT primary_examiner_firstname, primary_examiner_lastname FROM us_patents
            WHERE filename='USRE049258-20221025.XML'"""
        )
        (name, lastname) = result.fetchone()
        self.assertEqual(name, "Catherine M")
        self.assertEqual(lastname, "Tarae")

    def test_counts(self):
        self.assertEqual(self.record_count("us_patents"), 10)
        self.assertEqual(self.record_count("icpr_classifications"), 24)
        self.assertEqual(FileCache.parse_counter, 10)

        self.assertEqual(
            self.record_count(
                """(SELECT DISTINCT type
          FROM us_patents)"""
            ),
            4,
        )

        self.assertEqual(
            self.record_count(
                """(SELECT DISTINCT primary_examiner_firstname
          FROM us_patents)"""
            ),
            8,
        )


class TestUsptoPopulateMasterCondition(PopulateQueries):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(DATABASE_PATH):
            os.unlink(DATABASE_PATH)

        debug.set_flags(["sql", "dump-matched"])

        FileCache.file_reads = 0
        cls.uspto = uspto.Uspto(
            td("data/April 2023 Patent Grant Bibliographic Data")
        )
        cls.uspto.populate(DATABASE_PATH, None, "type = 'plant'")
        cls.con = sqlite3.connect(DATABASE_PATH)
        cls.cursor = cls.con.cursor()

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        os.unlink(DATABASE_PATH)

    def test_counts(self):
        self.assertEqual(self.record_count("us_patents"), 1)
        self.assertEqual(self.record_count("icpr_classifications"), 2)
        self.assertEqual(FileCache.parse_counter, 10)
