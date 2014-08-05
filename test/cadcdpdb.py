# Copyright (C) 2014 Science and Technology Facilities Council.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sqlite3
from unittest import TestCase

from jsa_proc.cadc.dpdb import CADCDP
from jsa_proc.db.sqlite import JSAProcSQLiteLock

schema = """
"""


class DummyCADCDP(CADCDP):
    def __init__(self):
        conn = sqlite3.connect(':memory:', check_same_thread=False)

        c = conn.cursor()
        c.execute('PRAGMA foreign_keys = ON')
        c.close()

        self.db = JSAProcSQLiteLock(conn)


class CADCDPDBTestCase(TestCase):
    """Base test case class for tests using a dummy CADC DP database.
    """

    def setUp(self):
        """Prepare for testing by creating an in-memory
        SQLite database from the test schema.
        """

        self.db = DummyCADCDP()

        with self.db.db as c:
            c.executescript(schema)

    def tearDown(self):
        """Disconnect from the database by deleting the
        CADC dummy DP database object.
        """

        del self.db
