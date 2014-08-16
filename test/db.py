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

from unittest import TestCase

from jsa_proc.db.sqlite import JSAProcSQLite

schema = None


def create_dummy_database():
    """Create an in-memory SQLite database from the schema."""

    global schema

    if schema is None:
        with open('doc/schema.sql') as f:
            schema = f.read()

    db = JSAProcSQLite(':memory:')

    with db.db as c:
        c.executescript(schema)

    return db


class DBTestCase(TestCase):
    """Base test case class for tests using the database.
    """

    def setUp(self):
        """Prepare for testing by creating an in-memory
        SQLite database from the schema file.
        """

        self.db = create_dummy_database()

    def tearDown(self):
        """Disconnect from the database by deleting the
        JSAProcDB object.
        """

        del self.db
