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

from .db import DBTestCase

class BasicDBTest(DBTestCase):
    """Perform basic low-level tests of the database system."""

    def test_tables(self):
        """Test that the database contains the expected tables."""

        with self.db.db as c:
            tables = set()
            for (name,) in c.execute('SELECT name FROM sqlite_master '
                          'WHERE type="table"'):
                if not name.startswith('sqlite'):
                    tables.add(name)

        self.assertEqual(tables, set(('job',)))
