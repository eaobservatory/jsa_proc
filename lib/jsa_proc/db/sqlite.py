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

import os
import re
import sqlite3
from threading import Lock

from .db import JSAProcDB


class FormatCursor(sqlite3.Cursor):
    """Custom SQLite cursor class.

    This is a custom subclass of sqlite3.Cursor which
    aims to improve compatability with MySQL.
    """

    def execute(self, query, *args, **kwargs):
        """
        Overridden execute method.

        Replaces format style parameter placeholders (%s) with question
        marks (?).  This allows queries intended for MySQL to be used
        with SQLite.
        """
        query = re.sub('\%s', '?', query)
        return sqlite3.Cursor.execute(self, query, *args, **kwargs)


class JSAProcSQLiteLock():
    """SQLite locking and cursor management class."""

    def __init__(self, conn):
        """Construct new locking object."""

        self._lock = Lock()
        self._conn = conn

    def __enter__(self):
        """Context manager block entry method.

        This waits to acquire the lock and then provides a
        database cursor.
        """

        self._lock.acquire(True)
        self._cursor = self._conn.cursor(FormatCursor)
        return self._cursor

    def __exit__(self, type_, value, tb):
        """Context manager block exit method.

        If the block exited cleanly, commit, otherwise rollback
        the current transaction.  Also closes the cursor object.
        """

        if type_ is None:
            self._conn.commit()
        else:
            self._conn.rollback()

        self._cursor.close()
        del self._cursor

        self._lock.release()

    def close(self):
        """Close the database connection."""

        self._conn.close()


class JSAProcSQLite(JSAProcDB):
    """JSA Processing database SQLite database access class."""

    def __init__(self, filename):
        """Construct SQLite access object.

        Opens the specified SQLite database file and prepares
        it for use.  Creates an attribute "db" which is an instance
        of the JSAProcSQLiteLock class -- this should be used as
        a context manager to acquire a database cursor whenever
        the database is to be accessed.
        """

        if filename != ':memory:' and not os.path.exists(filename):
            raise Exception('SQLite file ' + filename + ' not found')

        conn = sqlite3.connect(filename, check_same_thread=False)

        c = conn.cursor()
        c.execute('PRAGMA foreign_keys = ON')
        c.close()

        self.db = JSAProcSQLiteLock(conn)

        JSAProcDB.__init__(self)

    def __del__(self):
        """Destroy SQLite access object.

        Disconnects from the database.
        """

        self.db.close()
