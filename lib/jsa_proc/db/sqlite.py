# Copyright (C) 2014 Science and Technology Facilities Council.
# copyright (C) 2017 East Asian Observatory.
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

from jsa_proc.db.db import JSAProcDB
from jsa_proc.error import JSAProcError

# By default, sqlite3 only recognizes "TIMESTAMP" columns as containing
# datetimes.  Therefore copy the converter it installs for this column type
# to also be used for "DATETIME" columns.
sqlite3.register_converter('DATETIME', sqlite3.converters['TIMESTAMP'])


def add_types(query):
    """Add type information where needed for SQLite.

    For expressions in SELECT queries, setting sqlite3.PARSE_DECLTYPES
    is insufficient to allow the SQLite module to determine the type
    of the result.  Therefore we need to set sqlite3.PARSE_COLNAMES
    and include a type in the result column name.

    This function makes substitutions for expressions where this
    is known to happen in the JSAProcDB class.
    """

    query = re.sub('((MIN|MAX)\(([a-z]+\.)?datetime\)) AS ([a-z]+)',
                   '\\1 AS "\\2 [timestamp]"', query)

    return query


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
        query = add_types(query)
        return sqlite3.Cursor.execute(self, query, *args, **kwargs)


class AtCursor(sqlite3.Cursor):
    """Custom SQLite cursor class.

    This is a custom subclass of sqlite3.Cursor which
    aims to improve compatability with Sybase.
    """

    def execute(self, query, *args, **kwargs):
        # We really need to get the list of placeholders so we can extract
        # the values from the dictionary in the right order.  However for
        # now assume there are either 0 or 1 placeholders, in which case
        # we can simply replace the placeholder and use the single argument
        # if present.
        query = re.sub('\@[a-z]+', '?', query)
        query = add_types(query)
        if args:
            args = (args[0].values(),)
            if len(args) > 1:
                raise Exception('This compatability class can not yet '
                                'handle multiple placeholders')
        return sqlite3.Cursor.execute(self, query, *args, **kwargs)


class JSAProcSQLiteLock():
    """SQLite locking and cursor management class."""

    def __init__(self, conn, paramstyle='format'):
        """Construct new locking object."""

        self._lock = Lock()
        self._conn = conn
        self.paramstyle = paramstyle

    def __enter__(self):
        """Context manager block entry method.

        This waits to acquire the lock and then provides a
        database cursor.
        """

        self._lock.acquire(True)
        if self.paramstyle == 'format':
            self._cursor = self._conn.cursor(FormatCursor)
        elif self.paramstyle == 'at':
            self._cursor = self._conn.cursor(AtCursor)
        else:
            raise Exception('Unknown paramstyle {0}'.format(self.paramstyle))
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

        # If we got a database-specific error, re-raise it as our
        # generic error.  Let other exceptions through unchanged.
        if type_ is not None and issubclass(type_, sqlite3.Error):
            raise JSAProcError(str(value))

    def close(self):
        """Close the database connection."""

        self._conn.close()

    def unlock(self):
        """ Does nothing in sqlite? """
        pass


class JSAProcSQLite(JSAProcDB):
    """JSA Processing database SQLite database access class."""

    def __init__(self, filename, file_already_exists=True):
        """Construct SQLite access object.

        Opens the specified SQLite database file and prepares
        it for use.  Creates an attribute "db" which is an instance
        of the JSAProcSQLiteLock class -- this should be used as
        a context manager to acquire a database cursor whenever
        the database is to be accessed.
        """

        if file_already_exists:
            if filename != ':memory:' and 'mode=memory' not in filename and not os.path.exists(filename):
                raise Exception('SQLite file ' + filename + ' not found')

        conn = sqlite3.connect(
            filename, check_same_thread=False,
            detect_types=(sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES))

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
