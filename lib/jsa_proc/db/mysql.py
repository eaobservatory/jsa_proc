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

from __future__ import absolute_import

import mysql.connector
from threading import Lock

from jsa_proc.db.db import JSAProcDB
from jsa_proc.error import JSAProcError


class JSAProcMySQLLock():
    """MySQL locking and cursor management class."""

    def __init__(self, conn):
        """Construct new locking object."""

        self._lock = Lock()
        self._conn = conn
        self._tables = None

        with self as c:
            result = []
            c.execute('SHOW TABLES')

            while True:
                row = c.fetchone()
                if row is None:
                    break

                (table,) = row

                result.append(table)

        self._tables = result

    def __enter__(self):
        """Context manager block entry method."""

        self._lock.acquire(True)

        # Make sure we still have an active connection to MySQL.
        self._conn.ping(reconnect=True, attempts=3, delay=5)

        self._cursor = self._conn.cursor()

        if self._tables is not None:
            self._cursor.execute(
                'LOCK TABLES ' +
                ', '.join((x + ' WRITE' for x in self._tables)))

        return self._cursor

    def __exit__(self, type_, value, tb):
        """Context manager block exit method."""

        if type_ is None:
            self._conn.commit()
        else:
            self._conn.rollback()

        if self._tables is not None:
            self._cursor.execute('UNLOCK TABLES')

        self._cursor.close()
        del self._cursor

        self._lock.release()

        # If we got a database-specific error, re-raise it as our
        # generic error.  Let other exceptions through unchanged.
        if type_ is not None and issubclass(type_, mysql.connector.Error):
            raise JSAProcError(str(value))

    def close(self):
        """Close the database connection."""

        self._conn.close()


class JSAProcMySQL(JSAProcDB):
    """JSA processing database MySQL database access class."""

    def __init__(self, config):
        """Construct MySQL access object.

        Takes as an argument the configuration object.
        """

        conn = mysql.connector.connect(
            host=config.get('database', 'host'),
            database=config.get('database', 'database'),
            user=config.get('database', 'user'),
            password=config.get('database', 'password'))

        self.db = JSAProcMySQLLock(conn)

        JSAProcDB.__init__(self)

    def __del__(self):
        """Destroy MySQL access object."""

        self.db.close()

    def job_prev_next(self, job_id,
                      state=None, location=None, task=None, qa_state=None,
                      tag=None,
                      prioritize=False, sort=False, sortdir='ASC',
                      obsquery=None, tiles=None):
        """MySQL-specific previous and next job query.

        Return: a tuple of the previous and next job identifiers.
        """

        # Prepare the same kind of query which find_jobs would use, but
        # select only the job identifier.
        (where, param) = self._find_jobs_where(
            state, location, task, qa_state, tag, obsquery, tiles)

        order = self._find_jobs_order(prioritize, sort, sortdir)

        find_query = 'SELECT id FROM job'
        if where:
            find_query += ' WHERE ' + ' AND '.join(where)

        if order:
            find_query += ' ORDER BY ' + ', '.join(order)

        # Now create the query to get the next and previous entries.  This
        # is done in a MySQL-specific manner by using user-defined variables
        # to add the previous row's job identifer to the result set.  MySQL
        # tends to processes these variables just before the row is sent, so
        # handle them in the WHERE clause to prevent them being out of sync
        # between the SELECT and WHERE parts of the query.  If this stops
        # working then the alternative would be to use a second "derived
        # table" (a.k.a. "inline view") to make the rows containing the
        # previous identifier, and then to select from it in order to apply
        # the WHERE constraints.  Useful links for user-defined variables are:
        # http://dev.mysql.com/doc/refman/5.1/en/user-variables.html
        # http://code.openark.org/blog/mysql/sql-ranking-without-self-join-revisited
        # http://www.xaprb.com/blog/2006/12/15/advanced-mysql-user-variable-techniques/
        query = 'SELECT found.id, @prevprev ' \
                'FROM (' + find_query + ') AS found ' \
                'WHERE COALESCE(NULL + @prevprev := @prevjob, ' \
                'NULL + @prevjob := found.id, ' \
                '@prevprev = %s OR found.id = %s)'

        param.extend((job_id, job_id))

        prev = next = None

        with self.db as c:
            # Initialize the variables with the type we want them to be,
            # i.e. integer --  so we have to then detect 0 later.
            c.execute('SET @prevjob = 0, @prevprev = 0')
            c.execute(query, param)
            while True:
                row = c.fetchone()
                if row is None:
                    break

                (row_id, row_prev) = row

                if row_id == job_id:
                    prev = None if row_prev == 0 else row_prev

                elif row_prev == job_id:
                    next = row_id

                else:
                    raise JSAProcError('Unexpected result in job_prev_next')

        return (prev, next)
