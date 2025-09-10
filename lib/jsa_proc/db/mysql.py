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
                ', '.join([x + ' WRITE' for x in self._tables] #+ 
                          #[x + ' READ' for x in self._readonlytables]
                      ))



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


    def unlock(self):
        """ UNLOCK tables """
        self._cursor.execute('UNLOCK TABLES')

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
                      obsquery=None, tiles=None, parameters=None):
        """MySQL-specific previous and next job query.

        Return: a tuple of the previous and next job identifiers.
        """

        # Prepare the same kind of query which find_jobs would use.
        (where, param) = self._find_jobs_where(
            state, location, task, qa_state, tag, obsquery, tiles, parameters)

        order = self._find_jobs_order(prioritize, sort, sortdir)

        if where:
            where_query = 'WHERE ' + ' AND '.join(where)
        else:
            where_query = ''

        if order:
            order_query = 'ORDER BY ' + ', '.join(order)
        else:
            order_query = 'ORDER BY job.id ASC'

        # Now create the query to get the next and previous entries.  This
        # is done in using the LAG and LEAD windowing functions and then
        # an outer query to select the required row.
        query = 'SELECT id_prev, id_next FROM ' \
                '(SELECT id, LAG(id) OVER w AS id_prev, LEAD(id) OVER w AS id_next ' \
                'FROM job ' + where_query + ' WINDOW w AS (' + order_query + ')) ' \
                'AS prev_next WHERE id = %s'

        param.append((job_id))

        prev = next_ = None

        with self.db as c:
            if 'jcmt.COMMON' in query:
                c.execute('UNLOCK TABLES')
            c.execute(query, param)
            while True:
                row = c.fetchone()
                if row is None:
                    break

                (prev, next_) = row

        return (prev, next_)
