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

import Sybase
from threading import Lock

from jsa_proc.omp.siteconfig import get_omp_siteconfig

cadc_dp_server = 'CADC_ASE'
cadc_dp_db = 'data_proc'


class CADCDPLock:
    """CADC DP database lock and cursor management class.
    """

    def __init__(self, conn):
        """Construct object."""

        self._lock = Lock()
        self._conn = conn

    def __enter__(self):
        """Context manager block entry method.

        Acquires the lock and provides a cursor.
        """

        self._lock.acquire(True)
        self._cursor = self._conn.cursor()
        return self._cursor

    def __exit__(self, type, value, tb):
        """Context manager  block exit method.

        Closes the cursor and releases the lock.  Since this module
        is intended for read access only, it does not attempt to
        commit a transaction.
        """

        self._cursor.close()
        del self._cursor

        self._lock.release()

    def close(self):
        """Close the database connection."""

        self._conn.close()


class CADCDP:
    """CADC DP database access class.
    """

    def __init__(self):
        """Construct new CADC DP database object.

        Connects to the CADC data_proc database table.
        """

        config = get_omp_siteconfig()

        conn = Sybase.connect(
            cadc_dp_server,
            config.get('cadc_dp', 'user'),
            config.get('cadc_dp', 'password'),
            database=cadc_dp_db,
            auto_commit=0)

        self.db = CADCDPLock(conn)

    def __del__(self):
        """Destroy CADC DP database object.

        Disconnects from the database.
        """

        self.db.close()
