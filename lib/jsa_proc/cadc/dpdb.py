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

from collections import namedtuple
import Sybase
from threading import Lock

from jsa_proc.omp.siteconfig import get_omp_siteconfig

cadc_dp_server = 'CADC_ASE'
cadc_dp_db = 'data_proc'

CADCDPInfo = namedtuple('CADCDPInfo', 'id_ state tag parameters')

jsa_tile_recipes = (
    'REDUCE_SCAN_JSA_PUBLIC',
    'REDUCE_SCIENCE_LEGACY',
)


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

    def __exit__(self, type_, value, tb):
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
        self.recipe = None

    def __del__(self):
        """Destroy CADC DP database object.

        Disconnects from the database.
        """

        self.db.close()

    def get_recipe_info(self):
        """Fetch info for all JSA recipes in the CADC DP database.

        Returns a list of CADCDPInfo named tuples.
        """

        if self.recipe is None:
            self._determine_jsa_recipe()

        result = []

        with self.db as c:
            c.execute('SELECT identity_instance_id, state, tag, parameters '
                      'FROM dp_recipe_instance '
                      'WHERE recipe_id IN (' +
                          ', '.join((str(x) for x in self.recipe)) + ') '
                      'AND (' +
                          ' OR '.join((
                              'parameters LIKE "%-drparameters=\'' + x +
                              '\'%"' for x in jsa_tile_recipes)) + ')')

            while True:
                row = c.fetchone()
                if row is None:
                    break

                result.append(CADCDPInfo(*row))

        return result

    def get_recipe_input_files(self, id_):
        """Fetch the list of input files for a particular recipe instance.
        """

        result = []

        with self.db as c:
            c.execute('SELECT dp_input FROM dp_file_input '
                      'WHERE input_role="infile" '
                      'AND identity_instance_id=@i',
                      {'i': id_})

            while True:
                row = c.fetchone()
                if row is None:
                    break

                (uri,) = row

                assert(uri.startswith('ad:JCMT/'))
                result.append(uri[8:])

        return result

    def _determine_jsa_recipe(self):
        """Fetch the list of JSA recipes from the CADC database.

        The resultant set of recipes is stored in the object.
        """

        recipe = set()

        with self.db as c:
            c.execute('SELECT recipe_id FROM dp_recipe '
                      'WHERE project="JCMT_JAC" '
                      'AND script_name="jsawrapdr"')

            while True:
                row = c.fetchone()
                if row is None:
                    break

                (id_,) = row

                recipe.add(id_)

        self.recipe = recipe
