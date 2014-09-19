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

from jsa_proc.db.sybase import JSAProcSybaseLock
from omp.siteconfig import get_omp_siteconfig

cadc_dp_server = 'CADC_ASE'
cadc_dp_db = 'data_proc'

CADCDPInfo = namedtuple('CADCDPInfo', 'id state tag parameters priority')

jsa_tile_recipes = (
    'REDUCE_SCAN_JSA_PUBLIC',
    'REDUCE_SCIENCE_LEGACY',
)


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

        self.db = JSAProcSybaseLock(conn)
        self.recipe = None

    def __del__(self):
        """Destroy CADC DP database object.

        Disconnects from the database.
        """

        self.db.close()

    def get_recipe_info(self, tag_pattern=None):
        """Fetch info for all JSA recipes in the CADC DP database.

        Returns a list of CADCDPInfo named tuples.

        By default, fetch JSA HEALPix tile generation recipe
        instances, based on the recipe name in the parameters
        column.  Otherwise, if a tag pattern is specified,
        fetch recipes matching that pattern.
        """

        if self.recipe is None:
            self._determine_jsa_recipe()

        result = []

        if tag_pattern is None:
            # By default, import the HEALPix tile generation
            # recipe instances.
            where = '(' + ' OR '.join((
                'parameters LIKE "%-drparameters=\'' + x +
                '\'%"' for x in jsa_tile_recipes)) + ')'
            param = {}

        else:
            # Otherwise use the specified pattern.
            where = 'tag LIKE @t'
            param = {'@t': tag_pattern}

        with self.db as c:
            c.execute('SELECT identity_instance_id, state, tag, '
                      'parameters, priority '
                      'FROM dp_recipe_instance '
                      'WHERE recipe_id IN (' +
                      ', '.join((str(x) for x in self.recipe)) + ') '
                      'AND ' + where,
                      param)

            while True:
                row = c.fetchone()
                if row is None:
                    break

                # The Sybase module gives us back its own "NumericType" which
                # it can't even read itself.  Therefore convert to integer.
                result.append(CADCDPInfo(int(row[0]), *row[1:]))

        return result

    def get_recipe_input_files(self, id_):
        """Fetch the list of input files for a particular recipe instance.
        """

        result = []

        with self.db as c:
            c.execute('SELECT dp_input FROM dp_file_input '
                      'WHERE input_role="infile" '
                      'AND identity_instance_id=@i',
                      {'@i': id_})

            while True:
                row = c.fetchone()
                if row is None:
                    break

                (uri,) = row

                assert(uri.startswith('ad:JCMT/'))
                result.append(uri[8:])

        return result

    def get_recipe_output_files(self, id_):
        """Fetch the list of output files for a particular recipe instance.
        """

        result = []

        with self.db as c:
            c.execute('SELECT dp_output FROM dp_recipe_output '
                      'WHERE identity_instance_id=@i',
                      {'@i': id_})

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
