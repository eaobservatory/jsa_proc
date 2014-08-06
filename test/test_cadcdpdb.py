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

from .cadcdpdb import CADCDPDBTestCase


class BasicCADCPDPDBTest(CADCDPDBTestCase):
    def test_recipe(self):
        """Test retrieval of the JSA recipe numbers.
        """

        self.db._determine_jsa_recipe()

        self.assertEqual(self.db.recipe, set((2, 3, 4)))

    def test_recipe_instance(self):
        """Test retrieval of JSA recipe information.
        """

        info = self.db.get_recipe_info()

        expect = {
            1001: ('Q', 'hpx-1001-850um'),
            1002: ('Q', 'hpx-1002-850um'),
            1004: ('N', 'hpx-1004-850um'),
            1006: ('Y', 'hpx-1006-850um'),
            1007: ('E', 'hpx-1007-850um'),
        }

        for job in info:
            self.assertIn(job.id_, expect)

            (state, tag) = expect.pop(job.id_)

            self.assertEqual(job.state, state)
            self.assertEqual(job.tag, tag)
            self.assertRegexpMatches(job.parameters,
                                     "-drparameters='[A-Z_]*'")
