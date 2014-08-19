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
            1001: ('Q', 'hpx-1001-850um', -200),
            1002: ('Q', 'hpx-1002-850um', -300),
            1004: ('N', 'hpx-1004-850um', -1),
            1006: ('Y', 'hpx-1006-850um', -750),
            1007: ('E', 'hpx-1007-850um', -250),
        }

        for job in info:
            self.assertIn(job.id, expect)

            (state, tag) = expect.pop(job.id)

            self.assertEqual(job.state, state)
            self.assertEqual(job.tag, tag)
            self.assertRegexpMatches(job.parameters,
                                     "-drparameters='[A-Z_]*'")

    def test_recipe_input_files(self):
        """Test retrieval of recipe file list.
        """

        files = self.db.get_recipe_input_files(1004)

        self.assertEqual(sorted(files),
            ['s4a20140401_00051_0001', 's4a20140401_00052_0001'])

    def test_recipe_output_files(self):
        """Test retrieval of recipe output file list.
        """

        files = self.db.get_recipe_output_files(1006)
        self.assertEqual(sorted(files), [
                'reduced_1006_850.fits',
                'reduced_1006_850_preview.png'])

        files = self.db.get_recipe_output_files(1007)
        self.assertEqual(sorted(files), [
                'reduced_1007_850.fits',
                'reduced_1007_850_preview.png'])
