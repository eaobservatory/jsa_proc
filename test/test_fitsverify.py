# Copyright (C) 2015 Science and Technology Facilities Council.
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

from unittest import TestCase

from jsa_proc.action.fitsverify import valid_fits


class ValidFITSTestCase(TestCase):
    def test_valid_fits(self):
        # Allowing warnings.
        self.assertTrue(valid_fits('test/data/validfits.fits'))
        self.assertTrue(valid_fits('test/data/warningfits.fits'))
        self.assertFalse(valid_fits('test/data/invalidfits.fits'))

        # Not allowing warnings.
        self.assertTrue(valid_fits('test/data/validfits.fits', False))
        self.assertFalse(valid_fits('test/data/warningfits.fits', False))
        self.assertFalse(valid_fits('test/data/invalidfits.fits', False))
