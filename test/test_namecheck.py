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

from unittest import TestCase

from jsa_proc.cadc.namecheck import check_file_name, _get_namecheck_pattern


class NamecheckTestCase(TestCase):
    def test_parse_config(self):
        pattern = _get_namecheck_pattern()

        # Did we get a list?
        self.assertIsInstance(pattern, list)

        # Does it have entries in it?
        self.assertGreater(len(pattern), 0)

    def test_check_file_name(self):
        self.assertTrue(check_file_name(
            'jcmth20070101_00062_01_reduced001_nit_000.fits'))
        self.assertFalse(check_file_name(
            'jcmth20070101_00062_01_reduced001_not_000.fits'))
