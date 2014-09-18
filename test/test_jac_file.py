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

from jsa_proc.error import JSAProcError
from jsa_proc.jac.file import get_jac_data_dir


class JACFileTestCase(TestCase):
    def test_data_dir(self):
        self.assertEqual(
            get_jac_data_dir('s4a20140401_00001_0001'),
            '/jcmtdata/raw/scuba2/s4a/20140401/00001')
        self.assertEqual(
            get_jac_data_dir('a20140910_00001_01_0001'),
            '/jcmtdata/raw/acsis/spectra/20140910/00001')
        self.assertEqual(
            get_jac_data_dir('a19970424_00017_04_0001'),
            '/jcmtdata/raw/acsis-das/converted/1997/19970424/00017')

        with self.assertRaises(JSAProcError):
            get_jac_data_dir('s7q20140401_00001_0001')
        with self.assertRaises(JSAProcError):
            get_jac_data_dir('ab20140910_00001_01_0001')
