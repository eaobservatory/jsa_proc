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

from jsa_proc.action.datafile_handling import valid_hds
from jsa_proc.action.datafile_handling import assemble_input_data_for_job
from jsa_proc.action.datafile_handling import get_output_files
from jsa_proc.action.datafile_handling import filter_file_list


class ValidHDSTestCase(TestCase):
    def test_valid_hds(self):
        self.assertTrue(valid_hds('test/data/validhds.sdf'))
        self.assertFalse(valid_hds('test/data/invalidhds.sdf'))

class FileHandingTest(TestCase):
    def test_filter_file_list(self):
        testfilelist = ['1.sdf', '2.fits',
                        '3.png', 'jcmts20140925_00018_850_reduced001_nit_000.fits']

        # Check it does something apppropriate
        self.assertEqual(['3.png'],filter_file_list(testfilelist, '\.png$'))

        self.assertEqual(['2.fits',
                          'jcmts20140925_00018_850_reduced001_nit_000.fits'],
                         filter_file_list(testfilelist, '\.fits$'))
