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

from jsa_proc.cadc.tap import CADCTap, _partition_list
from jsa_proc.error import JSAProcError


class PartitionListTestCase(TestCase):
    def test_partition(self):
        self.assertEqual(_partition_list(['x', 'y', 'z'], 5),
                         [['x', 'y', 'z']])

        self.assertEqual(_partition_list([], 10), [[]])

        self.assertEqual(_partition_list(list(range(0, 10)), 10),
                         [list(range(0, 10))])

        self.assertEqual(_partition_list(list(range(0, 25)), 10),
                         [list(range(0, 10)), list(range(10, 20)),
                          list(range(20, 25))])


class CADCTapTestCase(TestCase):
    def test_pattern(self):
        caom2 = CADCTap()

        self.assertEqual(caom2._obsid_pattern('scuba2_00042_20120401t101010'),
                         'scuba2_%_20120401t%')

        with self.assertRaises(JSAProcError):
            caom2._obsid_pattern('scuba3_00042_20140401t101010')
