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

from jsa_proc.admin.directories \
    import get_input_dir, get_output_dir, get_scratch_dir, get_log_dir
from jsa_proc.error import JSAProcError


class DirectoryTestCase(TestCase):
    def test_directories(self):
        # Test all the directory functions.
        self.assertEqual(
            get_input_dir(18),
            '/net/kamaka/export/data/jsa_proc/input/000/000000/000000018')

        self.assertEqual(
            get_output_dir(46),
            '/net/kamaka/export/data/jsa_proc/output/000/000000/000000046')

        self.assertEqual(
            get_scratch_dir(92),
            '/export/data/jsa_proc/scratch/000/000000/000000092')

        self.assertEqual(
            get_log_dir(844),
            '/net/kamaka/export/data/jsa_proc/log/000/000000/000000844')

        # Test longer job ID numbers (we know all the functions use the same
        # private function to prepare the decimal number internally).
        self.assertEqual(
            get_log_dir(123456789),
            '/net/kamaka/export/data/jsa_proc/log/123/123456/123456789')

        self.assertEqual(
            get_log_dir(22333),
            '/net/kamaka/export/data/jsa_proc/log/000/000022/000022333')

        self.assertEqual(
            get_log_dir(22333999),
            '/net/kamaka/export/data/jsa_proc/log/022/022333/022333999')

        # Test what happens with a billion or more job IDs.
        self.assertEqual(
            get_log_dir(1999000999),
            '/net/kamaka/export/data/jsa_proc/log/1999/1999000/1999000999')

        with self.assertRaises(JSAProcError):
            get_input_dir('not an integer')
