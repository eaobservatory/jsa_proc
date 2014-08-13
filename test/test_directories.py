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
from jsa_proc.job_run.directories \
    import get_input_dir, get_output_dir, get_scratch_dir


class DirectoryTestCase(TestCase):
    def test_directories(self):
        self.assertEqual(get_input_dir(18),
                         '/net/kamaka/export/data/jsa_proc/input/18')

        self.assertEqual(get_output_dir(46),
                         '/net/kamaka/export/data/jsa_proc/output/46')

        self.assertEqual(get_scratch_dir(92),
                         '/export/data/jsa_proc/scratch/92')

        with self.assertRaises(JSAProcError):
            get_input_dir('not an integer')
