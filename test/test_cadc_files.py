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

from jsa_proc.cadc.files import CADCFiles
from jsa_proc.error import JSAProcError


class CADCFilesTestCase(TestCase):
    def test_pattern(self):
        ad = CADCFiles()

        # SCUBA-2 raw
        self.assertEqual(ad._filename_pattern(
            's8d20140401_00042_0001'),
            's8d20140401_00042_%')

        # SCUBA-2 reduced
        self.assertEqual(ad._filename_pattern(
            'jcmts20140910_00011_850_reduced001_nit_000.fits'),
            'jcmts20140910_00011_850_%_000')

        # CAOM-2 SCUBA-2 preview
        self.assertEqual(ad._filename_pattern(
            'jcmt_scuba2_00022_20140904t064528_raw-450um_preview_64.png'),
            'jcmt_scuba2_00022_20140904t064528_%')

        # ACSIS reduced
        self.assertEqual(ad._filename_pattern(
            'jcmth20140910_00042_02_cube001_obs_000.fits'),
            'jcmth20140910_00042_02_%_000')

        # CAOM-2 ACSIS preview
        self.assertEqual(ad._filename_pattern(
            'jcmt_acsis_00009_20140908t124124_reduced-'
            '330588mhz-250mhzx8192-1_preview_64.png'),
            'jcmt_acsis_00009_20140908t124124_%')

        # CAOM-2 nightly group preview
        self.assertEqual(ad._filename_pattern(
            'jcmt_20140904-4aeef42b19659ef49c488d1760c1f380_'
            'reduced-850um_preview_64.png'),
            'jcmt_20140904-4aeef42b19659ef49c488d1760c1f380_%')

        with self.assertRaises(JSAProcError):
            ad._filename_pattern('s7q20140401_00042_0001')
