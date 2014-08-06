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

from jsa_proc.state import JSAProcState
from jsa_proc.cadc.dpstate import CADCDPState


class StateTestCase(TestCase):
    def test_cadc_to_jsa_proc(self):
        """Test mapping of CADC data processing states to local states."""

        states = {
            'Q': 'Q',
            'D': 'Q',
            'S': 'S',
            'N': 'S',
            'C': 'S',
            'P': 'S',
            'Y': 'Y',
            'E': 'E',
            'R': 'Q',
            'U': 'E',
        }

        for (cadc, jsa) in states.items():
            self.assertEqual(CADCDPState.jsaproc_state(cadc), jsa)

        with self.assertRaises(Exception):
                CADCDPState.jsaproc_state('X')
