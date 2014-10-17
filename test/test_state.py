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

        with self.assertRaises(JSAProcError):
                CADCDPState.jsaproc_state('!')

    def test_state_name(self):
        """Test lookup of state names."""

        states = {
            JSAProcState.UNKNOWN: 'Unknown',
            JSAProcState.QUEUED: 'Queued',
            JSAProcState.MISSING: 'Missing',
            JSAProcState.FETCHING: 'Fetching',
            JSAProcState.WAITING: 'Waiting',
            JSAProcState.RUNNING: 'Running',
            JSAProcState.PROCESSED: 'Processed',
            JSAProcState.TRANSFERRING: 'Transferring',
            JSAProcState.INGESTION: 'Ingestion',
            JSAProcState.INGESTING: 'Ingesting',
            JSAProcState.COMPLETE: 'Complete',
            JSAProcState.ERROR: 'Error',
            JSAProcState.DELETED: 'Deleted',
        }

        for (state, name) in states.items():
            self.assertEqual(JSAProcState.get_name(state), name)

        with self.assertRaises(JSAProcError):
            JSAProcState.get_name('!')

    def test_state_info(self):
        """Test retrieval of state information."""

        # We should get an error if the state does not exist.
        with self.assertRaises(JSAProcError):
            JSAProcState.get_info('!')

        # Try a small sample of info values
        self.assertEqual(JSAProcState.get_info(JSAProcState.WAITING).active,
                         False)
        self.assertEqual(JSAProcState.get_info(JSAProcState.FETCHING).active,
                         True)
        self.assertEqual(JSAProcState.get_info(JSAProcState.INGESTION).phase,
                         JSAProcState.PHASE_RUN)
        self.assertEqual(JSAProcState.get_info(JSAProcState.UNKNOWN).phase,
                         JSAProcState.PHASE_QUEUE)
        self.assertEqual(JSAProcState.get_info(JSAProcState.RUNNING).name,
                         'Running')
        self.assertEqual(JSAProcState.get_info(JSAProcState.COMPLETE).name,
                         'Complete')
        self.assertEqual(JSAProcState.get_info(JSAProcState.INGESTING).final,
                         False)
        self.assertEqual(JSAProcState.get_info(JSAProcState.DELETED).final,
                         True)

    def test_cadc_state_name(self):
        """Test lookup of CADC state names."""

        states = {
            CADCDPState.QUEUED: 'Queued',
            CADCDPState.DRM_QUEUED: 'DRM Queued',
            CADCDPState.RETRIEVE_STARTED: 'Retrieve started',
            CADCDPState.RETRIEVE_ENDED: 'Retrieve ended',
            CADCDPState.CAPTURE_STARTED: 'Capture started',
            CADCDPState.CAPTURE_ENDED: 'Capture ended',
            CADCDPState.COMPLETE: 'Complete',
            CADCDPState.ERROR: 'Error',
            CADCDPState.DO_AGAIN: 'Do again',
            CADCDPState.UNDOABLE: 'Un-doable',
        }

        for (state, name) in states.items():
            self.assertEqual(CADCDPState.get_name(state), name)

        with self.assertRaises(JSAProcError):
            CADCDPState.get_name('!')
