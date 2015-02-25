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


class StateTestCase(TestCase):
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
            JSAProcState.INGEST_QUEUE: 'Queued to reingest',
            JSAProcState.INGEST_FETCH: 'Fetching to reingest',
            JSAProcState.INGESTION: 'Waiting to ingest',
            JSAProcState.INGESTING: 'Ingesting',
            JSAProcState.COMPLETE: 'Complete',
            JSAProcState.ERROR: 'Error',
            JSAProcState.DELETED: 'Deleted',
            JSAProcState.WONTWORK: 'Won\'t work',
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
