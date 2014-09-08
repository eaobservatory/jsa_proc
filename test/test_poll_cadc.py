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

from jsa_proc.cadc.dpdb import CADCDPInfo
from jsa_proc.error import NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.statemachine import JSAProcStateMachine

from .db import DBTestCase
from .dummycadcdp import DummyCADCDP


class PollCADCTestCase(DBTestCase):
    def test_poll_cadc(self):
        job_id_11 = self.db.add_job('tag-11', 'CADC', 'obs', 'params...',
                                    'test', [], '11', JSAProcState.RUNNING)
        job_id_12 = self.db.add_job('tag-12', 'CADC', 'obs', 'params...',
                                    'test', [], '12', JSAProcState.RUNNING)

        # Both jobs start in the running state.
        self.assertEqual(self.db.get_job(job_id_11).state,
                         JSAProcState.RUNNING)
        self.assertEqual(self.db.get_job(job_id_12).state,
                         JSAProcState.RUNNING)

        # Run the state machine.
        cadc = DummyCADCDP([
            (CADCDPInfo('11', 'Y', 'tag-11', 'params...', -250),
                ['f_11_01'], ['rf_11.fits']),
            (CADCDPInfo('12', 'E', 'tag-12', 'params...', -250),
                ['f_12_01'], ['rf_12.fits']),
        ])

        sm = JSAProcStateMachine(self.db, cadc)

        self.assertTrue(sm.poll_cadc_jobs(fetch_previews=False))

        # Job 11 state should have been changed to COMPLETE,
        # and the output files should have been added.
        self.assertEqual(self.db.get_job(job_id_11).state,
                         JSAProcState.COMPLETE)
        self.assertEqual(self.db.get_output_files(job_id_11),
                         ['rf_11.fits'])

        # Job 12 state should have been changed to ERROR,
        # and the output files should not have been added.
        self.assertEqual(self.db.get_job(job_id_12).state,
                         JSAProcState.ERROR)
        with self.assertRaises(NoRowsError):
            self.db.get_output_files(job_id_12)
