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
from jsa_proc.state import JSAProcState
from jsa_proc.statemachine import JSAProcStateMachine

from .db import DBTestCase
from .dummycadcdp import DummyCADCDP


class PollCADCTestCase(DBTestCase):
    def test_poll_cadc(self):
        job_id = self.db.add_job('tag-11', 'CADC', 'obs', 'params...', [],
                                 '11', 'S')

        # Job starts in the running state.
        self.assertEqual(self.db.get_job(job_id).state, JSAProcState.RUNNING)

        cadc = DummyCADCDP([
            (CADCDPInfo('11', 'Y', 'tag-11', 'params...', -250),
                ['f_11_01'], []),
        ])

        sm = JSAProcStateMachine(self.db, cadc)

        self.assertTrue(sm.poll_cadc_jobs())

        # Job state should have been changed to complete.
        self.assertEqual(self.db.get_job(job_id).state, JSAProcState.COMPLETE)
