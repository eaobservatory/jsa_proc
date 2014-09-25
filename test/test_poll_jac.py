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

from jsa_proc.admin.statemachine import JSAProcStateMachine
from jsa_proc.state import JSAProcState

from .db import DBTestCase


class PollJACTestCase(DBTestCase):
    def test_poll_jac(self):
        # A job which should pass validation:
        job1 = self.db.add_job(
            'tag1', 'JAC', 'obs', 'RECIPE_NAME', 'test', input_file_names=['f_1_01'])

        # Jobs which should fail validation:
        job2 = self.db.add_job(
            'tag2', 'JAC', 'fortnight', 'RECIPE_NAME', 'test', input_file_names=['f_2_01'])
        job3 = self.db.add_job(
            'tag3', 'JAC', 'obs', 'RECIPE_NAME', 'test', input_file_names=[''])
        job4 = self.db.add_job(
            'tag4', 'JAC', 'obs', 'RECIPE_NAME', 'test', input_file_names=['/jcmtdata/f_4_01'])
        job5 = self.db.add_job(
            'tag5', 'JAC', 'obs', 'RECIPE_NAME', 'test', input_file_names=['f_4_01.sdf'])

        # Run state machine.
        sm = JSAProcStateMachine(self.db, None)
        self.assertTrue(sm.poll_jac_jobs())

        # Check results of validation.
        self.assertEqual(self.db.get_job(job1).state, JSAProcState.QUEUED)
        self.assertEqual(self.db.get_job(job2).state, JSAProcState.ERROR)
        self.assertEqual(self.db.get_job(job3).state, JSAProcState.ERROR)
        self.assertEqual(self.db.get_job(job4).state, JSAProcState.ERROR)
        self.assertEqual(self.db.get_job(job5).state, JSAProcState.ERROR)
