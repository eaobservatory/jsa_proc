# Copyright (C) 2017 East Asian Observatory.
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

from jsa_proc.error import JSAProcError, NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.submit.update import add_upd_del_job

from .db import DBTestCase


class SubmitUpdateTest(DBTestCase):
    """
    Test the job add/update/delete submission function.
    """

    def test_child_job(self):
        # Create parent jobs.
        job_1 = self.db.add_job(
            'job-1', 'LOC', 'obs', 'PARAM', 'task-obs',
            input_file_names=['a1.sdf', 'a2.sdf'])

        self.assertIsInstance(job_1, int)

        job_2 = self.db.add_job(
            'job-2', 'LOC', 'obs', 'PARAM', 'task-obs',
            input_file_names=['a3.sdf', 'a4.sdf'])

        self.assertIsInstance(job_2, int)

        job_3 = self.db.add_job(
            'job-3', 'LOC', 'obs', 'PARAM', 'task-obs',
            input_file_names=['a5.sdf', 'a6.sdf'])

        self.assertIsInstance(job_3, int)

        # Try creating child job.
        kwargs = {
            'db': self.db,
            'tag': 'tag-c-1',
            'location': 'LOC',
            'mode': 'public',
            'parameters': 'PARAM',
            'task': 'task-coadd',
            'priority': 50,
            'parent_jobs': [job_1, job_2],
            'filters': ['.*\.fits', '.*\.fits'],
        }

        with self.assertRaisesRegexp(JSAProcError, 'adding is turned off'):
            add_upd_del_job(allow_add=False, **kwargs)

        job_id = add_upd_del_job(**kwargs)

        self.assertIsInstance(job_id, int)

        job = self.db.get_job(job_id)
        self.assertEqual(
            [job.id, job.tag, job.location, job.mode, job.parameters,
             job.priority, job.task, job.state],
            [job_id, 'tag-c-1', 'LOC', 'public', 'PARAM', 50, 'task-coadd',
             JSAProcState.UNKNOWN])

        self._compare_job(job_id, JSAProcState.UNKNOWN, [], [job_1, job_2])

        self.db.change_state(job_id, JSAProcState.COMPLETE, 'test')

        self._compare_job(job_id, JSAProcState.COMPLETE, [], [job_1, job_2])

        # Try updating child job: no change.
        job_id_upd = add_upd_del_job(
            allow_add=False, allow_upd=False, allow_del=False, **kwargs)

        self.assertEqual(job_id_upd, job_id)

        self.db.change_state(job_id, JSAProcState.COMPLETE, 'test')

        # Try updating child job: change parents.  State should be reset.
        kwargs['parent_jobs'] = [job_1, job_3]

        with self.assertRaisesRegexp(JSAProcError, 'updating is turned off'):
            add_upd_del_job(allow_upd=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        self._compare_job(job_id, JSAProcState.UNKNOWN, [], [job_1, job_3])

        # Try deleting a job: set empty parents list.
        kwargs['parent_jobs'] = []
        kwargs['filters'] = []

        with self.assertRaisesRegexp(JSAProcError, 'deleting is turned off'):
            add_upd_del_job(allow_del=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.DELETED)

    def _compare_job(self, job_id, state, input_files, parents):
        """
        Private method to compare a job record with expected values.
        """

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, state)

        if input_files:
            self.assertEqual(sorted(self.db.get_input_files(job_id)),
                             sorted(input_files))
        else:
            with self.assertRaises(NoRowsError):
                self.db.get_input_files(job_id)

        if parents:
            self.assertEqual(sorted(x[0] for x in self.db.get_parents(job_id)),
                             sorted(parents))

        else:
            with self.assertRaises(NoRowsError):
                self.db.get_parents(job_id)
