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

from datetime import date, datetime

from jsa_proc.error import JSAProcError, NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.submit.update import add_upd_del_job, compare_obsinfo

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
            'parameters': 'PARAM-C1',
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
            [job_id, 'tag-c-1', 'LOC', 'public', 'PARAM-C1', 50, 'task-coadd',
             JSAProcState.UNKNOWN])

        self._compare_job(job_id, JSAProcState.UNKNOWN, [], [job_1, job_2])

        self.db.change_state(job_id, JSAProcState.COMPLETE, 'test')

        self._compare_job(job_id, JSAProcState.COMPLETE, [], [job_1, job_2])

        # Try updating child job: no change.
        job_id_upd = add_upd_del_job(
            allow_add=False, allow_upd=False, allow_del=False, **kwargs)

        self.assertEqual(job_id_upd, job_id)

        # Try updating child job: change parents.  State should be reset.
        kwargs['parent_jobs'] = [job_1, job_3]

        with self.assertRaisesRegexp(JSAProcError, 'updating is turned off'):
            add_upd_del_job(allow_upd=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        self._compare_job(job_id, JSAProcState.UNKNOWN, [], [job_1, job_3])

        self.db.change_state(job_id, JSAProcState.COMPLETE, 'test')

        # Test updating child job: change mode.
        kwargs['mode'] = 'project'

        with self.assertRaisesRegexp(JSAProcError, 'updating is turned off'):
            add_upd_del_job(allow_upd=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        job = self._compare_job(
            job_id, JSAProcState.UNKNOWN, [], [job_1, job_3])

        self.assertEqual(job.mode, 'project')
        self.assertEqual(job.parameters, 'PARAM-C1')

        self.db.change_state(job_id, JSAProcState.COMPLETE, 'test')

        # Test updating child job: change parameters.
        kwargs['parameters'] = 'PARAM-C2'

        with self.assertRaisesRegexp(JSAProcError, 'updating is turned off'):
            add_upd_del_job(allow_upd=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        job = self._compare_job(
            job_id, JSAProcState.UNKNOWN, [], [job_1, job_3])

        self.assertEqual(job.mode, 'project')
        self.assertEqual(job.parameters, 'PARAM-C2')

        # Try deleting a job: set empty parents list.
        kwargs['parent_jobs'] = []
        kwargs['filters'] = []

        with self.assertRaisesRegexp(JSAProcError, 'deleting is turned off'):
            add_upd_del_job(allow_del=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.DELETED)

    def test_regular_job(self):
        # Try creating a regular job.
        obsinfo = {
            'obsid': 'x', 'obsidss': 'x_450',
            'utdate': date(2017, 3, 10), 'obsnum': 40, 'instrument': 'SCUBA-2',
            'backend': 'SCUBA-2', 'subsys': '450',
            'date_obs': datetime(2017, 3, 10, 14, 00, 00)}

        kwargs = {
            'db': self.db,
            'tag': 'tag-1',
            'location': 'LOC',
            'mode': 'night',
            'parameters': 'PARAM',
            'task': 'task-test',
            'priority': 50,
            'input_file_names': ['s4a_x.sdf', 's4a_y.sdf'],
            'obsinfolist': [obsinfo],
            'tilelist': [50, 51, 52],
        }

        with self.assertRaisesRegexp(JSAProcError, 'adding is turned off'):
            add_upd_del_job(allow_add=False, **kwargs)

        job_id = add_upd_del_job(**kwargs)

        self.assertIsInstance(job_id, int)

        self._compare_job(
            job_id, JSAProcState.UNKNOWN, ['s4a_x.sdf', 's4a_y.sdf'], [])

        self.assertEqual(sorted(self.db.get_tilelist(job_id)), [50, 51, 52])

        obsinfo_fetched = self.db.get_obs_info(job_id)
        self.assertEqual(len(obsinfo_fetched), 1)

        self.assertTrue(compare_obsinfo(obsinfo_fetched[0], obsinfo))

        self.db.change_state(job_id, JSAProcState.COMPLETE, 'test')

        # Try updating regular job: no change.
        job_id_upd = add_upd_del_job(
            allow_add=False, allow_upd=False, allow_del=False, **kwargs)

        self.assertEqual(job_id_upd, job_id)

        self._compare_job(
            job_id, JSAProcState.COMPLETE, ['s4a_x.sdf', 's4a_y.sdf'], [])

        # Try updating regular job: change input files.
        kwargs['input_file_names'] = ['s4a_x.sdf', 's4a_z.sdf']

        with self.assertRaisesRegexp(JSAProcError, 'updating is turned off'):
            add_upd_del_job(allow_upd=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        self._compare_job(
            job_id, JSAProcState.UNKNOWN, ['s4a_x.sdf', 's4a_z.sdf'], [])

        # Try deleting a regular job: set empty input file list.
        kwargs['input_file_names'] = []

        with self.assertRaisesRegexp(JSAProcError, 'deleting is turned off'):
            add_upd_del_job(allow_del=False, **kwargs)

        job_id_upd = add_upd_del_job(**kwargs)

        self.assertEqual(job_id_upd, job_id)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.DELETED)

        # Try deleting a regular job in an active state: should need force.
        self.db.change_state(job_id, JSAProcState.TRANSFERRING, 'test')

        with self.assertRaisesRegexp(JSAProcError, 'currently active'):
            add_upd_del_job(**kwargs)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.TRANSFERRING)

        add_upd_del_job(force=True, **kwargs)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.DELETED)

        # Try updating a regular job in an active state: should need force.
        self.db.change_state(job_id, JSAProcState.RUNNING, 'test')
        kwargs['input_file_names'] = ['s4a_w.sdf']

        with self.assertRaisesRegexp(JSAProcError, 'currently active'):
            add_upd_del_job(**kwargs)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.RUNNING)

        add_upd_del_job(force=True, **kwargs)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.UNKNOWN)

        # Try updating tilelist and obsinfo: state should not be reset.
        self.db.change_state(job_id, JSAProcState.PROCESSED, 'test')

        kwargs['tilelist'] = [53, 54, 55]
        kwargs['obsinfolist'][0]['obsnum'] = 42

        add_upd_del_job(**kwargs)

        job = self.db.get_job(job_id)
        self.assertEqual(job.state, JSAProcState.PROCESSED)

        self.assertEqual(sorted(self.db.get_tilelist(job_id)), [53, 54, 55])

        obsinfo_fetched = self.db.get_obs_info(job_id)
        self.assertEqual(len(obsinfo_fetched), 1)

        self.assertEqual(obsinfo_fetched[0].obsnum, 42)

    def _compare_job(self, job_id, state, input_files, parents):
        """
        Private method to compare a job record with expected values.

        Return the job object to allow the caller to make additional tests.
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

        return job
