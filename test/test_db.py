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

from socket import gethostname

from jsa_proc.error import JSAProcError, NoRowsError, ExcessRowsError
from jsa_proc.state import JSAProcState

from .db import DBTestCase

class BasicDBTest(DBTestCase):
    """Perform basic low-level tests of the database system."""

    def test_tables(self):
        """Test that the database contains the expected tables."""

        with self.db.db as c:
            tables = set()
            for (name,) in c.execute('SELECT name FROM sqlite_master '
                          'WHERE type="table"'):
                if not name.startswith('sqlite'):
                    tables.add(name)

        self.assertEqual(tables, set((
            'job', 'input_file', 'output_file', 'log', 'obs', 'tile',
        )))




class InterfaceDBTest(DBTestCase):
    """
    Perform tests of the itnerface to the database.
    """

    # self.db is the instance of the JSAProc

    def test_get_job_error(self):
        """
        Test that get_job raises error's correctly.
        """
        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        parameters = 'REDUCE_SCAN_JSA_PUBLIC'
        input_file_names=['testfile1', 'testfile2']
        priority=-321

        self.db.add_job(tag, location, mode, parameters, input_file_names, priority=priority)

        with self.assertRaises(JSAProcError):
            self.db.get_job()

        j = self.db.get_job(tag=tag)
        j = self.db.get_job(id_=1)

        with self.assertRaises(NoRowsError):
            self.db.get_job(id_=2)


    def test_get_last_log_error(self):
        """
        Test the get_last_log raises error if no row.
        """

        with self.assertRaises(NoRowsError):
            self.db.get_last_log(1)

    def test_add_job(self):
        """
        Test that a job can be added to the database.

        (This Uses get_job and get_input_files to test add_job.)
        """

        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        parameters = 'REDUCE_SCAN_JSA_PUBLIC'
        input_file_names=['testfile1', 'testfile2']
        priority=-321

        # Add a test job.
        job_id = self.db.add_job(tag, location, mode, parameters, input_file_names, priority=priority)

        # Check its added correctly to job database.
        job  = self.db.get_job(id_=job_id)
        self.assertEqual(job.state, '?')
        self.assertEqual([job.id, job.tag, job.location, job.mode, job.parameters, job.priority],
                         [job_id, tag, location, mode, parameters, priority])

        # Check that file list is added correctly.
        files = self.db.get_input_files(job_id)
        self.assertEqual(set(files), set(input_file_names))

        # Check that a log entry was written.
        logs = self.db.get_logs(job_id)
        self.assertEqual(len(logs), 1)
        self.assertIn('added to the database', logs[0].message)

        # Try adding a job with a state specified
        id_2 = self.db.add_job('tag2', 'JAC', 'obs', 'REC', [], state=JSAProcState.TRANSFERRING)
        job2 = self.db.get_job(id_=id_2)
        self.assertEqual(job2.state, JSAProcState.TRANSFERRING)

        with self.assertRaises(JSAProcError):
            self.db.add_job('tag3', 'CADC', 'night', 'REC', [], state='!')

        # Check we can't give the same file more than once (a database
        # constraint).
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'JAC', 'obs', 'REC', ['file1', 'file1'])

    def test_change_state(self):
        """
        Change the state of a job in the database using change_state.

        (This also tests add_job,get_job, get_logs and get_last_log).

        """
        # Add a job to database to ensure one is there
        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        parameters = 'REDUCE_SCAN_JSA_PUBLIC'
        input_file_names=['testfile1', 'testfile2']

        job_id = self.db.add_job(tag, location, mode, parameters, input_file_names)

        # Values to change to.
        newstate = JSAProcState.RUNNING
        message = 'Changed state of job %s to S'%(job_id)
        newstate2 = JSAProcState.WAITING
        message2 = 'Changed state of job %s to %s'%(job_id, newstate2)

        # Get the original  state of job 1.
        job = self.db.get_job(id_=job_id)
        state_orig = job.state

        # Change the state of job 1 twice.
        self.db.change_state(job_id, newstate, message)
        self.db.change_state(job_id, newstate2, message2)

        # Check the state and previous state of job 1
        job = self.db.get_job(id_=job_id)
        self.assertEqual(job.state, newstate2)
        self.assertEqual(job.state_prev, newstate)

        # Check log for state and messages. (check both get_last_log and get_logs).
        hostname = gethostname()
        last_log = self.db.get_last_log(job_id)
        self.assertEqual([last_log.state_new, last_log.state_prev, last_log.message, last_log.host],
                         [newstate2, newstate, message2, hostname])
        logs = self.db.get_logs(job_id)
        maxid = max([l.id for l in logs])
        for l in logs:
            if l.id == maxid:
                self.assertEqual([l.state_new, l.state_prev, l.message, l.host],
                                 [newstate2, newstate, message2, hostname])

        # Check two log lines were retrieved.
        self.assertEqual(len(logs), 3)

        # Check an error is raised if the job does not exist.
        with self.assertRaises(NoRowsError):
            self.db.change_state(job_id + 1, JSAProcState.INGESTION, 'test')

        # Try state_prev-checked version of the method.
        self.db.change_state(job_id, JSAProcState.RUNNING, 'test',
                             state_prev=JSAProcState.WAITING)

        with self.assertRaises(NoRowsError):
            self.db.change_state(job_id, JSAProcState.RUNNING, 'test',\
                                 state_prev=JSAProcState.WAITING)

        # Check that an error is raised if the new state is bad.
        with self.assertRaises(JSAProcError):
            self.db.change_state(job_id, '!', 'test bad state')

    def test_set_location_foreign_id(self):
        """
        Test setting a location and foreign id.
        """
        # First of all put a dummy job into the database.
        # Add a job to database to ensure one is there
        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        parameters = 'REDUCE_SCAN_JSA_PUBLIC'
        input_file_names=['testfile1', 'testfile2']
        job_id = self.db.add_job(tag, location, mode, parameters, input_file_names)

        # Values for testing
        location = 'CADC'
        location2 = 'SomewhereElse'
        foreign_id = 'DummyCADCId'
        foreign_id2 = 'DummyCADCId2'

        # Change the location and foreign_id
        self.db.set_location(job_id, location, foreign_id=foreign_id)

        # Check its changed correctly.
        job = self.db.get_job(id_=job_id)

        self.assertEqual(job.location, location)
        self.assertEqual(job.foreign_id, foreign_id)

        #Change the foreign_id on its own
        self.db.set_foreign_id(job_id, foreign_id2)

        # Check foreign_id has been updated correctly
        # but the location was left alone.
        job = self.db.get_job(id_=job_id)
        self.assertEqual(job.location, location)
        self.assertEqual(job.foreign_id, foreign_id2)

        # Change the location only.
        self.db.set_location(job_id, location2)

        # Check that the location changed but the foreign ID did not.
        job = self.db.get_job(id_=job_id)
        self.assertEqual(job.location, location2)
        self.assertEqual(job.foreign_id, foreign_id2)

    def test_set_output_files(self):
        """
        Test setting output files for a job.
        """
        # First of all put a dummy job into the database.
        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        parameters = 'REDUCE_SCAN_JSA_PUBLIC'
        input_file_names=['testfile1', 'testfile2']
        job_id = self.db.add_job(tag, location, mode, parameters, input_file_names)

        # Values used in updating.
        output_files1 = ['myoutputfile1',
                        'myoutputfile2']

        output_files2 = ['myoutputfile3',
                        'myoutputfile4']

        # Update the output files for this job.
        self.db.set_output_files(job_id, output_files1)

        # Check the values
        out_f = self.db.get_output_files(job_id)
        self.assertEqual(set(out_f), set(output_files1))

        # Re update to check it works when there are already files written in.
        self.db.set_output_files(job_id, output_files2)

        # Check new values
        out_f = self.db.get_output_files(job_id)
        self.assertEqual(set(out_f), set(output_files2))

    def test_output_files(self):
        """
        Test the set_output_files and get_output_files methods.
        """

        # Add a job
        job1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', [], priority=2)

        # Add an output file
        outputfiles = ['test.sdf','test.png','longfilenametahtisrandom.log']
        self.db.set_output_files(1, outputfiles)

        addfiles = self.db.get_output_files(1)

        for i in outputfiles:
            self.assertTrue(i in addfiles)

    def test_find_jobs(self):
        """Test the find_jobs method."""

        # Add some jobs.
        job1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', [], priority=2) # ?
        job2 = self.db.add_job('tag2', 'JAC',  'obs', 'RECIPE', [], priority=4) # Q
        job3 = self.db.add_job('tag3', 'JAC',  'obs', 'RECIPE', [], priority=6) # Q
        job4 = self.db.add_job('tag4', 'CADC', 'obs', 'RECIPE', [], priority=5) # Q
        job5 = self.db.add_job('tag5', 'CADC', 'obs', 'RECIPE', [], priority=3) # ?
        jobx = self.db.add_job('tagx', 'JAC',  'obs', 'RECIPE', [], priority=3) # X

        # Put some into another state.
        self.db.change_state(job2, 'Q', 'test')
        self.db.change_state(job3, 'Q', 'test')
        self.db.change_state(job4, 'Q', 'test')
        self.db.change_state(jobx, JSAProcState.DELETED, 'delete this job')

        # Now run some searches and check we get the right sets of jobs.
        self.assertEqual(
                set((x.tag for x in self.db.find_jobs(state='?'))),
                set(('tag1', 'tag5')))

        self.assertEqual(
                set((x.tag for x in self.db.find_jobs(state='Q'))),
                set(('tag2', 'tag3', 'tag4')))

        self.assertEqual(
                set((x.tag for x in self.db.find_jobs(location='JAC'))),
                set(('tag1', 'tag2', 'tag3')))

        self.assertEqual(
                set((x.tag for x in self.db.find_jobs(location='CADC'))),
                set(('tag4', 'tag5')))

        self.assertEqual(
                set((x.tag for x in self.db.find_jobs(state='Q', location='JAC'))),
                set(('tag2', 'tag3')))

        self.assertEqual(
                set((x.tag for x in self.db.find_jobs(JSAProcState.DELETED))),
                set(('tagx',)))

        # Finally check a query which should get a single job and check the
        # info is good.
        jobs = self.db.find_jobs(state='?', location='CADC')
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].id, job5)
        self.assertEqual(jobs[0].tag, 'tag5')
        self.assertEqual(jobs[0].state, '?')
        self.assertEqual(jobs[0].location, 'CADC')

        # Test number option.
        self.assertEqual(len(self.db.find_jobs(number=2)), 2)

        # Test offset option.
        self.assertEqual(len(self.db.find_jobs(number=2, offset=1)), 2)

        # Test prioritize option.
        self.assertEqual([x.tag for x in self.db.find_jobs(prioritize=True)],
                         ['tag3', 'tag4', 'tag2', 'tag5', 'tag1'])
        self.assertEqual([x.tag for x in self.db.find_jobs(number=3, offset=1,
                                                           prioritize=True)],
                         ['tag4', 'tag2', 'tag5'])

        job6 = self.db.add_job('tag6', 'FAKELOC', 'obs', 'RECIPE', [], priority=7)
        job7 = self.db.add_job('tag7', 'FAKELOC', 'obs', 'RECIPE', [], priority=8)
        job8 = self.db.add_job('tag8', 'FAKELOC', 'obs', 'RECIPE', [], priority=7)


        # Test sort option
        self.assertEqual([x.tag for x in self.db.find_jobs(prioritize=True,
                                                           sort=True,
                                                           location='FAKELOC')],
                         ['tag7', 'tag6', 'tag8'])
        self.assertEqual([x.tag for x in self.db.find_jobs(sort=True)],
                         ['tag1','tag2','tag3','tag4','tag5','tag6','tag7','tag8'])

        # Test the return preview files option..
        outfiles=['1.sdf', '2.sdf','name_preview_64.png']
        self.db.set_output_files(1, outfiles)
        self.assertEqual([x.outputs for x in self.db.find_jobs(number=1, outputs='%')][0],
                         outfiles)
        self.assertEqual([x.outputs for x in self.db.find_jobs(number=1, outputs='%preview_64.png')][0],
                         [outfiles[2]])
        self.assertEqual([x.outputs for x in self.db.find_jobs(number=1, outputs='%preview_64.pngs')][0],
                         None)

        # test the count option
        self.assertEqual(self.db.find_jobs(count=True), 8)
