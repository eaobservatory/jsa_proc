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
            'job', 'input_file', 'output_file', 'log',
        )))




class InterfaceDBTest(DBTestCase):
    """
    Perform tests of the itnerface to the database.
    """

    # self.db is the instance of the JSAProc

    def test_add_job(self):
        """
        Test that a job can be added to the database.

        (This Uses get_job and get_input_files to test add_job.)
        """

        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        input_file_names=['/dummy/data/loc/testfile1.sdf', '/dummy/data/local/testfile2.sdf']

        # Add a test job.
        job_id = self.db.add_job(tag, location, mode, input_file_names)

        # Check its added correctly to job database.
        job  = self.db.get_job(id_=job_id)
        self.assertEqual([job.id, job.tag, job.location],[job_id, tag, location])

        # Check that file list is added correctly.
        files = self.db.get_input_files(job_id)
        self.assertEqual(set(files), set(input_file_names))

    def test_change_state(self):
        """
        Change the state of a job in the database using change_state.

        (This also tests add_job,get_job, get_logs and get_last_log).

        """
        # Add a job to database to ensure one is there
        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        input_file_names=['/dummy/data/loc/testfile1.sdf', '/dummy/data/local/testfile2.sdf']

        job_id = self.db.add_job(tag, location, mode, input_file_names)

        # Values to change to.
        newstate = 'R'
        message = 'Changed state of job %s to R'%(job_id)
        newstate2 = 'W'
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
        last_log = self.db.get_last_log(job_id)
        self.assertEqual([last_log.state_new, last_log.state_prev, last_log.message],
                         [newstate2, newstate, message2])
        logs = self.db.get_logs(job_id)
        maxid = max([l.id for l in logs])
        for l in logs:
            if l.id == maxid:
                self.assertEqual([l.state_new, l.state_prev, l.message],
                                 [newstate2, newstate, message2])

        # Check two log lines were retrieved.
        self.assertEqual(len(logs), 2)

    def test_set_location_foreign_id(self):
        """
        Test setting a location and foreign id.
        """
        # First of all put a dummy job into the database.
        # Add a job to database to ensure one is there
        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        input_file_names=['/dummy/data/loc/testfile1.sdf', '/dummy/data/local/testfile2.sdf']
        job_id = self.db.add_job(tag, location, mode, input_file_names)

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

    def test_set_output_file_list(self):
        """
        Test setting output files for a job.
        """
        # First of all put a dummy job into the database.
        tag = 'scuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        input_file_names=['/dummy/data/loc/testfile1.sdf', '/dummy/data/local/testfile2.sdf']
        job_id = self.db.add_job(tag, location, mode, input_file_names)

        # Values used in updating.
        output_files1 = ['/dummy/data/output/myoutputfile1.sdf',
                        '/dummy/data/output2/myoutputfile2.sdf']

        output_files2 = ['/dummy/data/output/myoutputfile3.sdf',
                        '/dummy/data/output4/myoutputfile4.sdf']

        # Update the output files for this job.
        self.db.set_output_file_list(job_id, output_files1)

        # Check the values
        out_f = self.db.get_output_file_list(job_id)
        self.assertEqual(set(out_f), set(output_files1))

        # Re update to check it works when there are already files written in.
        self.db.set_output_file_list(job_id, output_files2)

        # Check new values
        out_f = self.db.get_output_file_list(job_id)
        self.assertEqual(set(out_f), set(output_files2))
