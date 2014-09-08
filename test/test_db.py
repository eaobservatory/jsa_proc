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

from collections import OrderedDict
from socket import gethostname
from unittest import TestCase

from jsa_proc.db.db import _dict_query_where_clause, Not, Fuzzy, Range
from jsa_proc.error import JSAProcError, NoRowsError, ExcessRowsError
from jsa_proc.jcmtobsinfo import ObsQueryDict
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
        input_file_names = ['testfile1', 'testfile2']
        priority = -321

        self.db.add_job(tag, location, mode, parameters, input_file_names,
                        priority=priority)

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
        input_file_names = ['testfile1', 'testfile2']
        priority = -321

        # Add a test job.
        job_id = self.db.add_job(tag, location, mode, parameters,
                                 input_file_names, priority=priority)

        # Check its added correctly to job database.
        job = self.db.get_job(id_=job_id)
        self.assertEqual(job.state, '?')
        self.assertEqual([job.id, job.tag, job.location, job.mode,
                          job.parameters, job.priority],
                         [job_id, tag, location, mode, parameters, priority])

        # Check that file list is added correctly.
        files = self.db.get_input_files(job_id)
        self.assertEqual(set(files), set(input_file_names))

        # Check that a log entry was written.
        logs = self.db.get_logs(job_id)
        self.assertEqual(len(logs), 1)
        self.assertIn('added to the database', logs[0].message)

        # Try adding a job with a state specified
        id_2 = self.db.add_job('tag2', 'JAC', 'obs', 'REC', [],
                               state=JSAProcState.TRANSFERRING)
        job2 = self.db.get_job(id_=id_2)
        self.assertEqual(job2.state, JSAProcState.TRANSFERRING)

        # Check can't add job with invalid state.
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag3', 'CADC', 'night', 'REC', [], state='!')

        # Check we can't give the same file more than once (a database
        # constraint).
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'JAC', 'obs', 'REC', ['file1', 'file1'])

        # Check that we can update the obs table while adding a job.
        obs1 = {'obsid': '01-asdfasd', 'obsidss': '01-asdfas-1',
                'utdate': 20140101, 'obsnum': 3, 'instrument': 'SCUBA-2',
                'backend': 'ACSIS', 'subsys': '1'}
        obs2 = {'obsid': '02-asdfasd', 'obsidss': '02-asdfas-1',
                'utdate': 20140102, 'obsnum': 3, 'instrument': 'SCUBA-2',
                'backend': 'ACSIS', 'subsys': '1'}

        self.db.set_obs_info(1, [obs1, obs2], replace_all=True)

        self.db.add_job('tag5', 'JAC', 'obs', 'RED', ['file1', 'file2'],
                        obsinfolist=[obs1, obs2])

        # Check that we can't update the obs table with the invalid
        # or bad column names.
        for (error, obsbad) in (
                # Invalid characters:
                ('invalid column name',
                 {'obsid*': 'asdfasd', 'obsidss': 'asdfas', 'utdate': 20140101,
                  'obsnum': 3, 'instrument': 'SCUBA-2', 'backend': 'ACSIS',
                  'subsys': '1'}),
                # Non-existant column names:
                ('has no column',
                 {'obsid': 'asdfasd', 'obsidsss': 'asdfas', 'utdate': 20140101,
                  'obsnum': 3, 'instrument': 'SCUBA-2', 'backend': 'ACSIS',
                  'subsys': '1'}),
                # Forbidden column names:
                ('private column name',
                 {'obsid': 'asdfasd', 'obsidss': 'asdfas', 'utdate': 20140101,
                  'obsnum': 3, 'instrument': 'SCUBA-2', 'backend': 'ACSIS',
                  'subsys': '1', 'id': 42}),
                ('private column name',
                 {'obsid': 'asdfasd', 'obsidss': 'asdfas', 'utdate': 20140101,
                  'obsnum': 3, 'instrument': 'SCUBA-2', 'backend': 'ACSIS',
                  'subsys': '1', 'job_id': 42}),
                ):

            with self.assertRaisesRegexp(JSAProcError, error):
                self.db.add_job('tag6', 'JAC', 'obs', 'RED',
                                ['file1', 'file2'], obsinfolist=[obsbad])

        # Check that we can set the tile list when adding a job.
        job_7 = self.db.add_job('tag7', 'JAC', 'obs', 'RED', [], tilelist=[42])

        self.assertEqual(self.db.get_tilelist(job_7), [42])

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
        input_file_names = ['testfile1', 'testfile2']

        job_id = self.db.add_job(tag, location, mode, parameters,
                                 input_file_names)

        # Values to change to.
        newstate = JSAProcState.RUNNING
        message = 'Changed state of job %s to S' % (job_id)
        newstate2 = JSAProcState.WAITING
        message2 = 'Changed state of job %s to %s' % (job_id, newstate2)

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

        # Check log for state and messages.
        # (check both get_last_log and get_logs).
        hostname = gethostname()
        last_log = self.db.get_last_log(job_id)
        self.assertEqual([last_log.state_new, last_log.state_prev,
                          last_log.message, last_log.host],
                         [newstate2, newstate, message2, hostname])
        logs = self.db.get_logs(job_id)
        maxid = max([l.id for l in logs])
        for l in logs:
            if l.id == maxid:
                self.assertEqual([l.state_new, l.state_prev, l.message,
                                  l.host],
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
            self.db.change_state(job_id, JSAProcState.RUNNING, 'test',
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
        input_file_names = ['testfile1', 'testfile2']
        job_id = self.db.add_job(tag, location, mode, parameters,
                                 input_file_names)

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

        # Change the foreign_id on its own
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
        input_file_names = ['testfile1', 'testfile2']
        job_id = self.db.add_job(tag, location, mode, parameters,
                                 input_file_names)

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
        outputfiles = ['test.sdf', 'test.png', 'longfilenametahtisrandom.log']
        self.db.set_output_files(1, outputfiles)

        addfiles = self.db.get_output_files(1)

        for i in outputfiles:
            self.assertTrue(i in addfiles)

    def test_find_jobs(self):
        """Test the find_jobs method."""

        # Add some jobs.
        job1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', [],
                               priority=2)  # ?
        job2 = self.db.add_job('tag2', 'JAC',  'obs', 'RECIPE', [],
                               priority=4)  # Q
        job3 = self.db.add_job('tag3', 'JAC',  'obs', 'RECIPE', [],
                               priority=6)  # Q
        job4 = self.db.add_job('tag4', 'CADC', 'obs', 'RECIPE', [],
                               priority=5)  # Q
        job5 = self.db.add_job('tag5', 'CADC', 'obs', 'RECIPE', [],
                               priority=3)  # ?
        jobx = self.db.add_job('tagx', 'JAC',  'obs', 'RECIPE', [],
                               priority=3)  # X

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

        job6 = self.db.add_job('tag6', 'FAKELOC', 'obs', 'RECIPE', [],
                               priority=7)
        job7 = self.db.add_job('tag7', 'FAKELOC', 'obs', 'RECIPE', [],
                               priority=8)
        job8 = self.db.add_job('tag8', 'FAKELOC', 'obs', 'RECIPE', [],
                               priority=7)

        # Test sort option
        self.assertEqual([x.tag for x in self.db.find_jobs(
            prioritize=True, sort=True, location='FAKELOC')],
            ['tag7', 'tag6', 'tag8'])
        self.assertEqual([x.tag for x in self.db.find_jobs(sort=True)],
                         ['tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'tag6',
                          'tag7', 'tag8'])

        # Test the return preview files option..
        outfiles = ['1.sdf', '2.sdf', 'name_preview_64.png']
        self.db.set_output_files(1, outfiles)
        self.assertEqual(
            [x.outputs for x in self.db.find_jobs(number=1, outputs='%')][0],
            outfiles)
        self.assertEqual(
            [x.outputs for x in self.db.find_jobs(
                number=1, outputs='%preview_64.png')][0],
            [outfiles[2]])
        self.assertEqual(
            [x.outputs for x in self.db.find_jobs(
                number=1, outputs='%preview_64.pngs')][0],
            None)

        # test the count option
        self.assertEqual(self.db.find_jobs(count=True), 8)

    def test_find_jobs_obsquery(self):

        info_1 = {'obsid': '1', 'obsidss': '1-1', 'utdate': '2014-01-01',
                  'obsnum': 1, 'instrument': 'F', 'backend': 'B',
                  'subsys': '1', 'survey': 'GBS', 'project': 'G01'}

        info_2 = info_1.copy()
        info_2.update(obsidss='1-2', subsys=2)

        info_3 = info_1.copy()
        info_3.update(survey='DDS', project='D01')

        job_1 = self.db.add_job('tag1', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_1])
        job_2 = self.db.add_job('tag2', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_2])
        job_3 = self.db.add_job('tag3', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_1, info_2])
        job_4 = self.db.add_job('tag4', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_3])
        job_5 = self.db.add_job('tag5', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_2, info_3])

        info_4 = info_1.copy()
        info_4.update(survey=None, project='XX01')

        info_5 = info_4.copy()
        info_5.update(project='JCMTCAL')

        info_6 = info_4.copy()
        info_6.update(project='CAL')

        job_6 = self.db.add_job('tag6', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_4])
        job_7 = self.db.add_job('tag7', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_5])
        job_8 = self.db.add_job('tag8', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_6])

        info_7 = info_1.copy()
        info_7.update(project='JCMTCAL')

        job_9 = self.db.add_job('tag9', 'JAC', 'obs', 'RECIPE', [],
                                obsinfolist=[info_7])

        queries = [
            (
                ObsQueryDict['Surveys']['GBS'].where,
                (job_1, job_2, job_3, job_5, job_9),
            ),
            (
                ObsQueryDict['Surveys']['DDS'].where,
                (job_4, job_5),
            ),
            (
                ObsQueryDict['Surveys']['NoSurvey'].where,
                (job_6, job_7, job_8),
            ),
            (
                ObsQueryDict['CalTypes']['Calibrations'].where,
                (job_7, job_8, job_9),
            ),
            (
                ObsQueryDict['CalTypes']['NoCalibrations'].where,
                (job_1, job_2, job_3, job_4, job_5, job_6),
            ),
            (
                d_add(ObsQueryDict['Surveys']['GBS'].where,
                      ObsQueryDict['CalTypes']['Calibrations'].where),
                (job_9,),
            ),
            (
                d_add(ObsQueryDict['Surveys']['GBS'].where,
                      ObsQueryDict['CalTypes']['NoCalibrations'].where),
                (job_1, job_2, job_3, job_5),
            ),
            (
                d_add(ObsQueryDict['Surveys']['NoSurvey'].where,
                      ObsQueryDict['CalTypes']['Calibrations'].where),
                (job_7, job_8),
            ),
            (
                d_add(ObsQueryDict['Surveys']['NoSurvey'].where,
                      ObsQueryDict['CalTypes']['NoCalibrations'].where),
                (job_6,),
            ),
        ]

        for (oq, expect) in queries:
            self.assertEqual(
                set(x.id for x in self.db.find_jobs(obsquery=oq)),
                set(expect))

            self.assertEqual(
                self.db.find_jobs(count=True, obsquery=oq),
                len(expect))

    def test_obs_info(self):
        job_1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', [])

        # Should start with no information.
        self.assertEqual(self.db.get_obs_info(job_1), [])

        info = {'obsid': 'x14_01_1T1', 'obsidss': 'x14_1_1T1_850',
                'utdate': '2014-01-01', 'obsnum': 3, 'instrument': 'SCUBA-2',
                'backend': 'ACSIS', 'subsys': '1'}

        self.db.set_obs_info(job_1, [info])

        # Create another job with this information from the start.
        job_2 = self.db.add_job('tag2', 'JAC',  'obs', 'RECIPE', [],
                                obsinfolist=[info])

        for job_id in (job_1, job_2):
            info_retrieved = self.db.get_obs_info(job_id)

            self.assertEqual(len(info_retrieved), 1)

            info_retrieved = info_retrieved[0]
            for key, value in info.items():
                self.assertEqual(getattr(info_retrieved, key), value)

    def test_processing_time(self):
        job_id = self.db.add_job('tag1', 'JAC', 'obs', 'RECIPE', [])

        self.db.change_state(job_id, JSAProcState.RUNNING, 'start')
        self.db.change_state(job_id, JSAProcState.PROCESSED, 'end')

        # Check that we find this job:
        (start, end, col) = self.db.get_processing_time_obs_type()

        self.assertEqual(len(start), 1)
        self.assertEqual(len(end), 1)

        self.assertEqual(start[0][3], JSAProcState.RUNNING)
        self.assertEqual(end[0][3], JSAProcState.PROCESSED)

        # Check we don't find the job if it's running or in the error state:
        self.db.change_state(job_id, JSAProcState.RUNNING, 'start again')
        self.assertEqual(self.db.get_processing_time_obs_type()[0], [])

        self.db.change_state(job_id, JSAProcState.ERROR, 'failure')
        self.assertEqual(self.db.get_processing_time_obs_type()[0], [])

        # Add some more jobs.
        job_2 = self.db.add_job('tag2', 'JAC', 'obs', 'RECIPE', [])
        job_3 = self.db.add_job('tag3', 'JAC', 'obs', 'RECIPE', [])
        job_4 = self.db.add_job('tag4', 'JAC', 'obs', 'RECIPE', [])
        self.db.change_state(job_2, JSAProcState.RUNNING, 'start')
        self.db.change_state(job_3, JSAProcState.RUNNING, 'start')
        self.db.change_state(job_4, JSAProcState.RUNNING, 'start')
        self.db.change_state(job_2, JSAProcState.PROCESSED, 'end')
        self.db.change_state(job_3, JSAProcState.PROCESSED, 'end')
        self.db.change_state(job_4, JSAProcState.PROCESSED, 'end')

        self.assertEqual(len(self.db.get_processing_time_obs_type(
            jobdict={'tag': 'tag3'})[0]),
            1)

        self.assertEqual(len(self.db.get_processing_time_obs_type(
            jobdict={'tag': ['tag3', 'tag4']})[0]),
            2)

    def test_tilelist(self):
        job_id = self.db.add_job('tag1', 'JAC', 'obs', 'RECIPE', [])

        # Start with no tilelist.
        self.assertEqual(self.db.get_tilelist(job_id), [])

        # Add tiles
        tiles = set((42, 43, 44))
        self.db.set_tilelist(job_id, tiles)
        self.assertEqual(set(self.db.get_tilelist(job_id)), tiles)

        # Change tiles
        newtiles = set((45, 46, 47))
        self.db.set_tilelist(job_id, newtiles)
        self.assertEqual(set(self.db.get_tilelist(job_id)), newtiles)


class DBUtilityTestCase(TestCase):
    def test_dict_query(self):
        with self.assertRaises(JSAProcError):
            _dict_query_where_clause('x;y', {'col': 'val'})

        with self.assertRaises(JSAProcError):
            _dict_query_where_clause('xyz', {'c;l': 'val'})

        queries = [
            (
                ('tab', OrderedDict()),
                ('', [])
            ),
            (
                ('tab', OrderedDict([('a', 'x'), ('b', 'y')])),
                ('(tab.`a`=%s AND tab.`b`=%s)', ['x', 'y'])
            ),
            (
                ('tab', OrderedDict([('a', 'x'), ('b', 'y')]), True),
                ('(tab.`a`=%s OR tab.`b`=%s)', ['x', 'y'])
            ),
            (
                ('tab', OrderedDict([('q', ['i', 'j'])])),
                ('(tab.`q` IN (%s, %s))', ['i', 'j'])
            ),
            (
                ('tab', OrderedDict([('q', Not(['i', 'j']))])),
                ('((tab.`q` NOT IN (%s, %s) OR tab.`q` IS NULL))', ['i', 'j'])
            ),
            (
                ('tab', OrderedDict([('z', Not('q'))])),
                ('((tab.`z`<>%s OR tab.`z` IS NULL))', ['q'])
            ),
            (
                ('tab', OrderedDict([('n', None)])),
                ('(tab.`n` IS NULL)', [])
            ),
            (
                ('tab', OrderedDict([('nn', Not(None))])),
                ('(tab.`nn` IS NOT NULL)', [])
            ),
            (
                ('tab', {'f': Fuzzy('x')}),
                ('(tab.`f` LIKE %s)', ['%x%'])
            ),
            (
                ('tab', {'f': Not(Fuzzy('x'))}),
                ('((tab.`f` NOT LIKE %s OR tab.`f` IS NULL))', ['%x%'])
            ),
            (
                ('tab', {'d': Range(28, 82)}),
                ('(tab.`d` BETWEEN %s AND %s)', [28, 82])
            ),
            (
                ('tab', {'d': Not(Range(28, 82))}),
                ('(tab.`d` NOT BETWEEN %s AND %s)', [28, 82])
            ),
            (
                ('tab', {'d': Range(28, None)}),
                ('(tab.`d` >= %s)', [28])
            ),
            (
                ('tab', {'d': Range(None, 82)}),
                ('(tab.`d` <= %s)', [82])
            ),
            (
                ('tab', {'d': Not(Range(28, None))}),
                ('(tab.`d` < %s)', [28])
            ),
            (
                ('tab', {'d': Not(Range(None, 82))}),
                ('(tab.`d` > %s)', [82])
            ),
        ]

        for (query, expect) in queries:
            self.assertEqual(_dict_query_where_clause(*query), expect)

def d_add(*args):
    return dict(sum((d.items() for d in args), []))
