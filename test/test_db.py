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
from datetime import date, datetime
from socket import gethostname
from unittest import TestCase

from jsa_proc.db.db import _dict_query_where_clause, Not, Fuzzy, Range, \
        JSAProcFileInfo, JSAProcTaskInfo
from jsa_proc.error import JSAProcError, NoRowsError, ExcessRowsError
from jsa_proc.jcmtobsinfo import ObsQueryDict
from jsa_proc.state import JSAProcState
from jsa_proc.qa_state import JSAQAState

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
            'job', 'input_file', 'output_file', 'log', 'note',
            'tile', 'qa', 'task', 'parent', 'obsidss', 'obs_preproc',
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

        self.db.add_job(tag, location, mode, parameters, 'test',
                        input_file_names=input_file_names, priority=priority)

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
        tag2 = 'xscuba2_20121009_5_850'
        location = 'JSA'
        mode = 'obs'
        parameters = 'REDUCE_SCAN_JSA_PUBLIC'
        input_file_names = ['testfile1', 'testfile2']
        priority = -321

        # Add a test job.
        job_id = self.db.add_job(tag, location, mode, parameters, 'test',
                                 input_file_names=input_file_names, priority=priority)

        # Check its added correctly to job database.
        job = self.db.get_job(id_=job_id)
        self.assertEqual(job.state, '?')
        self.assertEqual([job.id, job.tag, job.location, job.mode,
                          job.parameters, job.priority, job.task],
                         [job_id, tag, location, mode, parameters, priority,
                          'test'])

        # Check that file list is added correctly.
        files = self.db.get_input_files(job_id)
        self.assertEqual(set(files), set(input_file_names))

        # Check that a log entry was written.
        logs = self.db.get_logs(job_id)
        self.assertEqual(len(logs), 1)
        self.assertIn('added to the database', logs[0].message)

        # Try adding a job with a state specified
        id_2 = self.db.add_job('tag2', 'JAC', 'obs', 'REC', 'test', input_file_names=['test1'],
                               state=JSAProcState.TRANSFERRING)
        job2 = self.db.get_job(id_=id_2)
        self.assertEqual(job2.state, JSAProcState.TRANSFERRING)

        # Try adding a job with parents
        job_id_p = self.db.add_job(tag2, location, mode, parameters, 'test',
                                   parent_jobs=[1,2], priority=priority)
        # check that its added correctly to job database.
        job = self.db.get_job(id_=job_id_p)
        self.assertEqual(job.state, '?')
        self.assertEqual([job.id, job.tag, job.location, job.mode,
                          job.parameters, job.priority, job.task],
                         [job_id_p, tag2, location, mode, parameters, priority,
                          'test'])

        # Check can't add job with invalid state.
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag3', 'CADC', 'night', 'REC', 'test', input_file_names=['test2'],
                            state='!')

        # Check we can't give the same tag more than once.
        with self.assertRaisesRegexp(JSAProcError,
                                     'a job already exists with the same tag'):
            self.db.add_job('tag2', 'JAC', 'obs', 'REC', 'test', input_file_names=['test1'])

        # Check we can't give the same file more than once (a database
        # constraint).
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'JAC', 'obs', 'REC', 'test',
                            input_file_names=['file1', 'file1'])

        # Check that we can't add a job without either input files or parents
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'JAC', 'obs', 'REC', 'test')
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'JAC', 'obs', 'REC', 'test', input_file_names=[])
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'JAC', 'obs', 'REC', 'test', parent_jobs=[])

        # Check that we can add a job with a list of obsidss. Note
        #that this get the values from jcmt.COMMON, so can't be easily
        # CHecked.

        obs1 = '1-1'
        obs2 = '1-2'

        self.db.set_obsidss(1, [obs1, obs2], replace_all=True)

        self.db.add_job('tag5', 'JAC', 'obs', 'RED', 'test',
                        input_file_names=['file1', 'file2'], obsidss=[obs1, obs2])


        # Check that we can set the tile list when adding a job.
        job_7 = self.db.add_job('tag7', 'JAC', 'obs', 'RED', 'test',
                                input_file_names=['test1'], tilelist=[42])

        self.assertEqual(self.db.get_tilelist(job_7), set([42]))

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

        job_id = self.db.add_job(tag, location, mode, parameters, 'test',
                                 input_file_names=input_file_names)

        # Values to change to.
        newstate = JSAProcState.RUNNING
        message = 'Changed state of job %s to S' % (job_id)
        newstate2 = JSAProcState.WAITING
        message2 = 'Changed state of job %s to %s' % (job_id, newstate2)

        # Get the original  state of job 1.
        job = self.db.get_job(id_=job_id)
        self.assertEqual(job.priority, 0)

        # Change the state of job 1 twice.
        self.db.change_state(job_id, newstate, message)
        self.db.change_state(
            job_id, newstate2, message2, username='testuser', priority=-5)

        # Check the state and previous state of job 1
        job = self.db.get_job(id_=job_id)
        self.assertEqual(job.state, newstate2)
        self.assertEqual(job.state_prev, newstate)
        self.assertEqual(job.priority, -5)

        # Check log for state and messages.
        # (check both get_last_log and get_logs).
        hostname = gethostname().partition('.')[0]
        last_log = self.db.get_last_log(job_id)
        self.assertEqual([last_log.state_new, last_log.state_prev,
                          last_log.message, last_log.host, last_log.username],
                         [newstate2, newstate, message2, hostname, 'testuser'])
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
            self.db.change_state(job_id, JSAProcState.FETCHING, 'test',
                                 state_prev=JSAProcState.WAITING)

        with self.assertRaises(NoRowsError):
            self.db.change_state(job_id, JSAProcState.RUNNING, 'test',
                                 state_prev=JSAProcState.WAITING,
                                 priority=-10)

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
        job_id = self.db.add_job(tag, location, mode, parameters, 'test',
                                 input_file_names=input_file_names,
                                 state=JSAProcState.COMPLETE)

        # Values for testing
        location = 'CADC'
        location2 = 'SomewhereElse'
        foreign_id = 'DummyCADCId'
        foreign_id2 = 'DummyCADCId2'

        # Change the location and foreign_id without changing the state.
        self.db.set_location(job_id, location, foreign_id=foreign_id,
                             state_new=None)

        # Check its changed correctly.
        job = self.db.get_job(id_=job_id)

        self.assertEqual(job.location, location)
        self.assertEqual(job.foreign_id, foreign_id)
        self.assertEqual(job.state, JSAProcState.COMPLETE)

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

        # Check that the state was reset to UNKNOWN.
        self.assertEqual(job.state, JSAProcState.UNKNOWN)

    def test_set_mode(self):
        """
        Test setting the mode of a job.
        """

        job_id = self.db.add_job('test_sm', 'JAC', 'obs', '', 'testtask',
                                 input_file_names=['file1', 'file2'])

        job = self.db.get_job(id_=job_id)

        self.assertEqual(job.mode, 'obs')

        self.db.set_mode(job_id, 'night')

        job = self.db.get_job(id_=job_id)

        self.assertEqual(job.mode, 'night')

    def test_set_paramters(self):
        """
        Test setting the parameters of a job.
        """

        job_id = self.db.add_job('test_sp', 'JAC', 'obs', 'R_P_X', 'testtask',
                                 input_file_names=['file1', 'file2'])

        job = self.db.get_job(id_=job_id)

        self.assertEqual(job.parameters, 'R_P_X')

        self.db.set_parameters(job_id, 'R_P_Y')

        job = self.db.get_job(id_=job_id)

        self.assertEqual(job.parameters, 'R_P_Y')

    def test_set_input_files(self):
        """
        Test setting input files for a job.
        """

        job_id = self.db.add_job('test_sif', 'JAC', 'obs', '', 'testtask',
                                 input_file_names=['file1', 'file2'])

        files = self.db.get_input_files(job_id)
        self.assertEqual(sorted(files), ['file1', 'file2'])

        self.db.set_input_files(job_id, ['file3', 'file4'])

        files = self.db.get_input_files(job_id)
        self.assertEqual(sorted(files), ['file3', 'file4'])

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
        job_id = self.db.add_job(tag, location, mode, parameters, 'test',
                                 input_file_names=input_file_names)

        # Values used in updating.
        output_files1 = [JSAProcFileInfo('myoutputfile1',
                                         '615371394d2240f49c3553a172d4e5fd'),
                         JSAProcFileInfo('myoutputfile2',
                                         '16e3adef1598a3dd36703e488c958e2d')]

        output_files2 = [JSAProcFileInfo('myoutputfile3',
                                         '1e9c97b58f65509464a61cc689ea5ce2'),
                         JSAProcFileInfo('myoutputfile4',
                                         'c10a05bfd8cd7a1d0c1aae71a351b4ad')]

        # Update the output files for this job.
        self.db.set_output_files(job_id, output_files1)

        # Check the values
        out_f = self.db.get_output_files(job_id, with_info=True)
        self.assertEqual(set(out_f), set(output_files1))

        # Re update to check it works when there are already files written in.
        self.db.set_output_files(job_id, output_files2)

        # Check new values
        out_f = self.db.get_output_files(job_id, with_info=True)
        self.assertEqual(set(out_f), set(output_files2))

    def test_output_files(self):
        """
        Test the set_output_files and get_output_files methods.
        """

        # Add a job
        job1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', 'test',
                               ['test1'], priority=2)

        # Add an output file
        outputfiles = ['test.sdf', 'test.png', 'longfilenametahtisrandom.log']
        self.db.set_output_files(
            1,
            [JSAProcFileInfo(x, None) for x in outputfiles])

        addfiles = self.db.get_output_files(1)

        for i in outputfiles:
            self.assertTrue(i in addfiles)

    def test_find_jobs(self):
        """Test the find_jobs method."""

        # Add some jobs.
        job1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', 'test', input_file_names=['test1'],
                               priority=2)  # ?
        job2 = self.db.add_job('tag2', 'JAC',  'obs', 'RECIPE', 'test2', input_file_names=['test1'],
                               priority=4)  # Q
        job3 = self.db.add_job('tag3', 'JAC',  'obs', 'RECIPE', 'test', input_file_names=['test1'],
                               priority=6)  # Q
        job4 = self.db.add_job('tag4', 'CADC', 'obs', 'RECIPE', 'test', input_file_names=['test1'],
                               priority=5)  # Q
        job5 = self.db.add_job('tag5', 'CADC', 'obs', 'RECIPE', 'test', input_file_names=['test1'],
                               priority=3)  # ?
        jobx = self.db.add_job('tagx', 'JAC',  'obs', 'RECIPE', 'test', input_file_names=['test1'],
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

        self.assertEqual(
            set((x.tag for x in self.db.find_jobs(task='test2'))),
            set(('tag2',)))

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

        job6 = self.db.add_job('tag6', 'FAKELOC', 'obs', 'RECIPE', 'test', input_file_names=['test1'],
                               priority=7)
        job7 = self.db.add_job('tag7', 'FAKELOC', 'obs', 'RECIPE', 'test', input_file_names=['test1'],
                               priority=8)
        job8 = self.db.add_job('tag8', 'FAKELOC', 'obs', 'RECIPE', 'test', input_file_names=['test1'],
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
        self.db.set_output_files(1,
                                 [JSAProcFileInfo(x, None) for x in outfiles])
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

    def test_parent_jobs(self):

        # Test it raises an error if no results.
        with self.assertRaises(NoRowsError):
            self.db.get_parents(1)

        # Add 2 jobs to the data base.
        jobid = self.db.add_job('tag1', 'FAKELOC', 'obs', 'RECIPE', 'test',
                                input_file_names=['test1','test2'],
                               priority=7)
        jobid2 = self.db.add_job('tag2', 'FAKELOC', 'obs', 'RECIPE', 'test',
                                 input_file_names=['test3','test4'],
                               priority=7)
        # Add a job that depends on this one
        jobid3 = self.db.add_job('tag3', 'FAKELOC', 'obs', 'RECIPE', 'test',
                                 parent_jobs=[1,2], filters=['850um', '850um'],
                               priority=7)
        # Check you get back the right values
        self.assertEqual(set([(1, '850um'), (2, '850um')]),
                         set(self.db.get_parents(jobid3)))

        # Check you can recover the other way
        self.assertEqual([jobid3], self.db.get_children(jobid))

        # Check you can't add a job with parents that aren't in the database
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'FAKELOC', 'obs', 'RECIPE', 'test',
                            parent_jobs=[5,6], filters='850um', priority=8)

        # Check that you can't add a job that lists itself as a parent
        with self.assertRaises(JSAProcError):
            self.db.add_job('tag4', 'FAKELOC', 'obs', 'RECIPE', 'test',
                            parent_jobs=[4], filters='850um', priority=8)

        # Test that you can delete a single parent job.
        self.db.delete_some_parents(jobid3, [1])
        self.assertEqual(set([(2, '850um')]),
                         set(self.db.get_parents(jobid3)))

        # Test that you can't delete a parent that doesn't exist
        with self.assertRaises(JSAProcError):
            self.db.delete_some_parents(jobid3, [1])

        # Test that you can add a single job.
        self.db.add_to_parents(jobid3, [jobid], filters='450um')
        self.assertEqual(set([(1, '450um'), (2, '850um')]),
                         set(self.db.get_parents(jobid3)))
        # Test that you can delete all parents
        self.db.delete_parents(jobid3)
        with self.assertRaises(NoRowsError):
            self.db.get_parents(jobid3)

        # Test that you can't delete parents if none are present
        with self.assertRaises(JSAProcError):
            self.db.delete_parents(jobid3)


    def test_find_jobs_obsquery(self):

        info_1 = {'obsid': '1', 'obsidss': '1-1', 'utdate': 20140101,
                  'obsnum': 1, 'instrume': 'F', 'backend': 'B',
                  'subsys': 1, 'survey': 'GBS', 'project': 'G01',
                  'date_obs': datetime(2014, 1, 1, 10, 0, 0)}



        info_2 = info_1.copy()
        info_2.update(obsidss='1-2', subsys=2)

        info_3 = info_1.copy()
        info_3.update(obsid='2', obsidss='2-3', survey='DDS', project='D01')

        job_1 = self.db.add_job('tag1', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test1'],
                                obsidss=[info_1['obsidss']])
        job_2 = self.db.add_job('tag2', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test2'],
                                obsidss=[info_2['obsidss']])
        job_3 = self.db.add_job('tag3', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test1', 'test2'],
                                obsidss=[info_1['obsidss'], info_2['obsidss']])
        job_4 = self.db.add_job('tag4', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test3'],
                                obsidss=[info_3['obsidss']])
        job_5 = self.db.add_job('tag5', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test2', 'test3'],
                                obsidss=[info_2['obsidss'], info_3['obsidss']])

        info_4 = info_1.copy()
        info_4.update(obsid='3', obsidss='3-4',survey=None, project='XX01')

        info_5 = info_4.copy()
        info_5.update(obsid='4', obsidss='4-5',project='JCMTCAL')

        info_6 = info_4.copy()
        info_6.update(obsid= '5', obsidss='5-6', project='CAL')

        job_6 = self.db.add_job('tag6', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test4'],
                                obsidss=[info_4['obsidss']])
        job_7 = self.db.add_job('tag7', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test5'],
                                obsidss=[info_5['obsidss']])
        job_8 = self.db.add_job('tag8', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test6'],
                                obsidss=[info_6['obsidss']])

        info_7 = info_1.copy()
        info_7.update(obsid= '6', obsidss='6-7', project='JCMTCAL')

        job_9 = self.db.add_job('tag9', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test7'],
                                obsidss=[info_7['obsidss']])

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
            try:
                results = set(x.id for x in self.db.find_jobs(obsquery=oq))
                self.assertEqual(
                    results,
                    set(expect))
            except:
                print(oq, expect, results)
                raise

            self.assertEqual(
                self.db.find_jobs(count=True, obsquery=oq),
                len(expect))


    def test_processing_time(self):
        job_id = self.db.add_job('tag1', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test1'])

        self.db.change_state(job_id, JSAProcState.RUNNING, 'start')
        self.db.change_state(job_id, JSAProcState.PROCESSED, 'end')

        # Check that we find this job:
        (ids, duration, obsinfo) = self.db.get_processing_time_obs_type()

        self.assertEqual(len(ids), 1)
        self.assertEqual(len(duration), 1)
        self.assertEqual(len(obsinfo), 1)

        # Check that it has no obsinfo but is the correct length
        self.assertEqual([i for i in obsinfo[0]], [None]*5 + [0])

        # Check we don't find the job if it's running or in the error state:
        self.db.change_state(job_id, JSAProcState.RUNNING, 'start again')
        self.assertEqual(self.db.get_processing_time_obs_type()[0], [])

        self.db.change_state(job_id, JSAProcState.ERROR, 'failure')
        self.assertEqual(self.db.get_processing_time_obs_type()[0], [])

        # Add some more jobs.
        job_2 = self.db.add_job('tag2', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test1'])
        job_3 = self.db.add_job('tag3', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test1'])
        job_4 = self.db.add_job('tag4', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test1'])
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

    def test_taskinfo(self):
        self.db.add_task('testtask', True, 'mystarpath')
        self.db.add_task('testtask2', False, raw_output=True)
        self.db.add_task('testtask3', None, 'mystarpath', 1, raw_output=False)
        self.db.add_task('testtask4', None, 'myotherstarpath', 2,
                         command_run='custom_run_command',
                         command_xfer='custom_xfer_command',
                         command_ingest='custom_ingest_command')
        self.assertEqual(self.db.get_task_info('testtask').starlink_dir,
                         'mystarpath')
        self.assertTrue(self.db.get_task_info('testtask').etransfer)
        self.assertIsNone(self.db.get_task_info('testtask').version)
        self.assertIsNone(self.db.get_task_info('testtask').raw_output)
        self.assertIsNone(self.db.get_task_info('testtask2').starlink_dir)
        self.assertFalse(self.db.get_task_info('testtask2').etransfer)
        self.assertIsNone(self.db.get_task_info('testtask2').version)
        self.assertTrue(self.db.get_task_info('testtask2').raw_output)
        self.assertIsNone(self.db.get_task_info('testtask3').etransfer)
        self.assertFalse(self.db.get_task_info('testtask3').raw_output)
        self.assertEqual(self.db.get_task_info('testtask3').starlink_dir,
                         'mystarpath')
        self.assertEqual(self.db.get_task_info('testtask3').version, 1)
        self.assertIsNone(self.db.get_task_info('testtask3').command_run)
        self.assertIsNone(self.db.get_task_info('testtask3').command_xfer)
        self.assertIsNone(self.db.get_task_info('testtask3').command_ingest)
        self.assertIsNone(self.db.get_task_info('testtask4').etransfer)
        self.assertEqual(self.db.get_task_info('testtask4').starlink_dir,
                         'myotherstarpath')
        self.assertEqual(self.db.get_task_info('testtask4').version, 2)
        self.assertEqual(self.db.get_task_info('testtask4').command_run,
                         'custom_run_command')
        self.assertEqual(self.db.get_task_info('testtask4').command_xfer,
                         'custom_xfer_command')
        self.assertEqual(self.db.get_task_info('testtask4').command_ingest,
                         'custom_ingest_command')

        with self.assertRaises(NoRowsError):
            self.db.get_task_info('notatask')

        # Try getting info for all tasks at once: should get a dictionary
        # containing sensible entries, one for each task.
        result = self.db.get_task_info()
        self.assertIsInstance(result, dict)

        expected_tasks = ['testtask', 'testtask2', 'testtask3', 'testtask4']
        self.assertEqual(sorted(result.keys()), expected_tasks)
        for task in expected_tasks:
            task_info = result[task]
            self.assertIsInstance(task_info, JSAProcTaskInfo)
            self.assertIsInstance(task_info.id, int)
            self.assertEqual(task_info.taskname, task)

    def test_tilelist(self):
        job_id = self.db.add_job('tag1', 'JAC', 'obs', 'RECIPE', 'test', input_file_names=['test1'])

        # Start with no tilelist.
        self.assertEqual(self.db.get_tilelist(job_id), set([]))

        # Add tiles
        tiles = set((42, 43, 44))
        self.db.set_tilelist(job_id, tiles)
        self.assertEqual(set(self.db.get_tilelist(job_id)), set(tiles))

        # Change tiles
        newtiles = set((45, 46, 47))
        self.db.set_tilelist(job_id, newtiles)
        self.assertEqual(set(self.db.get_tilelist(job_id)), set(newtiles))

    def test_get_tasks(self):
        with self.assertRaises(NoRowsError):
            self.db.get_tasks()

        self.db.add_job('tag1', 'JAC', 'obs', 'REC', 'test1', input_file_names=['test1'])
        self.db.add_job('tag2', 'JAC', 'obs', 'REC', 'test2', input_file_names=['test1'])

        self.assertEqual(self.db.get_tasks(), ['test1', 'test2'])

    def test_qa(self):
        self.db.add_job('tag1', 'JAC', 'obs', 'REC', 'test1', input_file_names=['test1'])
        self.db.add_job('tag2', 'JAC', 'obs', 'REC', 'test2', input_file_names=['test1'])
        self.db.add_qa_entry(1, 'B', 'Testing qa entry', 'testUser')
        self.db.add_qa_entry(1, 'G', 'Testing qa entry', 'testUser')
        self.db.add_qa_entry(1, 'Q', 'Testing qa entry', 'testUser')
        self.assertEqual(self.db.get_last_qa(1).status, 'Q')
        self.assertEqual(self.db.get_last_qa(1).message, 'Testing qa entry')
        self.assertEqual(self.db.get_last_qa(1).username, 'testUser')

        # Test changing the state of job to one in running updates the QA state
        self.db.change_state(1, newstate=JSAProcState.RUNNING, message='Testing changing state')
        self.assertEqual(self.db.get_job(1).qa_state, JSAQAState.UNKNOWN)
        self.assertEqual(len(self.db.get_qas(1)), 4)
        self.assertEqual(len(self.db.get_qas(2)), 0)

    def test_get_date_range(self):
        with self.assertRaises(NoRowsError):
            self.db.get_date_range()

        job_1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', 'test', input_file_names=['test1'])
        info = {'obsid': 'x14_01_1T1', 'obsidss': 'x14_1_1T1_850',
                'utdate': 20140101, 'obsnum': 3,
                'instrument': 'SCUBA-2',
                'backend': 'ACSIS', 'subsys': '1',
                'date_obs': datetime(2014, 1, 1, 9, 0, 0)}
        with self.db.db as c:
            c.execute('INSERT INTO jcmt.FILES (file_id, obsid, subsysnr, nsubscan, obsid_subsysnr) ' +
                      'VALUES ("testfile.sdf", %s, %s, 100, %s)',
                      (info['obsid'], info['subsys'], info['obsidss']))
            c.execute('INSERT INTO jcmt.COMMON (obsid, utdate, obsnum, instrume, backend, date_obs) ' +
                      'VALUES (%s, %s, %s, %s, %s, %s)',
                      (info['obsid'], info['utdate'], info['obsnum'], info['instrument'],
                       info['backend'], info['date_obs']))


        self.db.set_obsidss(job_1, [info['obsidss']])

        self.assertEqual( (info['utdate'], info['utdate']), self.db.get_date_range())

        self.assertEqual((info['utdate'], info['utdate']), self.db.get_date_range(task='test'))

        info2 = {'obsid': 'ax14_01_1T1', 'obsidss': 'ax14_1_1T1_850',
                'utdate': 20140501, 'obsnum': 3,
                'instrument': 'SCUBA-2',
                'backend': 'ACSIS', 'subsys': '1',
                'date_obs': datetime(2014, 5, 1, 9, 0, 0)}
        with self.db.db as c:
            c.execute('INSERT INTO jcmt.FILES (file_id, obsid, subsysnr, nsubscan, obsid_subsysnr) ' +
                      'VALUES ("testfile.sdf", %s, %s, 100, %s)',
                      (info2['obsid'], info2['subsys'], info2['obsidss']))
            c.execute('INSERT INTO jcmt.COMMON (obsid, utdate, obsnum, instrume, backend, date_obs) ' +
                      'VALUES (%s, %s, %s, %s, %s, %s)',
                      (info2['obsid'], info2['utdate'], info2['obsnum'], info2['instrument'],
                       info2['backend'], info2['date_obs']))
        self.db.set_obsidss(job_1, [info['obsidss'], info2['obsidss']])

        self.assertEqual((info['utdate'], info2['utdate']), self.db.get_date_range())
        self.assertEqual((info['utdate'], info2['utdate']), self.db.get_date_range(task='test'))
        job_2 = self.db.add_job('tag2', 'JAC',  'obs', 'RECIPE', 'test2', input_file_names=['test1'])

        info3 = {'obsid': 'bx14_01_1T1', 'obsidss': 'bx14_1_1T1_850',
                 'utdate': 20130501, 'obsnum': 3,
                 'instrument': 'SCUBA-2',
                 'backend': 'ACSIS', 'subsys': '1',
                 'date_obs': datetime(2013, 5, 1, 9, 0, 0)}
        with self.db.db as c:
            c.execute('INSERT INTO jcmt.FILES (file_id, obsid, subsysnr, nsubscan, obsid_subsysnr) ' +
                      'VALUES ("testfile.sdf", %s, %s, 100, %s)',
                      (info3['obsid'], info3['subsys'], info3['obsidss']))
            c.execute('INSERT INTO jcmt.COMMON (obsid, utdate, obsnum, instrume, backend, date_obs) ' +
                      'VALUES (%s, %s, %s, %s, %s, %s)',
                      (info3['obsid'], info3['utdate'], info3['obsnum'], info3['instrument'],
                       info3['backend'], info3['date_obs']))

        info4 = {'obsid': 'cx14_01_1T1', 'obsidss': 'cx14_1_1T1_850',
                'utdate': 20130101, 'obsnum': 3,
                'instrument': 'SCUBA-2',
                'backend': 'ACSIS', 'subsys': '1',
                'date_obs': datetime(2013, 1, 1, 9, 0, 0)}
        with self.db.db as c:
            c.execute('INSERT INTO jcmt.FILES (file_id, obsid, subsysnr, nsubscan, obsid_subsysnr) ' +
                      'VALUES ("testfile.sdf", %s, %s, 100, %s)',
                      (info4['obsid'], info4['subsys'], info4['obsidss']))
            c.execute('INSERT INTO jcmt.COMMON (obsid, utdate, obsnum, instrume, backend, date_obs) ' +
                      'VALUES (%s, %s, %s, %s, %s, %s)',
                      (info4['obsid'], info4['utdate'], info4['obsnum'], info4['instrument'],
                       info4['backend'], info4['date_obs']))
        self.db.set_obsidss(job_2, [info3['obsidss'], info4['obsidss']])
        self.assertEqual((info4['utdate'], info2['utdate']), self.db.get_date_range())
        self.assertEqual((info4['utdate'], info3['utdate']), self.db.get_date_range(task='test2'))


    def test_find_errors_logs(self):
        """
        Check the db.find_errors_logs function
        """
        job_1 = self.db.add_job('tag1', 'JAC',  'obs', 'RECIPE', 'test', input_file_names=['test1'])
        job_2 = self.db.add_job('tag2', 'CANFAR',  'obs', 'RECIPE', 'test1', input_file_names=['test1'])
        # Values to change to.
        newstate = JSAProcState.RUNNING
        message = 'Changed state of job %s to S' % (job_1)
        newstate2 = JSAProcState.ERROR
        message2 = 'Changed state of job %s to %s' % (job_1, newstate2)
        self.db.change_state(job_1, newstate, message)
        self.db.change_state(job_1, newstate2, message2)
        newstate3 = JSAProcState.QUEUED
        message3 = 'Changed state of job %s to S' % (job_2)
        newstate4 = JSAProcState.ERROR
        message4 = 'Changed state of job %s to %s' % (job_2, newstate4)
        self.db.change_state(job_2, newstate3, message3)
        self.db.change_state(job_2, newstate4, message4)

        elog_all = self.db.find_errors_logs()
        ej1 = elog_all[job_1]
        self.assertEqual((ej1[1].message, ej1[1].state, ej1[1].location, ej1[1].id),
                         (message, newstate, 'JAC', job_1))
        self.assertEqual((ej1[0].message, ej1[0].state, ej1[0].location, ej1[0].id),
                         (message2, newstate2, 'JAC', job_1))
        ej2 = elog_all[job_2]
        self.assertEqual((ej2[1].message, ej2[1].state, ej2[1].location, ej2[1].id),
                         (message3, newstate3, 'CANFAR', job_2))
        self.assertEqual((ej2[0].message, ej2[0].state, ej2[0].location, ej2[0].id),
                         (message4, newstate4, 'CANFAR', job_2))

        elog_canfar = self.db.find_errors_logs(location='CANFAR')
        ej2 = elog_canfar[job_2]
        self.assertEqual((ej2[1].message, ej2[1].state, ej2[1].location, ej2[1].id),
                         (message3, newstate3, 'CANFAR', job_2))
        self.assertEqual((ej2[0].message, ej2[0].state, ej2[0].location, ej2[0].id),
                         (message4, newstate4, 'CANFAR', job_2))
        with self.assertRaises(KeyError):
            ej1 = elog_canfar[job_1]

        elog_test1 = self.db.find_errors_logs(task='test1')
        ej2 = elog_test1[job_2]
        self.assertEqual((ej2[1].message, ej2[1].state, ej2[1].location, ej2[1].id),
                         (message3, newstate3, 'CANFAR', job_2))
        self.assertEqual((ej2[0].message, ej2[0].state, ej2[0].location, ej2[0].id),
                         (message4, newstate4, 'CANFAR', job_2))
        with self.assertRaises(KeyError):
            ej1 = elog_test1[job_1]

    def test_note(self):
        job_id = self.db.add_job('noteTest', 'JAC', 'obs', '', 'test',
                                 input_file_names=['file1'])

        self.db.add_note(job_id, 'Note 1', 'user1')
        self.db.add_note(job_id, 'Note 2', 'user2')

        notes = self.db.get_notes(job_id)

        self.assertEqual(len(notes), 2)
        self.assertEqual(notes[0].message, 'Note 2')
        self.assertEqual(notes[0].username, 'user2')
        self.assertEqual(notes[1].message, 'Note 1')
        self.assertEqual(notes[1].username, 'user1')

    def test_obs_preproc(self):
        self.assertIsNone(self.db.get_obs_preproc_recipe(
            'acsis_00047_20191222T145926'))

        with self.db.db as c:
            c.execute(
                'INSERT INTO obs_preproc (obsid, recipe) '
                'VALUES ("acsis_00047_20191222T145926", "FIX_HEADER_IFFREQ")')

        self.assertEqual(self.db.get_obs_preproc_recipe(
            'acsis_00047_20191222T145926'), 'FIX_HEADER_IFFREQ')

        self.assertIsNone(self.db.get_obs_preproc_recipe(
            'acsis_99999_20191222T145926'))


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
    return dict(sum((list(d.items()) for d in args), []))
