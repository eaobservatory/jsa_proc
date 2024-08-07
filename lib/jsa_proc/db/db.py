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

from collections import defaultdict, namedtuple, OrderedDict
import logging
import re
from socket import gethostname

# Python2/3 compatability:
try:
    basestring
except NameError:
    basestring = str

from getpass import getuser

from jsa_proc.error import \
    JSAProcError, JSAProcDBError, NoRowsError, ExcessRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.qa_state import JSAQAState

logger = logging.getLogger(__name__)

# Named tuples that are created ahead of time instead of dynamically
# defined from table rows:
JSAProcLog = namedtuple(
    'JSAProcLog',
    'id job_id datetime state_prev state_new message host username')
JSAProcQa = namedtuple(
    'JSAProcQa',
    'id job_id datetime status  message username')

JSAProcJob = namedtuple(
    'JSAProcJob',
    ['id', 'tag', 'state', 'state_prev', 'location', 'foreign_id', 'mode',
     'parameters', 'priority', 'task', 'qa_state'])
JSAProcObs = namedtuple(
    'JSAProcObs',
    ['job_id', 'obsid', 'obsidss', 'date_obs', 'date_end', 'utdate',
     'obsnum', 'instrument', 'backend', 'project', 'survey', 'subsys',
     'scanmode', 'sourcename', 'obstype', 'omp_status',
     'tau', 'seeing'])
JSAProcJobInfo = namedtuple(
    'JSAProcJobInfo',
    'id tag state location foreign_id task qa_state outputs')
JSAProcErrorInfo = namedtuple(
    'JSAProcErrorInfo',
    'id time message state state_prev location')
JSAProcFileInfo = namedtuple(
    'FileInfo',
    'filename md5')
JSAProcJobNote = namedtuple(
    'JSAProcJobNote',
    'id message username')
JSAProcTaskInfo = namedtuple(
    'JSAProcTaskInfo',
    'id taskname etransfer starlink_dir version command_run command_xfer raw_output command_ingest log_ingest')

# Regular expressions to be used to check pieces of SQL being generated
# automatically.
valid_column = re.compile('^[a-z0-9_]+$')


class Not:
    """Class representing negative conditions.

    Instances contain a single attribute "value" which is the value
    with which the object was constructed.
    """

    def __init__(self, value):
        """Create new Not object containing the given value.
        """

        self.value = value


class Fuzzy:
    """Class for fuzzy string match conditions.
    """

    def __init__(self, value, wildcards=True):
        """Construct new fuzzy string match object."""

        self.value = value
        self.wildcards = wildcards


class Range:
    """Class representing range conditions."""

    def __init__(self, min, max):
        """Create a new range object."""

        self.min = min
        self.max = max

    def __iter__(self):
        """Iterate over the min and max values."""
        yield self.min
        yield self.max


class JSAProcDB:
    """
    JSA Processing database access class.

    This is an abstract class -- only database-specific subclasses
    may be constructed.
    """

    def __init__(self):
        """Base class constructor.

        This checks that the subclass has created a "db" attribute.
        """

        assert (hasattr(self, 'db'))

    def get_job(self, id_=None, tag=None):
        """
        Get a JSA data processing job from the database.

        Requires either the tag or id of the job table, and raises a
        JSAProcDBError if one or the other is not provided. If the tag
        or id does not exist in the database than it needs to raise an
        error. If both are set it does not raise an error, but will use
        the id value.

        id_: integer

        tag: string

        Returns: namedtuple of values of all columns in job database.
        """

        if (id_ is None) and (tag is None):
            raise JSAProcError("You must set either id_ or tag to use get_job")

        if id_ is not None:
            name = 'id'
            value = id_
        else:
            name = 'tag'
            value = tag

        with self.db as c:
            job = self._get_job(c, name, value)

        return job

    def _get_job(self, c, name, value):
        """
        Private function to get a job from the database.

        name must be 'id' or 'tag'
        value is either the integer job_id or the string tag for
        the job you want to get.

        Takes in a cursor instance "c" (assumes you already
        have a cursor lock).

        Returns JSAProcJob named tuple.
        """
        # Get the values form the database
        c.execute(
            'SELECT ' +
            ', '.join(JSAProcJob._fields) +
            ' FROM job WHERE ' + name + '=%s', (value,))
        job = c.fetchall()
        if len(job) == 0:
            raise NoRowsError(
                'job',
                'SELECT * FROM job WHERE ' + name + '=' + str(value))
        if len(job) > 1:
            raise ExcessRowsError(
                'job',
                'SELECT * FROM job WHERE ' + name + '=' + str(value))

        # Turn list into single item
        job = job[0]

        # Turn job into namedtuple
        job = JSAProcJob(*job)

        return job

    def add_job(self, tag, location, mode, parameters, task,
                input_file_names=None, parent_jobs=None, filters=None,
                foreign_id=None, state='?',
                priority=0, obsidss=None, tilelist=None):
        """
        Add a JSA data processing job to the database.

        This will raise an error if the job already exists, if the
        database interface raises an error. The job must be specified
        by its unique tag.

        If the job creation is successful, an entry will be added to the
        log table to record this event.

        Parameters:

        tag: string, unique identifier for observation/job

        location: string, where the job will be run.

        mode: JSA processing mode (obs / night / project / public),

        parameters: processing parameters to pass to jsawrapdr
        (typically the recipe name). (string)

        input_file_names: OPTIONAL iterable, each item being a string that
        identifies the name of an input file for the job.

        parent_jobs: OPTIONAL, iterable, each item being the integer
        job id of a parent job in this database.

        [ 1 of input_file_name or parent_jobs must be provided]

        filters: OPTIONAL, either a string or a list of strings.

        If a string, it is a regular expression string to select only
        the correct filenames from the output files of all the parent
        job.

        If it is an iterable, then each item is a regular expression
        string to select the correct files from the output files for a
        job, and it must be the samelength as the list of parent_jobs,
        and each item is the filter for that specific job.

        foreign_id: OPTIONAL, default=None. (string), identifier from
        foreign system (probably  CADC).

        state: initial job state (character, default ?).

        priority: priority number (integer, default 0, higher number
        represents greater priority).

        task: name of data processing project (string).

        tilelist: optional, list of integers.
        The list of tiles this job will produce.

        obsidss: optional, list
        A list of obsidss, as found in the jcmt FILES, SCUBA2 or ACSIS tables.

        Returns the job identifier.
        """

        # Validate input.
        if not JSAProcState.is_valid(state):
            raise JSAProcError('State {0} is not recognised'.format(state))

        if not parent_jobs and not input_file_names:
            raise JSAProcError(
                'A Job must have either input files or parent jobs')

        if parent_jobs:
            job_id, parents, filters = _validate_parents(None, parent_jobs,
                                                         filters=filters)

        with self.db as c:
            # Check if the tag already exists.  The database constraints
            # should already check for this, but with MySQL's InnoDB
            # engine, a job number is allocated (and lost) if the
            # insert constraint fails.
            c.execute('SELECT COUNT(*) FROM job WHERE tag=%s', (tag,))
            row = c.fetchone()
            if row[0] != 0:
                raise JSAProcError('a job already exists with the same tag: ' +
                                   tag)

            # Insert job into table
            c.execute(
                'INSERT INTO job '
                '(tag, state, location, mode, parameters, '
                'foreign_id, priority, task) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (tag, state, location, mode, parameters, foreign_id,
                 priority, task))

            # Get the autoincremented id from job table (job_id in all other
            # tables).
            job_id = c.lastrowid

            # Add parent jobs to parent table with filters.
            if parent_jobs:
                # Check job_id is not contained within parent_list
                if job_id in parent_jobs:
                    raise JSAProcError('Cannot insert a job as its own parent')

                self._insert_parents(job_id, c, parent_jobs, filters)

            # Add input file names to input_file table.
            if input_file_names:
                self._set_input_files(c, job_id, input_file_names)

            # Log the job creation
            self._add_log_entry(c, job_id, JSAProcState.UNKNOWN, state,
                                'Job added to the database', None)

            # If present, insert the tile list.
            if tilelist:
                self._set_tilelist(c, job_id, tilelist)

            # If present, replace/update the observation list.
            if obsidss:
                self._set_obsidss(c, job_id, obsidss, False)

        # job_id may not be necessary but sometimes useful.
        return job_id

    def get_tilelist(self, job_id=None, task=None):
        """Retrieve the unique list of tiles.
        OPtionally filtered either by job_id or by task (or both)

        Returns a set.
        """

        tiles = []
        query = 'SELECT DISTINCT(tile) FROM tile'
        where = ()
        params = ()
        join = ''
        if job_id:
            where += ('job_id = %s',)
            params += (job_id,)
        if task:
            where += ('job.task = %s',)
            join = ' JOIN job ON tile.job_id = job.id'
            params += (task,)
        query = query + join
        if where:
            query += ' WHERE ' + ' '.join(where)

        with self.db as c:
            c.execute(query + ' ORDER BY tile ', params)

            while True:
                row = c.fetchone()
                if row is None:
                    break

                tiles.append(row[0])

        return set(tiles)

    def set_tilelist(self, job_id, tiles):
        """
        Delete and replace the entries for job_id in the tiles tables.

        job_id: integer required.

        tiles: list of integer, required
        """
        with self.db as c:
            self._set_tilelist(c, job_id, tiles)

    def _set_tilelist(self, c, job_id, tiles):
        c.execute('DELETE FROM tile WHERE job_id = %s', (job_id,))
        for tile in tiles:
            c.execute('INSERT INTO tile (job_id, tile) '
                      'VALUES (%s, %s)',
                      (job_id, tile))

    def change_task(self, job_id, oldtask, newtask):
        """
        Move a job from one task (oldtask) to another (newtask).

        This will raise an error if the job can't be found in oldtask.
        """
        if oldtask == newtask:
            raise JSAProcError(
                "Can't change task if oldtask (%s) "
                "is the same as newtask (%s)" %
                (oldtask, newtask))
        with self.db as c:
            query = 'UPDATE job SET task=%s WHERE id=%s AND task=%s'
            params = (newtask, job_id, oldtask,)
            c.execute(query, params)
            if c.rowcount == 0:
                raise NoRowsError('job', query % tuple(params))
        logger.debug(
            'Moved job %i from task %s to task %s',
            job_id, oldtask, newtask)

    def get_obs_info(self, job_id):
        """
        Get all entries in the obs table for a given job_id.

        job_id: integer, required

        returns:

        List of NamedTuples with entries from obs table.
        """
        logger.debug('Getting observation info for job %i', job_id)
        with self.db as c:
            # Get all observations with job_id
            self.db.unlock()
            c.execute(
                'SELECT job_id, obsidss.obsid, obsidss.obsid_subsysnr, date_obs, date_end, utdate, ' +
                ' obsnum, ' +
                ' CASE WHEN instrume="SCUBA-2" AND inbeam like "%%POL" THEN "POL-2" ELSE instrume END as instrume, ' +
                ' backend, project, survey, obsidss.subsys, '+
                " CASE WHEN jcmt.COMMON.sam_mode='SCAN' THEN jcmt.COMMON.scan_pat ELSE jcmt.COMMON.sam_mode END AS scanmode, " +
                ' object, obs_type, ' +
                " CASE WHEN o.commentstatus is NULL THEN 0 ELSE o.commentstatus END AS omp_status, " +
                " (wvmtaust + wvmtauen)/2.0 AS tau, " +
                " (seeingst + seeingen)/2.0 AS seeing " +
                ' FROM obsidss LEFT JOIN jcmt.COMMON  ON obsidss.obsid=jcmt.COMMON.obsid ' +
                ' LEFT OUTER JOIN omp.ompobslog AS o ON o.obslogid = (SELECT MAX(obslogid) FROM omp.ompobslog AS o2 WHERE o2.obsid=jcmt.COMMON.obsid ) ' +
                ' WHERE job_id = %s '
                ' ORDER BY utdate ASC, obsnum ASC',
                (job_id,))
            results = c.fetchall()

        results = [JSAProcObs(*obs) for obs in results]

        return results

    def update_obs_info(self, obsidss, obsinfodict):
        """
        update the columns and values given in the obsinfodict
        for all observations with a given obsidss.
        """
        columnnames, values = zip(*obsinfodict.items())
        for column in columnnames:
            if column in ('job_id', 'id', 'obsidss'):
                raise JSAProcError('Cannot insert private column name '
                                   + column + ' into obs table')

        # Escape column names with back ticks.
        columnnames = ['`' + i + '`=%s' for i in columnnames]
        column_query = ','.join(columnnames)

        with self.db as c:

            query = ('UPDATE obs SET ' + column_query +
                     ' WHERE obsidss=%s ')
            params = values + (obsidss,)
            logging.debug(query % params)
            c.execute(query, params)

    def set_obsidss(self, job_id, obsidss, replace_all=True):
        """
        Update the obs table with additional observations for a given job.

        job_id: integer, required

        obsidss: list of obsidss values.


        replace_all: Boolean, default True

        If set True, delete all existing entries for the job_id before
        updating the table with the obsinfo dictionaries.
        """
        with self.db as c:
            self._set_obsidss(c, job_id, obsidss, replace_all)

    def _set_obsidss(self, c, job_id, obsidss, replace_all):

        # If replace_all is set, then delete the existing observations.
        if replace_all:
            c.execute('DELETE FROM obsidss WHERE job_id = %s', (job_id,))

        # Get the obsid from jcmt.FILES
        self.db.unlock()

        query = 'SELECT obsid_subsysnr, obsid, subsysnr FROM jcmt.FILES WHERE obsid_subsysnr IN ({0}) GROUP BY obsid_subsysnr, obsid'.format(
            ', '.join(('%s',)*len(obsidss)))

        c.execute(query, tuple(obsidss))

        results = c.fetchall()


        # Check if any are missing, and warn if so.
        obsidss_fromquery = [i[0] for i in results]
        for o in obsidss:
            if o not in obsidss_fromquery:
                logger.warning(
                    'OBSIDSS %s was not added to job %i as no matching OBSID was found in jcmt FILES Table',
                    o, job_id)

        # Go through each pair of obsid_subsysnr and obsid, and insert
        # into obsidss for current job.
        for (obsid_subsysnr, obsid, subsys) in results:

            c.execute('INSERT INTO obsidss (job_id, obsid_subsysnr, obsid, subsys) ' +
                      ' VALUES (%s, %s, %s, %s)' ,
                      (job_id, obsid_subsysnr, obsid, subsys))


    def change_state(self, job_id, newstate, message, state_prev=None,
                     username=None, priority=None):

        """
        Change the state of a job in the JSA processing database.

        the id is the id in the job table, which uniquely identifies a
        job. The newstate will be written into the state column, and
        the current state will be moved over to the state_prev column.

        The log table will be updated with a line corresponding to
        this change, using the message.

        Parameters:
        id: integer, job_id of the job whose state is being changed.

        newstate: string, one character, new state of the job.

        message: string, human readable text describin the change of state.

        state_prev: the state we expect the job to currently be in
        (optional character).  If this is specified and the job is
        not already in that state, an error is raised.

        Return:
        job_id, integer

        """

        with self.db as c:
            self._change_state(c, job_id, newstate, message, state_prev,
                               username, priority)

    def _change_state(self, c, job_id, newstate, message, state_prev,
                      username, priority):
        # Validate input.
        if not JSAProcState.is_valid(newstate):
            raise JSAProcError('State {0} is not recognised'.format(newstate))

        # Change the state to new state and update the state_prev
        update_expr = ['state_prev = state', 'state = %s']
        update_param = [newstate]

        where_expr = ['id = %s']
        where_param = [job_id]

        if state_prev is not None:
            where_expr.append('state = %s')
            where_param.append(state_prev)

        if priority is not None:
            update_expr.append('priority = %s')
            update_param.append(priority)

        query = 'UPDATE job SET {} WHERE {}'.format(
            ', '.join(update_expr),
            ' AND '.join(where_expr))
        param = update_param + where_param

        c.execute(query, param)

        if c.rowcount == 0:
            # (With MySQL) this might be because the job was already in the
            # requested state.  Check this now (rather than before since
            # we expect the state to be changing most times this method is
            # called).
            if ((self._get_job(c, 'id', job_id).state == newstate) and
                    ((state_prev is None) or (state_prev == newstate))):
                logger.warning('Job %i already in state %s', job_id, newstate)
            else:
                raise NoRowsError('job', query % tuple(param))

        elif c.rowcount > 1:
            raise ExcessRowsError('job', query % tuple(param))

        # Get state_prev value if we were not given it.
        if state_prev is None:
            c.execute('SELECT state_prev FROM job WHERE id=%s',
                      (job_id,))
            state_prev = c.fetchall()

            if len(state_prev) > 1:
                raise ExcessRowsError(
                    'job',
                    'SELECT state_prev FROM job WHERE id=%s' % (job_id))

            state_prev = state_prev[0][0]

        # Update log table.
        self._add_log_entry(c, job_id, state_prev, newstate, message, username)

        # Update QA table if appropriate
        if newstate in JSAProcState.STATE_PRE_QA:
            # Check the current QA state:
            job = self._get_job(c, 'id', job_id)

            # If a non-unknown QA state has been set, change it to unknown
            # and update the qa table.
            if job.qa_state != JSAQAState.UNKNOWN:
                c.execute('UPDATE job SET qa_state = %s WHERE id= %s',
                          (JSAQAState.UNKNOWN, job_id))
                self._add_qa_entry(c, job_id, JSAQAState.UNKNOWN,
                                   'This job is being reprocessed;' +
                                   ' QA state reset automatically.',
                                   getuser())

    def get_input_files(self, job_id):
        """
        Get the list of input files for specific job from the
        input_file table.

        takes integer job_id to identify file (this is the
        auto-incremented primary key from the job table)

        Returns a list of file names.
        """

        with self.db as c:
            c.execute('SELECT filename FROM input_file WHERE job_id=%s',
                      (job_id,))
            input_files = c.fetchall()

            if len(input_files) == 0:
                raise NoRowsError(
                    'input_file',
                    'SELECT filename FROM input_file WHERE job_id = ' +
                    (str(job_id)))

        # input_files will be a list of tuples, each tuple containgin
        # one file. Flatten this into a list of strings.
        input_files = [file for i in input_files for file in i]

        return input_files

    def set_input_files(self, job_id, input_files):
        """
        Set the list of input files for a specific job.
        """

        with self.db as c:
            self._set_input_files(c, job_id, input_files)

    def _set_input_files(self, c, job_id, input_files):
        # Remove any current input files for this job_id.
        c.execute('DELETE FROM input_file WHERE job_id = %s', (job_id,))

        # Insert the new input file records.
        for filename in input_files:
            c.execute('INSERT INTO input_file (job_id, filename) '
                      'VALUES (%s, %s)',
                      (job_id, filename))

    def _add_log_entry(self, c, job_id, state_prev, state_new, message,
                       username):
        """Private method to add an entry to the log table.

        Assumes the database is already locked an takes a cursor
        object as argument "c".
        """

        if username is None:
            username = getuser()

        c.execute('INSERT INTO log '
                  '(job_id, state_prev, state_new, message, host, username) '
                  'VALUES (%s, %s, %s, %s, %s, %s)',
                  (job_id, state_prev, state_new, message,
                   gethostname().partition('.')[0], username))

    def _add_qa_entry(self, c, job_id, status, message, username):
        """
        Private method: adds an entry to the qa table for a job with job_id.

        Assumes the database is already locked and takes a cursor
        object as argument "c"
        """

        # Check status is allowed
        c.execute('INSERT INTO qa '
                  '(job_id, status, message, username) '
                  'VALUES (%s, %s, %s, %s)',
                  (job_id, status, message, username))

    def add_qa_entry(self, job_id, status, message, username):
        """
        Add an entry to the QA table for a job of given job_id, and
        update the qa_state in the jobtable for the given job_id.

        Status must be in JSAQAState.STATE_ALL
        """
        if status in JSAQAState.STATE_ALL:
            with self.db as c:
                self._add_qa_entry(c, job_id, status, message, username)
                c.execute('UPDATE job SET qa_state = %s WHERE id = %s',
                          (status, job_id))
        else:
            raise JSAProcError(
                'QA status can only be changed to allowed values.')

    def get_logs(self, job_id):
        """
        Get the full log of states of a given job from the log table.

        Parameters:
        job_id : integer (id from job table)

        Returns:
        list of JSAProcLog nametuples, 1 entry per row in log table for that
        job_id.
        """
        # Get all log entries
        logs = self._get_all_entries(job_id, 'log')
        # Create JSAProcLog namedtuple object to hold values.
        logs = [JSAProcLog(*i) for i in logs]

        return logs

    def get_qas(self, job_id):
        """
        Get the full history of qa states of a given job from the qa table.

        Parameters:
        job_id : integer (id from job table)

        Returns:
        list of JSAProcQa nametuples, 1 entry per row in qa table for that
        job_id.
        """
        # Get all qa entries
        qas = self._get_all_entries(job_id, 'qa')

        # Create JSAProcLog namedtuple object to hold values.
        qas = [JSAProcQa(*i) for i in qas]

        return qas

    def _get_all_entries(self, job_id, tablename):
        """
        Private method to get all entries in a table with a
        given job_id.
        """
        with self.db as c:
            c.execute('SELECT * FROM ' + tablename + ' WHERE job_id = %s',
                      (job_id,))
            entries = c.fetchall()

        return entries

    def _get_last_entry(self, job_id, tablename):
        """
        Private method to get the last entry from a table which has a
        timestamp and multiple entries per job_id.
        """

        with self.db as c:
            c.execute("SELECT * FROM " + tablename + " WHERE job_id = %s " +
                      "ORDER BY id DESC LIMIT 1",
                      (job_id,))
            entry = c.fetchall()
        if len(entry) < 1:
            raise NoRowsError(
                'job',
                'SELECT * FROM ' + tablename +
                ' WHERE job_id = %i ORDER BY id DESC LIMIT 1' % (job_id))

        return entry[0]

    def get_last_qa(self, job_id):
        """
        Return the lastqa entry for a given job.

        Parameters:
        job_id: integer (id from job tale)

        Returns:
        qa: namedtuple JSAProcQa
        """

        qa = self._get_last_entry(job_id, 'qa')
        qa = JSAProcQa(*qa)
        return qa

    def get_last_log(self, job_id):
        """
        Return the last log entry for a given job.

        Parameters:
        job_id: integer (id from job tale)

        Returns:
        log: namedtuple JSAProcLog
        """

        log = self._get_last_entry(job_id, 'log')
        log = JSAProcLog(*log)
        return log

    def set_location(self, job_id, location, foreign_id=(),
                     state_new='?', message=None):
        """
        Update the location, and optionally the foreign_id of a job.

        parameters;
        job_id (required), ingteger, identifies the job to update.

        location (required), string, where to process the job.

        foregin_id (option), string, None to set to NULL or empty
        tuple (default) to not alter the current value.

        state_new: new state to change the job to after moving it.
        (default: UNKNOWN, None to not change the state)

        message: log message to use when changing the state.
        (string, default None to generate a message automatically).
        Not used if state_new is set to None.
        """

        with self.db as c:
            if foreign_id == ():
                query = 'UPDATE job SET location = %s WHERE id = %s'
                param = (location, job_id)
            else:
                query = 'UPDATE job SET location = %s, foreign_id = %s ' \
                        'WHERE id = %s'
                param = (location, foreign_id, job_id)

            c.execute(query, param)

            if c.rowcount == 0:
                raise NoRowsError('job', query % param)
            elif c.rowcount > 1:
                raise ExcessRowsError('job', query % param)

            if state_new is not None:
                if message is None:
                    message = 'Location changed to {0}'.format(location)

                self._change_state(
                    c, job_id, state_new, message, None, None, None)

    def set_foreign_id(self, job_id, foreign_id):
        """
        Update the foreign_id of a job of id job_id.

        parameters:
        job_id (required), integer, identify job to update (id of table=job).

        foreign_id (reuiqred), string.
        """
        with self.db as c:
            c.execute('UPDATE job SET foreign_id = %s WHERE id = %s',
                      (foreign_id, job_id))

    def set_mode(self, job_id, mode):
        """
        Update the mode of a job.
        """

        with self.db as c:
            c.execute('UPDATE job SET mode = %s WHERE id = %s',
                      (mode, job_id))

    def set_parameters(self, job_id, parameters):
        """
        Update the parameters of a job.
        """

        with self.db as c:
            c.execute('UPDATE job SET parameters = %s WHERE id = %s',
                      (parameters, job_id))

    def get_date_range(self, task=None):
        """
        Get the minimum and maximum utdate for
        observations associated with a given task.

        *task*: optional, string

        Returns: (utdatemin, utdatemax)
        Atuple of datetime.date object.s
        """

        select = 'SELECT MIN(jcmt.COMMON.utdate), MAX(jcmt.COMMON.utdate) ' + \
                 'FROM job JOIN obsidss ON job.id = obsidss.job_id ' + \
                 '  JOIN jcmt.COMMON on obsidss.obsid=jcmt.COMMON.obsid '
        params = ()
        if task:
            select += ' WHERE job.task = %s'
            params = (task,)

        with self.db as c:
            self.db.unlock()
            c.execute(select, params)

            times = c.fetchall()[0]

        if times[0] is None and times[1] is None:
            raise NoRowsError('COMMON', select % params)

        return times

    def get_output_files(self, job_id, with_info=False):
        """
        Get the output file list for a job.

        parameters:
        job_id, (required), integer.
        Identify which job to get the output file list from.

        with_info: choose whether to retrieve full information
        or just the file names (which is the default).

        Returns:
        list of output files unless with_info is enabled, in which
        case a list of JSAProcFileInfo namedtuples is returned.

        Will raise an NoRowsError if there are no output files found.
        """

        with self.db as c:
            c.execute(
                'SELECT filename, md5 FROM output_file WHERE job_id = %s',
                (job_id,))
            output_files = c.fetchall()
            if len(output_files) == 0:
                raise NoRowsError(
                    'output_file',
                    'SELECT filename FROM output_file WHERE job_id = ' +
                    (str(job_id)))

        if with_info:
            return [JSAProcFileInfo(*row) for row in output_files]

        else:
            # Turn list of tuples into single list of strings.
            return [row[0] for row in output_files]

    def set_output_files(self, job_id, output_files):

        """
        This will set the output file list for a job.

        This first blanks any lines set with that job_id, and then
        creates new entries for each item in output_files.

        parameters:
        job_id, required, integer
        Identify which job to change/set the output file list from.

        output_files, required, list of JSAProcFileInfo objects.
        List of output files for the job (can be any iterable,
        e.g. tuple etc., and the object could be any object which
        has the requred attributes, which currently are
        filename and md5).

        """

        with self.db as c:
            # First of all blank out any current output files for this job_id.
            c.execute('DELETE FROM output_file WHERE job_id = %s', (job_id,))

            for f in output_files:
                # Now add in the new output files, one at a time.
                c.execute('INSERT INTO output_file (job_id, filename, md5) '
                          'VALUES (%s, %s, %s)',
                          (job_id, f.filename, f.md5))

    def get_log_files(self, job_id):
        """
        Get the  list of log files for a job.

        parameters:
        job_id, (required), integer.
        Identify which job to get the output file list from.


        Returns:
        list of output files.

        Will raise an NoRowsError if there are no output files found.
        """

        with self.db as c:
            c.execute(
                'SELECT filename FROM log_file WHERE job_id = %s',
                (job_id,))
            output_files = c.fetchall()
            if len(output_files) == 0:
                raise NoRowsError(
                    'log_file',
                    'SELECT filename FROM log_file WHERE job_id = ' +
                    (str(job_id)))

        # Turn list of tuples into single list of strings.
        return [row[0] for row in output_files]

    def set_log_files(self, job_id, log_files):

        """
        This will set the output log file list for a job.

        This first blanks any lines set with that job_id, and then
        creates new entries for each item in log_files.

        parameters:
        job_id, required, integer
        Identify which job to change/set the output file list from.

        log_files, required, list of log_file names.

        """

        with self.db as c:
            # First of all blank out any current output files for this job_id.
            c.execute('DELETE FROM log_file WHERE job_id = %s', (job_id,))

            for f in log_files:
                # Now add in the new output files, one at a time.
                c.execute('INSERT INTO log_file (job_id, filename) '
                          'VALUES (%s, %s)',
                          (job_id, f))

    def find_errors_logs(self, location=None, task=None, state_prev=None,
                         error_state=JSAProcState.ERROR):
        """
        Retrieve list of all jobs in an error state, together with their logs.

        Search is limited by:
             * location (default None, can be 'JAC' or 'CADC')
             * task (default None)
             * state_prev

        Return: an ordered dictionary by job identifier.  This will be in
        reverse chronological order of the last entry for each job
        (i.e. newest first).
        """

        query = 'SELECT job.id, log.datetime, log.message, log.state_new, ' \
                'log.state_prev, job.location '\
                'FROM job JOIN log ON job.id=log.job_id'
        query += ' WHERE job.state=%s'
        param = [error_state]

        if location is not None:
            query += ' AND job.location=%s '
            param.append(location)
        if task is not None:
            query += ' AND job.task=%s '
            param.append(task)
        if state_prev is not None:
            query += ' AND job.state_prev=%s '
            param.append(state_prev)

        query += ' ORDER BY log.id DESC'

        # Execute query
        with self.db as c:
            c.execute(query, param)
            error_jobs = c.fetchall()

            edict = OrderedDict()
            for j in error_jobs:
                einfo = JSAProcErrorInfo(*j)
                edict[einfo.id] = edict.get(einfo.id, []) + [einfo]

        return edict
        # Now sort out error jobs in sensible option

    def find_jobs(self, state=None, location=None, task=None, qa_state=None,
                  tag=None, state_prev=None,
                  prioritize=False, number=None, offset=None,
                  sort=False, sortdir='ASC', outputs=None, count=False,
                  obsquery=None, tiles=None):
        """Retrieve a list of jobs matching the given values.

        Searches by the following values:

            * state (jobs in the deleted state are not returned unless
              specifically asked for)
            * location
            * task
            * qa_state
            * tiles (a list)
            * tag

        Results can be affected by the following optional parameters:

            * count (Boolean, only return number of results)
            * prioritize (Boolean, results sorted by priority order)
            * number (integer, number of results to return)
            * offset (integer, offset the results from start by this many)
            * order (Boolean, sort results by id (after priority))
            * outputs (string, pattern to matche against output table to
              get output_files that match the string. e.g. '%preview_1024.png'
              would include all 1024 size preview images with jobs.
              If this argument is None then no outputs will be fetched.)

        In addition the jobs returned can be affected by an optional
        obsquery parameter. If given, this must be a dictionary of
        columns from the jcmt COMMON table giving their required value.
        This dictionary is processed by _dict_query_where_clause
        and accepts any type of value permitted by that method.

        Returns a list (which may be empty) of namedtuples, each  of which have
        values:

            * id
            * tag
            * state
            * location
            * task
            * qa_state
            * outputs (list)

        """

        param = []
        join = ''

        if count is True:
            query = 'SELECT COUNT(*)'
        else:
            query = 'SELECT job.id, job.tag, job.state, job.location, ' \
                    'job.foreign_id, job.task, job.qa_state'

            if outputs:
                query += ', GROUP_CONCAT(output_file.filename) '
                join = (' LEFT JOIN output_file ON job.id=output_file.job_id '
                        'AND output_file.filename LIKE %s')
                param.append(outputs)

            else:
                query += ', NULL'

        # Note: join and count cannot be used together.
        query += ' FROM job' + join

        # Use the _find_jobs_where method to prepare the WHERE clauses.
        (where, whereparam) = self._find_jobs_where(
            state, location, task, qa_state, tag, obsquery, tiles,
            state_prev=state_prev)

        if where:
            query += ' WHERE ' + ' AND '.join(where)
            param.extend(whereparam)

        # If we performed a join, we need to group by job.id, on the
        # assumption that it was a one-to-many join.  If we ever
        # add any one-to-one joins, this step should be made more
        # conditional.
        if join:
            query += ' GROUP BY job.id '

        # Do not generate the ORDER BY clause if we are only selecting
        # the count.
        if not count:
            # Use the _find_jobs_order method to prepare the ORDER clauses.
            order = self._find_jobs_order(prioritize, sort, sortdir)

            if order:
                query += ' ORDER BY ' + ', '.join(order)

        # Return [number] of results, starting at [offset]
        if number:

            query += ' LIMIT %s'

            if offset:
                query += ', %s'
                param.append(offset)

            param.append(int(number))

        result = []

        with self.db as c:

            if 'jcmt.COMMON' in query:
                self.db.unlock()
            logger.debug([query, param])
            c.execute(query, param)
            while True:
                row = c.fetchone()

                # If you've asked for a count, just return that (only one row)
                if count is True:
                    return row[0]

                if row is None:
                    break

                # Turn the row into a namedtuple.

                row = JSAProcJobInfo(*row)

                # If output files were returned, split them into a list.
                if row.outputs is not None:
                    row = row._replace(outputs=row.outputs.split(','))

                # Append the (possibly modified) row to the result list.
                result.append(row)

        return result

    def _find_jobs_where(self, state, location, task, qa_state, tag,
                         obsquery, tiles, state_prev=None):
        """Prepare WHERE expression for the find_jobs method.

        Return: a tuple containing a list of SQL expressions
        and a list of placeholder parameters.
        """

        where = []
        param = []

        jobquery = {}

        if state is not None:
            jobquery['state'] = state
        else:
            jobquery['state'] = Not(JSAProcState.DELETED)

        if location is not None:
            jobquery['location'] = location

        if task is not None:
            jobquery['task'] = task

        if qa_state is not None:
            jobquery['qa_state'] = qa_state

        if tag is not None:
            jobquery['tag'] = tag

        if state_prev is not None:
            jobquery['state_prev'] = state_prev

        (jobwhere, jobparam) = _dict_query_where_clause('job', jobquery)
        where.append(jobwhere)
        param.extend(jobparam)

        if obsquery:
            (obswhere, obsparam) = _dict_query_where_clause('jcmt.COMMON', obsquery)
            where.append('job.id IN (SELECT job_id FROM obsidss JOIN jcmt.COMMON ON obsidss.obsid=jcmt.COMMON.obsid  WHERE ' +
                         obswhere + ')')
            param.extend(obsparam)

        if tiles:
            (tilewhere, tileparam) = _dict_query_where_clause('tile',
                                                              {'tile': tiles})
            where.append('job.id IN (SELECT job_id FROM tile WHERE ' +
                         tilewhere + ')')
            param.extend(tileparam)

        return (where, param)

    def _find_jobs_order(self, prioritize, sort, sortdir):
        """Prepare ORDER expressions for the find_jobs method.

        Return: a list of ORDER expressions.
        """

        if sortdir != 'ASC' and sortdir != 'DESC':
            raise JSAProcError('Can only sort jobs in ASC or DESC direction. '
                               'You picked %s' % (sortdir))

        order = []

        if prioritize:
            order.append('job.priority DESC')

        if sort:
            order.append('job.id ' + sortdir)

        return order

    def get_processing_time_obs_type(self, obsdict=None, jobdict=None, ):
        """Get the processing times.

        By default looks for all types of observations in location JAC
        and in states corresponding to JSAProcState.STATE_POST_RUN.

        It can be limited by the usual job and obs column querys.

        This function will get the difference in time between the
        movement into the PROCESSING state and the movement into the PROCESSED
        state. (It will look for the last occurence of each in the log)

        Returns job_ids, duration_seconds, obs_info

        job_ids is a list of the job_ids that the processing
        times were calculated for.
        duration_seconds is a list of the processing time in
        seconds for each job.
        obs_info is a list of the obs.obstype, obs.scanmode,
        obs.project, obs.survey and obs.instrument for each job.

        For tasks where multiple observations are in the same job
        this could produce odd results. Careful filtering of the
        output will be required (it will return one result for each
        observation, so multiple identical times for a single job).
        """
        from_query = "FROM job " + \
                     " LEFT JOIN log ON job.id = log.job_id " + \
                     " LEFT JOIN obsidss ON job.id=obsidss.job_id " + \
                     " LEFT JOIN jcmt.COMMON ON obsidss.obsid=jcmt.COMMON.obsid " + \
                     " LEFT OUTER JOIN omp.ompobslog AS o ON o.obslogid = (SELECT MAX(obslogid) FROM omp.ompobslog o2 WHERE o2.obsid=jcmt.COMMON.obsid) "

        select_query = "SELECT job.id, MAX(log.datetime) AS maxdt, " + \
                       "jcmt.COMMON.obs_type, " + \
                       "CASE WHEN jcmt.COMMON.sam_mode='SCAN' THEN jcmt.COMMON.scan_pat ELSE jcmt.COMMON.sam_mode END AS scanmode, " + \
                       "jcmt.COMMON.project, " + \
                       "jcmt.COMMON.survey, jcmt.COMMON.instrume, CASE WHEN o.commentstatus is NULL THEN 0 ELSE o.commentstatus END AS omp_status "

        where = ['log.state_new=%s']

        param = []
        group_query = " GROUP BY job.id "

        if obsdict:
            obsquery, obsparam = _dict_query_where_clause('jcmt.COMMON', obsdict)
            where.append(obsquery)
            param += obsparam

        if not jobdict:
            jobdict = {}
        if 'location' not in jobdict:
            jobdict['location'] = "JAC"
        if 'state' not in jobdict:
            jobdict['state'] = JSAProcState.STATE_POST_RUN

        jobquery, jobparam = _dict_query_where_clause('job', jobdict)
        where.append(jobquery)
        param += jobparam

        query = select_query + from_query + \
            ' WHERE ' + ' AND '.join(where) + \
            group_query

        where = ['log.state_prev=%s'] + where

        query_processed = select_query + from_query + \
            ' WHERE ' + ' AND '.join(where) + \
            group_query

        with self.db as c:
            if 'jcmt.COMMON' in query:
                self.db.unlock()
            c.execute(query, [JSAProcState.RUNNING] + param)
            startresults = c.fetchall()
            columns = c.description

        with self.db as c:
            if 'jcmt.COMMON' in query:
                self.db.unlock()
            c.execute(query_processed,
                      [JSAProcState.RUNNING, JSAProcState.PROCESSED] + param)
            endresults = c.fetchall()

        job_ids_starts = [i[0] for i in startresults]
        job_ids_ends = [i[0] for i in endresults]

        for i in range(len(job_ids_starts)):
            if job_ids_starts[i] not in job_ids_ends:
                startresults.pop(i)
        for i in range(len(job_ids_ends)):
            if job_ids_ends[i] not in job_ids_starts:
                endresults.pop(i)

        # Get the times, job_id numbers and observation infos.
        duration_seconds = []
        job_ids = []
        job_infos = []
        for i in range(len(startresults)):
            seconds = (endresults[i][1] - startresults[i][1]).total_seconds()
            duration_seconds.append(seconds)
            job_ids.append(startresults[i][0])
            job_infos.append(startresults[i][2:])

        return job_ids, duration_seconds, job_infos

    def get_tasks(self):
        """Retrieve list of task names which have been assigned to jobs.

        Results are returned in alphabetical order.
        """

        query = 'SELECT DISTINCT task FROM job ORDER BY task ASC'

        result = []

        with self.db as c:
            c.execute(query)

            while True:
                row = c.fetchone()
                if row is None:
                    break

                result.append(row[0])

        if not result:
            raise NoRowsError('job', query)

        return result

    def get_task_info(self, task=None):
        """
        Get the values from task table for a given task or for all tasks.

        Returns:
        JSAProcJobNote namedtuple: contains the id, taskname,
        etransfer and starlink values from the table.  If task is None
        then results for all tasks are returned as a dictionary
        organized by task name.
        """

        query = 'SELECT id, taskname, etransfer, starlink, version, ' \
            'command_run, command_xfer, raw_output, command_ingest, log_ingest ' \
            'FROM task'
        params = []

        if task is not None:
            query += ' WHERE taskname=%s'
            params.append(task)

        result = {}

        with self.db as c:
            c.execute(query, params)

            while True:
                row = c.fetchone()
                if row is None:
                    break

                row = JSAProcTaskInfo(*row)

                result[row.taskname] = row

        if task is not None:
            if len(result) == 0:
                raise NoRowsError('task', query % tuple(params))
            elif len(result) > 1:
                raise ExcessRowsError('task', query % tuple(params))
            try:
                return result[task]
            except KeyError:
                raise JSAProcDBError('task', query % tuple(params))

        return result

    def add_task(self, taskname, etransfer, starlink=None, version=None,
                 command_run=None, command_xfer=None, raw_output=None,
                 command_ingest=None):
        """
        Add a task to the task table.

        Args:
          taskname: string, name of task. (limited to 80 characters)
          etransfer: Boolean, if jobs should be etransferred &
            ingested or not.
          starlink (str, optional): Path to a STARLINK_DIR to be used
            for this task. Default is '' (use value of $STARLINK_DIR).
          version: file version.
          command_run: custom command to run job
          command_xfer: custom data transfer command
          raw_output: request raw output files
          command_ingest: custom data ingestion command
        """
        with self.db as c:
            c.execute(
                'INSERT INTO task (taskname, etransfer, starlink, version, '
                'command_run, command_xfer, raw_output, command_ingest) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (taskname, etransfer, starlink, version,
                 command_run, command_xfer, raw_output, command_ingest))

    def get_parents(self, job_id, with_state=False):
        """
        Look in the parent table and get all parent jobs
        for the given job_id.

        Returns a list of tuples of (parent job id, filters).  The state of the
        parent job can optionally be included.

        Raises NoRowsError if no results are found.
        """
        if not with_state:
            query = 'SELECT parent, filter FROM parent WHERE job_id = %s'
        else:
            query = 'SELECT parent.parent, parent.filter, job.state' \
                ' FROM parent JOIN job ON parent.parent=job.id' \
                ' WHERE parent.job_id = %s'
        params = (job_id,)
        with self.db as c:
            c.execute(query, params)
            result = c.fetchall()

        if len(result) == 0:
            raise NoRowsError('parent', query % params)
        return result

    def get_children(self, job_id):
        """
        Get all jobs that list the current job as a parent.

        Return a list of integer job_ids.

        Raise NoRowsError if no results are found.
        """
        query = 'SELECT job_id FROM parent WHERE parent = %s'
        params = (job_id,)

        with self.db as c:
            c.execute(query, params)
            result = c.fetchall()

        if len(result) == 0:
            raise NoRowsError('parent', query % params)

        # Fix up the format so its not a list of 1-item tuples:
        result = [i[0] for i in result]
        return result

    def add_to_parents(self, job_id, parents, filters=None):
        """
        Add additional jobs to the parent table for the child 'job_id'.
        DOES NOT REMOVE CURRENT JOBS.

        job_id: integer, id of child job.

        parents: list of integers, ids of parent jobs.

        filters: string or list of strings.
        RE filter to select only appropriate input files for child from
        output file list of parent.
        """

        # Validate input.
        job_id, parents, filters = _validate_parents(job_id, parents,
                                                     filters=filters)

        # Update table.
        with self.db as c:
            self._insert_parents(job_id, c, parents, filters)

        return job_id

    def _delete_all_parents(self, job_id, c):

        c.execute('DELETE FROM parent WHERE job_id = %s',
                  (job_id,))

    def _insert_parents(self, job_id, c, parents, filters):

        for parent, filt in zip(parents, filters):
            c.execute('INSERT INTO parent (job_id, parent, filter) '
                      'VALUES (%s, %s, %s)',
                      (job_id, parent, filt))

    def delete_some_parents(self, job_id, parents):
        """
        removes specified jobs from the parent table for the child 'job_id'.
        DOES NOT ATTEMPT TO REMOVE ALL JOBS.
        Will raise an error if parent job_id is not present in parent table
        for given child job_id

        job_id: integer, id of child job.

        parents: list of integers, ids of parent jobs.

        """

        # Validate input.
        isvalid = _validate_parents_to_remove(job_id, parents, self)

        # Update table
        with self.db as c:
            for parent in parents:
                c.execute(
                    'DELETE FROM parent WHERE job_id =%s and parent = %s ',
                    (job_id, parent))

        return job_id

    def replace_parents(self, job_id, newparents, filters=None):
        """
        Replace all parent jobs in data base with new parents

        """

        jov_id, parents, filters = _validate_parents(job_id, newparents,
                                                     filters=filters)
        with self.db as c:
            self._delete_all_parents(job_id, c)
            self._insert_parents(job_id, c, parents, filters)

    def delete_parents(self, job_id):
        _validate_parents_to_remove(job_id, [], self)
        with self.db as c:
            self._delete_all_parents(job_id, c)

    def add_note(self, job_id, message, username=None):
        """
        Add a note about a job.

        The given message will be inserted into the note table.
        """

        if username is None:
            username = getuser()

        with self.db as c:
            c.execute('INSERT INTO note (job_id, message, username) '
                      'VALUES (%s, %s, %s)',
                      (job_id, message, username))

    def get_notes(self, job_id):
        """
        Retrieve a list of notes for a given job.

        Returns an empty list if there are no notes for the
        job.  Otherwise the list contains JSAProcJobNote namedtuples.
        Notes should be returned in reverse chronological order.
        """

        result = []

        with self.db as c:
            c.execute('SELECT id, message, username FROM note '
                      'WHERE job_id=%s '
                      'ORDER BY id DESC', (job_id,))

            while True:
                row = c.fetchone()
                if row is None:
                    break

                result.append(JSAProcJobNote(*row))

        return result

    def get_job_summary(self):
        """
        Get an ordered dictionary summarizaing the number of jobs in each
        task and state.
        """

        result = defaultdict(dict)

        with self.db as c:
            c.execute('SELECT task, state, COUNT(*) AS number '
                      'FROM job '
                      'GROUP BY task, state '
                      'ORDER BY task, state')

            while True:
                row = c.fetchone()
                if row is None:
                    break

                (task, state, number) = row
                result[task][state] = number

        return result

    def get_obs_preproc_recipe(self, obsid):
        """
        Get the preprocessing recipe required for the given observation,
        if present, or None otherwise.
        """

        with self.db as c:
            c.execute(
                'SELECT recipe FROM obs_preproc WHERE obsid=%s',
                (obsid,))

            result = c.fetchall()

        if len(result) != 1:
            return None

        return result[0][0]


def _dict_query_where_clause(table, wheredict, logic_or=False):
    """Semi-private function that takes in a dictionary of column names
    and allowed options, and turns them into a string that can be added
    to a where query to limit the options.

    Currently only supports the = operator. Could be adjusted to allow for
    other tests if needed.

    parameters:

    table: name of the table in which the fields are to be found.

    wheredict: dictionary, required.

    A dictionary where the field names are columns in a table, and the
    values are allowed values for those columns in a mysql WHERE query.

    If the value for a single column is a list, then a row that matches
    any of the values will be returned when the query is used.  If the value
    or list is wrapped in a "Not" object, then the condition will
    be inverted (the column does not match the given value, or the
    column's value is not in the given list of values).  If the value
    is None then the query will use "IS NULL" (or "IS NOT NULL").

    logic_or: boolean, optional, default=False

    Logic for combining  the different fields.  By default an "AND"
    combination is used (all fields must match), but if this parameter
    is set then an "OR" combination is used (only one field need
    match).

    returns:

    where_query: string,

    params: list

    parameters for the query.
    """

    if not valid_column.match(table) and not table in ['jcmt.COMMON']:
        raise JSAProcError('Invalid table name "{0}"'.format(table))

    where = []
    params = []
    for key, value in wheredict.items():
        table_key = '{0}.`{1}`'.format(table, key)
        # Column names can only use valid characters.
        if not valid_column.match(key):
            raise JSAProcError('Non allowed column name %s for SQL matching' %
                               (str(key)))

        # Fix up column names (due to switch from jsa_proc.obs to using jcmt.COMMON
        if table == 'jcmt.COMMON':
            if key == 'obstype':
                table_key = 'jcmt.COMMON.obs_type'
            if key == 'instrument':
                table_key = 'jcmt.COMMON.instrume'
            if key == 'tau':
                table_key = '(jcmt.COMMON.wvmtaust+jcmt.COMMON.wvmtauen)/2.0'



        if isinstance(value, Not):
            value = value.value
            logic_not = True
        else:
            logic_not = False

        if value is None:
            where.append(
                '{0} IS {1}'.format(table_key,
                                    'NOT NULL' if logic_not else 'NULL'))


        elif isinstance(value, Range):
            if value.min is None and value.max is None:
                pass

            elif value.min is not None and value.max is not None:
                where.append(
                    '{0} {1} %s AND %s'.format(
                        table_key, 'NOT BETWEEN' if logic_not else 'BETWEEN'))
                params.extend(value)

            else:
                if value.min is not None:
                    params.append(value.min)
                    range_op = '<' if logic_not else '>='

                elif value.max is not None:
                    params.append(value.max)
                    range_op = '>' if logic_not else '<='

                where.append('{0} {1} %s'.format(table_key, range_op))

        elif isinstance(value, Fuzzy):
            # Not really very fuzzy, but for now implement this as a LIKE
            # expression with wildcards at both ends (LIKE is case
            # insensitive).
            where.append((
                '({0} NOT LIKE %s OR {0} IS NULL)'
                if logic_not else
                '{0} LIKE %s'
                ).format(table_key))
            params.append(
                '%{0}%'.format(value.value)
                if value.wildcards else
                value.value)

        elif isinstance(value, basestring) or not hasattr(value, '__iter__'):
            # If string or non iterable object, use simple comparison.
            where.append((
                '({0}<>%s OR {0} IS NULL)'
                if logic_not else
                '{0}=%s'
                ).format(table_key))
            params.append(value)

        else:
            # Otherwise use an IN expression.
            where.append((
                '({0} NOT IN ({1}) OR {0} IS NULL)'
                if logic_not else
                '{0} IN ({1})'
                ).format(table_key, ', '.join(('%s',) * len(value))))
            params.extend(value)

    if not where:
        return ('', [])

    where = (' OR ' if logic_or else ' AND ').join(where)
    return ('({0})'.format(where), params)


def _validate_parents(job_id, parents, filters=None):
    """
    Validate that parents and filters are
    valid parents for given job_id.

    Raise JSAProc Error if not valid.

    """
    if job_id in parents:
        raise JSAProcError('Cannot insert a job as its own parent.')
    if filters is None:
        filters = ''
    if isinstance(filters, basestring):
        filters = [filters] * len(parents)
    else:
        if not len(filters) == len(parents):
            raise JSAProcError(
                'If more than one filter is given ' +
                'len(filters) should match len(parents)')

    return job_id, parents, filters


def _validate_parents_to_remove(job_id, parents, db):
    """
    Check all the parents are in the parents table
    for child job_id.

    Returns True if all are present, False if they are not.

    Raises an error if no results are in the database.
    """
    with db.db as c:
        c.execute('SELECT parent FROM parent WHERE job_id = %s',
                  (job_id,))
        results = c.fetchall()

    if len(results) == 0:
        raise JSAProcError('job %s has no entries in parent table' % job_id)
    results = [i[0] for i in results]

    isvalid = set(parents) <= set(results)
    if not isvalid:
        raise JSAProcError('Not all parent-jobs to be removed are ' +
                           "present in parent table for child %s" % job_id)
    return isvalid
