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

from collections import namedtuple, OrderedDict
import logging
from socket import gethostname

from jsa_proc.error import *
from jsa_proc.state import JSAProcState


logger = logging.getLogger(__name__)

# Named tuples that are created ahead of time instead of dynamically
# defined from table rows:
JSAProcLog = namedtuple('JSAProcLog', 'id job_id datetime state_prev state_new message host')
JSAProcJobInfo = namedtuple('JSAProcJobInfo', 'id tag state location foreign_id outputs')
JSAProcErrorInfo = namedtuple('JSAProcErrorInfo', 'id time message state location')

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

        assert(hasattr(self, 'db'))


    def get_job(self, id_=None, tag=None):
        """
        Get a JSA data processing job from the database.

        Requires either the tag or id of the job table, and raises a
        JSAProcDBError if one or the other is not provided. If the tag
        or id does not exist in the database than it needs to raise an
        error. If both are set it does not raise an error, but will use the id value.

        id_: integer

        tag: string

        Returns: namedtuple of values of all columns in job database.
        """

        if (id_ is None) and (tag is None):
            raise JSAProcError("You must set either id_ or tag to use get_job")

        if id_ is not None:
            name='id'
            value=id_
        else:
            name='tag'
            value=tag

        # Get the values form the database
        with self.db as c:
            c.execute('SELECT * FROM job WHERE '+name+'=%s', (value,))
            job = c.fetchall()
            if len(job) == 0:
                raise NoRowsError('job',
                                  'SELECT * FROM job WHERE '+name+'='+str(value))
            if len(job) > 1:
                raise ExcessRowsError('job', 'SELECT * FROM job WHERE '+name+'='+str(value))
            # Turn list into single item
            job = job[0]
            rows = c.description

        # Define namedtuple dynamically, to ensure we always return
        # full information about jobs (specific to this function),
        # define others at top of this file. (Can be defined
        # statically instead if wanted).
        rows = ' '.join([i[0] for i in rows])
        JSAProcJob = namedtuple('JSAProcJob', rows)

        # Turn job into namedtuple
        job = JSAProcJob(*job)

        return job

    def add_job(self, tag, location, mode, parameters,
                input_file_names, foreign_id=None, state='?',
                priority=0):
        """
        Add a JSA data processing job to the database.

        This will raise an error if the job already exists, if the
        database interface raises an erro. The job must be specified
        by its unique tag.

        If the job creation is successful, an entry will be added to the
        log table to record this event.

        Parameters:

        tag: string, unique identifier for observation/job

        location: string, where the job will be run.

        mode: JSA processing mode (obs / night / project / public),
        ignored for now but included in this function's interface for
        future use.

        parameters: processing parameters to pass to jsawrapdr
        (typically the recipe name). (string)

        input_file_names: iterable, each item being a string that
        identifies the name of an input file for the job.

        foreign_id: OPTIONAL, default=None. (string), identifier from
        foreign system (probably  CADC).

        state: initial job state (character, default ?).

        priority: priority number (integer, default 0, higher number
        represents greater priority).

        Returns the job identifier.
        """

        # Validate input.
        if not JSAProcState.is_valid(state):
            raise JSAProcError('State {0} is not recognised'.format(state))

        # insert job into table
        with self.db as c:
            c.execute('INSERT INTO job '
                      '(tag, state, location, mode, parameters, foreign_id, priority) '
                      'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                      (tag, state, location, mode, parameters, foreign_id, priority))

            # Get the autoincremented id from job table (job_id in all other tables)
            job_id = c.lastrowid

            # Need to get input file names and add them to table input_file
            for filename in input_file_names:
                c.execute('INSERT INTO input_file (job_id, filename) VALUES (%s, %s)',
                          (job_id, filename))

            # Log the job creation
            self._add_log_entry(c, job_id, JSAProcState.UNKNOWN, state,
                                'Job added to the database')

        # job_id may not be necessary but sometimes useful.
        return job_id


    def change_state(self, job_id, newstate, message, state_prev=None):
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

        # Validate input.
        if not JSAProcState.is_valid(newstate):
            raise JSAProcError('State {0} is not recognised'.format(newstate))

        with self.db as c:

            # Change the state to new state and update the state_prev
            query = 'UPDATE job SET state_prev = state, state = %s WHERE id = %s'
            param = [newstate, job_id]

            if state_prev is not None:
                query += ' AND state=%s'
                param.append(state_prev)

            c.execute(query, param)

            if c.rowcount == 0:
                raise NoRowsError('job', query % tuple(param))
            elif c.rowcount > 1:
                raise ExcessRowsError('job', query % tuple(param))

            # Get state_prev value if we were not given it.
            if state_prev is None:
                c.execute('SELECT state_prev FROM job WHERE id=%s',
                          (job_id,))
                state_prev = c.fetchall()

                if len(state_prev) > 1:
                    raise ExcessRowsError('job',
                                          'SELECT state_prev FROM job WHERE id=%s'%(str(job_id)))

                state_prev=state_prev[0][0]

            # Update log table.
            self._add_log_entry(c, job_id, state_prev, newstate, message)

        return job_id

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
                raise NoRowsError('input_file',
                                  'SELECT filename FROM input_file WHERE job_id = '+(str(job_id)))

        # input_files will be a list of tuples, each tuple containgin
        # one file. Flatten this into a list of strings.
        input_files = [file for i in input_files for file in i]

        return input_files

    def _add_log_entry(self, c, job_id, state_prev, state_new, message):
        """Private method to add an entry to the log table.

        Assumes the database is already locked an takes a cursor
        object as argument "c".
        """

        c.execute('INSERT INTO log (job_id, state_prev, state_new, message, host) '
                  'VALUES (%s, %s, %s, %s, %s)',
                  (job_id, state_prev, state_new, message, gethostname()))

    def get_logs(self, job_id):
        """
        Get the full log of states of a given job from the log table.

        Parameters:
        job_id : integer (id from job table)

        Returns:
        list of JSAProcLog nametuples, 1 entry per row in log table for that job_id.
        """
        with self.db as c:
            c.execute('SELECT * FROM log WHERE job_id = %s',(job_id,))
            logs = c.fetchall()

        # Create JSAProcLog namedtuple object to hold values.
        logs = [JSAProcLog(*i) for i in logs]

        return logs


    def get_last_log(self, job_id):
        """
        Return the last log entry for a given job.

        Parameters:
        job_id: integer (id from job tale)

        Returns:
        logentry: namedtuple JSAProcLog
        """

        with self.db as c:
            c.execute('SELECT * FROM log WHERE job_id = %s ORDER BY id DESC LIMIT 1',
                      (job_id,))
            log = c.fetchall()
        if len(log) < 1:
            raise NoRowsError('job','SELECT * FROM log WHERE job_id = %s ORDER BY id DESC LIMIT 1'%(str(job_id))
                              )

        log = JSAProcLog(*log[0])
        return log

    def set_location(self, job_id, location, foreign_id=()):
        """
        Update the location, and optionally the foreign_id of a job.

        parameters;
        job_id (required), ingteger, identifies the job to update.

        location (required), string, where to process the job.

        foregin_id (option), string, None to set to NULL or empty
        tuple (default) to not alter the current value.

        """

        with self.db as c:
            if foreign_id == ():
                c.execute('UPDATE job SET location = %s WHERE id = %s',(location, job_id))
            else:
                c.execute('UPDATE job SET location = %s, foreign_id = %s WHERE id = %s',
                          (location, foreign_id, job_id))

    def set_foreign_id(self, job_id, foreign_id):
        """
        Update the foreign_id of a job of id job_id.

        parameters:
        job_id (required), integer, identify job to update (id of table=job).

        foreign_id (reuiqred), string.
        """
        with self.db as c:
            c.execute('UPDATE job SET foreign_id = %s WHERE id = %s', (foreign_id, job_id))

    def get_output_files(self, job_id):
        """
        Get the output file list for a job.

        parameters:
        job_id, (required), integer.
        Identify which job to get the output file list from.

        Returns:
        list of output files.

        Will raise an NoRowsError if there are no output files found.
        """

        with self.db as c:
            c.execute('SELECT filename FROM output_file WHERE job_id = %s', (job_id,))
            output_files = c.fetchall()
            if len(output_files) == 0:
                raise NoRowsError('output_file',
                                  'SELECT filename FROM output_file WHERE job_id = '+(str(job_id)))

        # Turn list of tuples into single list of strings.
        output_files = [file for i in output_files for file in i]

        return output_files

    def set_output_files(self, job_id, output_files):

        """
        This will set the output file list for a job.

        This first blanks any lines set with that job_id, and then
        creates new entries for each item in output_files.

        parameters:
        job_id, required, integer
        Identify which job to change/set the output file list from.

        output_files, required, list of strings.
        List of output files for the job (can be any iterable of strings, e.g. tuple etc.)

        """


        with self.db as c:

            # First of all blank out any current output files for this job_id.
            c.execute('DELETE FROM output_file WHERE job_id = %s', (job_id,))

            for f in output_files:
                # Now add in the new output files, one at a time.
                c.execute('INSERT INTO output_file (job_id, filename) VALUES (%s, %s)',
                          (job_id, f))


    def find_errors_logs(self, location=None):
        """
        Retrieve list of all jobs in an error state, together with their logs.

        Search is limited by:
             * location (default None, can be 'JAC' or 'CADC')
        """

        param = []
        query = 'SELECT job.id, log.datetime, log.message, log.state_new, job.location  FROM job JOIN log ON job.id=log.job_id'
        query += ' WHERE job.state="E"'

        if location is not None:
            query+= 'AND job.location=%s'
            param.append(location)

        query += ' ORDER BY job.location DESC, job.id DESC, log.id DESC'

        # Execute query
        with self.db as c:

            print query
            print param
            c.execute(query,param)
            error_jobs = c.fetchall()

            edict = OrderedDict()
            for j in error_jobs:
                einfo = JSAProcErrorInfo(*j)
                edict[einfo.id] = edict.get(einfo.id, []) + [einfo]


        return edict
            # Now sort out error jobs in sensible option


    def find_jobs(self, state=None, location=None,
                  prioritize=False, number=None, offset=None,
                  sort=False, outputs=None, count=False):
        """Retrieve a list of jobs matching the given values.

        Searches by the following values:

            * state
            * location

        Results can be affected by the following optional parameters:

            * count (Boolean, only return number of results)
            * prioritize (Boolean, results sorted by priority order)
            * number (integer, number of results to return)
            * offset (integer, offset the results from start by this many)
            * order (Boolean, sort results by id (after priority))
            * outputs (True or string, matches against output table to
              get output_files that match the string. e.g. 'preview_1024.png'
              would include all 1024 size preview images with jobs.)


        Returns a list (which may be empty) of namedtuples, each  of which have
        values:

            * id
            * tag
            * state
            * location
            * outputs (list)

        """

        if count is True:
            query_get = 'SELECT COUNT(*) '
        else:
            query_get = 'SELECT job.id, job.tag, job.state, job.location, job.foreign_id'

        query_from = ' FROM job '

        # If you're getting outputs
        if outputs:
            separator = ' '
            query_get += ', GROUP_CONCAT(output_file.filename)'
            query_from +=' LEFT JOIN output_file ON job.id=output_file.job_id '

        query = query_get + query_from

        where = []
        param = []
        order = []

        if state is not None:
            where.append('job.state=%s')
            param.append(state)

        if location is not None:
            where.append('job.location=%s')
            param.append(location)

        if where:
            query += ' WHERE ' + ' AND '.join(where)

        # If getting output files, need to group by job.id:
        if outputs:
            query += ' GROUP BY job.id '

        if prioritize:
            order.append('job.priority DESC')

        if sort:
            order.append('job.id ASC')

        if order:
            query += ' ORDER BY ' + ', '.join(order)


        # Return [number] of results, starting at [offset]
        if number:
            query += ' LIMIT %s'

            if offset:
                query += ', %s'
                param.append(offset)

            param.append(number)

        result = []

        with self.db as c:

            c.execute(query, param)

            while True:
                row = c.fetchone()

                # If you've asked for a count, just return that (only one row)
                if count is True:
                    return row[0]

                if row is None:
                    break
                if not outputs:
                    row += (None,)

                row = list(row)

                # If getting outputs, fix the results up appropriately
                if outputs:
                    # If final value is not None, split it up.
                    if row[-1]:
                        row[-1]= row[-1].split(',')

                        # If a pattern has been provided, try to match it.
                        if isinstance(outputs, basestring):
                            returned_outputs=[]
                            for s in row[-1]:
                                if s.find(outputs) !=-1:
                                    returned_outputs.append(s)

                            # Explicitily return None instead of empty
                            # list if no results.
                            if not returned_outputs:
                                returned_outputs=None
                            row[-1] = returned_outputs

                # Turn the row into a namedtuple and append to results
                result.append(JSAProcJobInfo(*row))


        return result
