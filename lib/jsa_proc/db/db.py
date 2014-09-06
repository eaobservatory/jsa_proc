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
import re
from socket import gethostname

from jsa_proc.error import *
from jsa_proc.state import JSAProcState


logger = logging.getLogger(__name__)

# Named tuples that are created ahead of time instead of dynamically
# defined from table rows:
JSAProcLog = namedtuple(
    'JSAProcLog',
    'id job_id datetime state_prev state_new message host')
JSAProcJobInfo = namedtuple(
    'JSAProcJobInfo',
    'id tag state location foreign_id outputs')
JSAProcErrorInfo = namedtuple(
    'JSAProcErrorInfo',
    'id time message state location')

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

        assert(hasattr(self, 'db'))

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

        # Get the values form the database
        with self.db as c:
            c.execute('SELECT * FROM job WHERE '+name+'=%s', (value,))
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
            rows = c.description

        # Define namedtuple dynamically, to ensure we always return
        # full information about jobs (specific to this function),
        # define others at top of this file. (Can be defined
        # statically instead if wanted).
        JSAProcJob = namedtuple('JSAProcJob', [x[0] for x in rows])

        # Turn job into namedtuple
        job = JSAProcJob(*job)

        return job

    def add_job(self, tag, location, mode, parameters,
                input_file_names, foreign_id=None, state='?',
                priority=0, obsinfolist=None, tilelist=None):
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

        print 'testing add jobs...'
        parameters: processing parameters to pass to jsawrapdr
        (typically the recipe name). (string)

        input_file_names: iterable, each item being a string that
        identifies the name of an input file for the job.

        foreign_id: OPTIONAL, default=None. (string), identifier from
        foreign system (probably  CADC).

        state: initial job state (character, default ?).

        priority: priority number (integer, default 0, higher number
        represents greater priority).

        tilelist: optional, list of integers.
        The list of tiles this job will produce.

        obsinfolist: optional, list of dictionarys.
        A list of observations dictionarys. Each item in list represents a
        single observation which is included in this job. The dictionary
        should contain an entry for each column in the 'obs' table.

        Returns the job identifier.
        """

        # Validate input.
        if not JSAProcState.is_valid(state):
            raise JSAProcError('State {0} is not recognised'.format(state))

        # insert job into table
        with self.db as c:
            c.execute(
                'INSERT INTO job '
                '(tag, state, location, mode, parameters, '
                'foreign_id, priority) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                (tag, state, location, mode, parameters, foreign_id, priority))

            # Get the autoincremented id from job table (job_id in all other
            # tables).
            job_id = c.lastrowid

            # Need to get input file names and add them to table input_file
            for filename in input_file_names:
                c.execute('INSERT INTO input_file (job_id, filename) '
                          'VALUES (%s, %s)',
                          (job_id, filename))

            # Log the job creation
            self._add_log_entry(c, job_id, JSAProcState.UNKNOWN, state,
                                'Job added to the database')

            # If present, insert the tile list.
            if tilelist:
                self._set_tilelist(c, job_id, tilelist)

            # If present, replace/update the observation list.
            if obsinfolist:
                self._set_obs_info(c, job_id, obsinfolist, False)

        # job_id may not be necessary but sometimes useful.
        return job_id

    def get_tilelist(self, job_id):
        """Retrieve the list of tiles for a given job.
        """

        tiles = []

        with self.db as c:
            c.execute('SELECT tile FROM tile WHERE job_id = %s',
                      (job_id,))

            while True:
                row = c.fetchone()
                if row is None:
                    break

                tiles.append(row[0])

        return tiles

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

    def get_obs_info(self, job_id):
        """
        Get all entries in the obs table for a given job_id.

        job_id: integer, required

        returns:

        List of NamedTuples with entries from obs table.
        """

        with self.db as c:

            # Get all observations with job_id
            c.execute('SELECT * FROM obs WHERE job_id = %s', (job_id,))
            results = c.fetchall()
            columns = [i[0] for i in c.description]

            # Create a named tuple with the correct column output
            JSAProcObs = namedtuple('JSAProcObs', columns)

        results = [JSAProcObs(*obs) for obs in results]

        return results

    def set_obs_info(self, job_id, obsinfolist, replace_all=True):
        """
        Update the obs table with additional observations for a given job.

        job_id: integer, required

        obsinfolist: list of dictionaries.

        Each dictionary is the key/values for the obs table headings
        and values for 1 observation belonging to the job_id.

        replace_all: Boolean, default True

        If set True, delete all existing entries for the job_id before
        updating the table with the obsinfo dictionaries.
        """

        with self.db as c:
            self._set_obs_info(c, job_id, obsinfolist, replace_all)

    def _set_obs_info(self, c, job_id, obsinfolist, replace_all):
        # If replace_all is set, then delete the existing observations.
        if replace_all:
            c.execute(' DELETE FROM obs WHERE job_id = %s', (job_id,))

        # Go through each observation dictionary in the list.
        for obs in obsinfolist:
            columnnames, values = zip(*obs.items())

            # Column names can only use valid characters.
            for column in columnnames:
                if column in ('id', 'job_id'):
                    raise JSAProcError('Could not insert into obs table: '
                                       'private column name: ' + column)

                if not valid_column.match(column):
                    raise JSAProcError('Could not insert into obs table: '
                                       'invalid column name: ' + column)

            # Escape column names with back ticks.
            column_query = '(job_id, `' + '`, `'.join(columnnames) + '`)'
            values_questions = '(%s, ' + ', '.join(['%s'] * len(values))+')'

            c.execute('INSERT INTO obs ' + column_query +
                      ' VALUES ' + values_questions,
                      (job_id,) + values)

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
            query = ('UPDATE job SET state_prev = state, state = %s '
                     'WHERE id = %s')
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
                    raise ExcessRowsError(
                        'job',
                        'SELECT state_prev FROM job WHERE id=%s' % (job_id))

                state_prev = state_prev[0][0]

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
                raise NoRowsError(
                    'input_file',
                    'SELECT filename FROM input_file WHERE job_id = ' +
                    (str(job_id)))

        # input_files will be a list of tuples, each tuple containgin
        # one file. Flatten this into a list of strings.
        input_files = [file for i in input_files for file in i]

        return input_files

    def _add_log_entry(self, c, job_id, state_prev, state_new, message):
        """Private method to add an entry to the log table.

        Assumes the database is already locked an takes a cursor
        object as argument "c".
        """

        c.execute('INSERT INTO log '
                  '(job_id, state_prev, state_new, message, host) '
                  'VALUES (%s, %s, %s, %s, %s)',
                  (job_id, state_prev, state_new, message, gethostname()))

    def get_logs(self, job_id):
        """
        Get the full log of states of a given job from the log table.

        Parameters:
        job_id : integer (id from job table)

        Returns:
        list of JSAProcLog nametuples, 1 entry per row in log table for that
        job_id.
        """
        with self.db as c:
            c.execute('SELECT * FROM log WHERE job_id = %s', (job_id,))
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
            c.execute('SELECT * FROM log WHERE job_id = %s '
                      'ORDER BY id DESC LIMIT 1',
                      (job_id,))
            log = c.fetchall()
        if len(log) < 1:
            raise NoRowsError(
                'job',
                'SELECT * FROM log WHERE job_id = %i '
                'ORDER BY id DESC LIMIT 1' % (job_id))

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
                c.execute('UPDATE job SET location = %s WHERE id = %s',
                          (location, job_id))
            else:
                c.execute('UPDATE job SET location = %s, foreign_id = %s '
                          'WHERE id = %s',
                          (location, foreign_id, job_id))

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
            c.execute('SELECT filename FROM output_file WHERE job_id = %s',
                      (job_id,))
            output_files = c.fetchall()
            if len(output_files) == 0:
                raise NoRowsError(
                    'output_file',
                    'SELECT filename FROM output_file WHERE job_id = ' +
                    (str(job_id)))

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
        List of output files for the job (can be any iterable of strings,
        e.g. tuple etc.)

        """

        with self.db as c:

            # First of all blank out any current output files for this job_id.
            c.execute('DELETE FROM output_file WHERE job_id = %s', (job_id,))

            for f in output_files:
                # Now add in the new output files, one at a time.
                c.execute('INSERT INTO output_file (job_id, filename) '
                          'VALUES (%s, %s)',
                          (job_id, f))

    def find_errors_logs(self, location=None):
        """
        Retrieve list of all jobs in an error state, together with their logs.

        Search is limited by:
             * location (default None, can be 'JAC' or 'CADC')
        """

        param = []
        query = 'SELECT job.id, log.datetime, log.message, log.state_new, ' \
                'job.location  FROM job JOIN log ON job.id=log.job_id'
        query += ' WHERE job.state="E"'

        if location is not None:
            query += 'AND job.location=%s'
            param.append(location)

        query += ' ORDER BY job.location DESC, job.id DESC, log.id DESC'

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

    def find_jobs(self, state=None, location=None,
                  prioritize=False, number=None, offset=None,
                  sort=False, sortdir='ASC', outputs=None, count=False,
                  obsquery=None):
        """Retrieve a list of jobs matching the given values.

        Searches by the following values:

            * state (jobs in the deleted state are not returned unless
              specifically asked for)
            * location

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
        columns from the obs table giving their required value.
        This dictionary is processed by _dict_query_where_clause
        and accepts any type of value permitted by that method.

        Returns a list (which may be empty) of namedtuples, each  of which have
        values:

            * id
            * tag
            * state
            * location
            * outputs (list)

        """

        where = []
        param = []
        order = []
        join = ''

        if sortdir != 'ASC' and sortdir != 'DESC':
            raise JSAProcError('Can only sort jobs in ASC or DESC direction. '
                               'You picked %s' % (sortdir))

        if count is True:
            query = 'SELECT COUNT(*)'
        else:
            query = 'SELECT job.id, job.tag, job.state, job.location, ' \
                    'job.foreign_id'

            if outputs:
                query += ', GROUP_CONCAT(output_file.filename) '
                join = (' LEFT JOIN output_file ON job.id=output_file.job_id '
                        'AND output_file.filename LIKE %s')
                param.append(outputs)

            else:
                query += ', NULL'

        # Note: join and count cannot be used together.
        query += ' FROM job' + join

        if state is not None:
            where.append('job.state=%s')
            param.append(state)
        else:
            where.append('job.state<>%s')
            param.append(JSAProcState.DELETED)

        if location is not None:
            where.append('job.location=%s')
            param.append(location)

        if obsquery:
            (obswhere, obsparam) = _dict_query_where_clause('obs', obsquery)

            where.append('job.id IN (SELECT job_id FROM obs WHERE ' +
                         obswhere + ')')
            param.extend(obsparam)

        if where:
            query += ' WHERE ' + ' AND '.join(where)

        # If we performed a join, we need to group by job.id, on the
        # assumption that it was a one-to-many join.  If we ever
        # add any one-to-one joins, this step should be made more
        # conditional.
        if join:
            query += ' GROUP BY job.id '

        if prioritize:
            order.append('job.priority DESC')

        if sort:
            order.append('job.id ' + sortdir)

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

    def get_processing_time_obs_type(self, obsdict=None, jobdict=None,):
        """
        Get the processing time
        """
        from_query = "FROM job " + \
                     " LEFT JOIN log ON job.id = log.job_id " + \
                     " LEFT JOIN obs ON job.id = obs.job_id "
        select_query = " SELECT job.id, obs.obstype, MAX(log.datetime), " + \
                       "state_new, obs.obstype, obs.scanmode, " + \
                       "obs.survey, obs.instrument "
        where = ['state_new=%s AND job.location="JAC" AND '
                 'job.state != %s AND job.state != %s']
        param = [JSAProcState.ERROR, JSAProcState.RUNNING]
        group_query = " GROUP BY job.id "

        if obsdict:
            obsquery, obsparam = _dict_query_where_clause('obs', obsdict)
            where.append(obsquery)
            param += obsparam

        if jobdict:
            jobquery, jobparam = _dict_query_where_clause('job', jobdict)
            where.append(jobquery)
            param += jobparam

        query = select_query + from_query + \
            ' WHERE ' + ' AND '.join(where) + \
            group_query

        with self.db as c:
            c.execute(query, [JSAProcState.RUNNING] + param)
            startresults = c.fetchall()
            columns = c.description

        with self.db as c:
            c.execute(query, [JSAProcState.PROCESSED] + param)
            endresults = c.fetchall()

        return startresults, endresults, columns


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

    If the value for a single column is a list, then a row that matches any of
    the values will be returned when the query is used.  If the value
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

    if not valid_column.match(table):
        raise JSAProcError('Invalid table name "{0}"'.format(table))

    where = []
    params = []

    for key, value in wheredict.items():

        # Column names can only use valid characters.
        if not valid_column.match(key):
            raise JSAProcError('Non allowed column name %s for SQL matching' %
                               (str(key)))

        if isinstance(value, Not):
            value = value.value
            logic_not = True
        else:
            logic_not = False

        if value is None:
            where.append(
                '{0}.`{1}` IS {2}'.format(table, key,
                                          'NOT NULL' if logic_not else 'NULL'))

        elif isinstance(value, Range):
            if value.min is None and value.max is None:
                pass

            elif value.min is not None and value.max is not None:
                where.append(
                    '{0}.`{1}` {2} %s AND %s'.format(
                        table, key, 'NOT BETWEEN' if logic_not else 'BETWEEN'))
                params.extend(value)

            else:
                if value.min is not None:
                    params.append(value.min)
                    range_op = '<' if logic_not else '>='

                elif value.max is not None:
                    params.append(value.max)
                    range_op = '>' if logic_not else '<='

                where.append('{0}.`{1}` {2} %s'.format(table, key, range_op))

        elif isinstance(value, basestring) or not hasattr(value, '__iter__'):
            # If string or non iterable object, use simple comparison.
            where.append('{0}.`{1}`{2}%s'.format(table, key,
                                                 '<>' if logic_not else '='))
            params.append(value)

        else:
            # Otherwise use an IN expression.
            where.append(
                '{0}.`{1}` {2} ('.format(table, key,
                                         'NOT IN' if logic_not else 'IN') +
                ', '.join(('%s',) * len(value)) + ')')
            params.extend(value)

    if not where:
        return ('', [])

    where = (' OR ' if logic_or else ' AND ').join(where)
    return ('({0})'.format(where), params)
