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


from collections import namedtuple

# Named tuples that are created ahead of time instead of dynamically
# defined from table rows:
JSAProcLog = namedtuple('JSAProcLog', 'id job_id datetime state_prev state_new message')


class JSAProcDB:
    """JSA Processing database access class.

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
    
        if not id_ or tag:
            raise Exception("You must set either id_ or tag to use get_job")

        if id_:
            name='id'
            value=id_
        else:
            name='tag'
            value=tag

        # Get the values form the database
        with self.db as c:
            c.execute('SELECT * FROM job WHERE '+name+'=?', (value,))
            job = c.fetchall()
            if len(job) == 0:
                raise exception('get_job found 0 jobs matching %s=%s'%(name, str(value)))
            if len(job) > 1:
                raise Exception('get_job found more than one job with id or tag %s'%(value))
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

    def add_job(self, tag, location, input_file_names, foreign_id=None):
        """
        Add a JSA data processing job to the database.

        This will raise an error if the job already exists, if the
        database interface raises an erro. The job must be specified
        by its unique tag.
        
        tag: string, unique identifier for observation/job

        location: string, where the job will be run.

        input_file_names: iterable, each item being a string that
        idetnifies the path of an input file for the job.

        foreign_id: OPTIONAL, default=None. (string), identifier from
        foreign system (probably  CADC).
        

        Does not return anything
        """

        # insert job into table
        with self.db as c:
            c.execute('INSERT INTO job (tag, location, foreign_id) VALUES (?, ?, ?)', 
                           (tag, location, foreign_id))
        
            # Get the autoincremented id from job table (job_id in all other tables)
            job_id = c.lastrowid
        
            # Need to get input file names and add them to table input_file
            for filepath in input_file_names:
                c.execute('INSERT INTO input_file (job_id, filename) VALUES (?, ?)',
                          (job_id, filepath))

        # job_id may not be necessary but sometimes useful.
        return job_id
            
                       
    def change_state(self, job_id, newstate, message):
        """
        Change the state of a job in the JSA processing database.

        the id is the id in the job table, which uniquely identifies a
        job. The newstate will be written into the state column, and
        the current state will be moved over to the state_prev column.

        The log table will be updated with a line corresponding to
        this change, using the message.

        variables:
        id: integer, job_id of the job whose state is being changed.

        newstate: string, one character, new state of the job.
        
        message: string, human readable text describin the change of state.

        Return:
        job_id, integer

        """

        with self.db as c:
            
            # Change the state to new state and update the state_prev
            c.execute('UPDATE job SET state_prev = state, state = ? WHERE id = ?',
                      (newstate, job_id))

            # Get state_prev value.
            c.execute('SELECT state_prev FROM job where id=?',
                      (job_id,))
            state_prev = c.fetchall()

            if len(state_prev) > 1:
                raise Exception('more than one result for state_prev for job %i'%(job_id), 
                                    state_prev)
            state_prev=state_prev[0][0]

            # Update log table.
            c.execute('INSERT INTO log (job_id, state_prev, state_new, message) VALUES (?, ?, ?, ?)',
                      (job_id, state_prev, newstate, message))
            
        return job_id

    def get_input_files(self, job_id):
        """
        Get the list of input files for specific job from the
        input_file table.

        takes integer job_id to identify file (this is the
        auto-incremented primary key from the job table)

        Returns a list of filepaths
        """

        with self.db as c:
            c.execute('SELECT filename FROM input_file where job_id=?',
                      (job_id,))
            input_files = c.fetchall()

        # input_files will be a list of tuples, each tuple containgin
        # one file. Flatten this into a list of strings.
        input_files = [file for i in input_files for file in i]

        return input_files


    def get_logs(self, job_id):
        """
        Get the full log of states of a given job from the log table.

        Parameters:
        job_id : integer (id from job table)

        Returns:
        list of JSAProcLog nametuples, 1 entry per row in file for that job_id.
        """
        with self.db as c:
            c.execute('SELECT * FROM log WHERE job_id='+str(job_id))
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
            c.execute('SELECT * FROM log WHERE id = (SELECT MAX(id) FROM log WHERE job_id = ?)',
                      (job_id,))
            log = c.fetchall()
        if len(log) > 1:
            raise StandardError('get_last_log found more than one row?', log)

        log = JSAProcLog(*log[0])
        return log

