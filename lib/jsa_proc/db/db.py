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
        """
    
        if not id_ or tag:
            raise JSAProcDBError("You must set either id_ or tag to use get_job")
        if id_:
            name='id'
            value=id_
        else:
            name='tag'
            value=tag

        self.db.db.execute('SELECT all FROM job WHERE '+name+'=?', (value))
        job = db.db.fetch_all()

        # Should this check to see that only 1 result is returned?
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

        """

        with self.db as c:
            
            # Change the state to new state and update the state_prev
            c.execute('UPDATE job SET state_prev = state, state = ? WHERE id = ?',
                      (newstate, job_id))

            # Get state_prev value.
            c.execute('SELECT state_prev FROM job where id=job_id')
            state_prev = c.fetch_all()

            # Update log table.
            c.execute('INSERT INTO log (job_id, state_prev, state_new, message) VALUES (?, ?, ?)',
                      (job_id, state_prev, newstate, message))
            
        
        return job_id
