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


from jsa_proc.config.get_database()
from jsa_proc.state import JSAProcState
from jsa_proc.error import JSAProcError
from jsa_proc.job_run.datafile_handling import assemble_input_data_for_job
from jsa_proc.job_run.job_running import jsawrapdr_run


def fetch(job_id, db=None):
    """
    Assemble the files required to process a job.

    Requires an integer job_id.

    Optionally allows a db to be given, for testing purposes. Otherwise 
    uses usualy database from config

    This will raise an error if job is not in QUEUED state to start with.
    This will advance the state of the job to WAITING on completion.
    """

    if not db:
        # Get link to database
        db = get_database()

    # Change status of job to 'Fetching', raise error if not in QUEUED
    db.change_state(job_id, JSAProcState.FETCHING, 'Data is being assembled',
                 state_prev=JSAProcState.QUEUED)

    # Get the list of files.
    input_files = db.get_input_files(job_id)

    # Assemble the data.
    input_file_list = assemble_input_data_for_job(job_id, input_files)

    # Advance the state of the job to 'Waiting'.
    db.change_state(job_id, JSAProcState.WAITING,
                 'Data has been assembled for job and job can now be executed',
                 state_prev=JSAProcSTATE.FETCHING)

    return job_id



def run_job(job_id, db=None):
    """
    Run the JSA processing of a given job.

    Requries an integer job_id.

    Optionally allows a db to be provided for testing
    purposes. Otherwise it will link to the database in the jsa_proc
    config.

    """
    if not db:
        # Get link to database
        db = get_database()

    # Change status of job to Running, raise an error if not currently in
    # WAITING state.
    db.change_state(job_id, JSAProcState.RUNNING,
                 'Job is about to be run', state_prev=JSAProcState.WAITING)

    # input file_list -- this hsould be better? or in jsawrapdr?
    input_dir = get_input_dir(job_id)
    input_file_list = os.path.join(input_dir, 'input_files_job.lis')
    if not os.path.exists(input_file_list):
        raise JSAProcERROR('Input file list %s not found for job_id %i'
                           % (input_file_list, job_id))

    # Get the mode and drparameters of the job.
    job = db.get_job(id_=job_id)
    mode = job.mode
    drparameters = job.parameters

    # Run the processing job.
    log = jsawrapdr_run(job_id, input_file_list, mode, drparameters,
                       'REDUCE_SCAN_JSA_PUBLIC',
                       cleanup='CADC', location='JAC', persist=True)
    # Change state
    db.change_state(job_id, JSAProcSTATE.PROCESSED,
                 'Job has been sucessfully processed',
                 state_prev = JSAProcState.RUNNING)

    return job_id

