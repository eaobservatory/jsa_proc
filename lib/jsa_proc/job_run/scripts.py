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

import logging
import os
from socket import gethostname

from jsa_proc.config import get_database
from jsa_proc.state import JSAProcState
from jsa_proc.error import JSAProcError, NoRowsError
from jsa_proc.job_run.decorators import ErrorDecorator
from jsa_proc.job_run.datafile_handling \
    import assemble_input_data_for_job, get_output_files
from jsa_proc.job_run.job_running import jsawrapdr_run
from jsa_proc.job_run.directories import get_input_dir

logger = logging.getLogger(__name__)


def fetch(job_id=None, db=None):
    """
    Assemble the files required to process a job.

    If it is not given a job_id, it will take the next JAC job
    with the highest priority and a state of MISSING.

    Optionally allows a database object to be given for testing purposes.
    Otherwise uses usual database from config file.

    This will raise an error if job is not in MISSING state to start with.
    This will advance the state of the job to WAITING on completion.
    Any error's raised in the process will be logged to the job log.

    """

    # Get the database.
    if not db:
        db = get_database()

    # Get next job if a job_id is not specified.
    if not job_id:
        logger.debug('Looking for a job for which to fetch data')

        jobs = db.find_jobs(state=JSAProcState.MISSING, location='JAC',
                            prioritize=True, number=1, sort=True)

        if jobs:
            job_id = jobs[0].id

        else:
            logger.warning('Did not find a job to fetch!')
            return

    fetch_a_job(job_id, db=db)


@ErrorDecorator
def fetch_a_job(job_id, db=None):
    """
    Assemble the files required to process a job.

    Requires an integer job_id.

    Optionally allows a db to be given, for testing purposes. Otherwise
    uses usual database from config file.

    This will raise an error if job is not in MISSING state to start with.
    This will advance the state of the job to WAITING on completion.
    """

    if not db:
        # Get link to database
        db = get_database()

    logger.info('About to fetch data for job %i', job_id)

    try:
        # Change status of job to 'Fetching', raise error if not in MISSING
        db.change_state(job_id, JSAProcState.FETCHING,
                        'Data is being assembled',
                        state_prev=JSAProcState.MISSING)

    except NoRowsError:
        # If the job was not in the MISSING state, it is likely that another
        # process is also trying to fetch it.  Trap the error so that the
        # ErrorDecorator does not put the job into the ERROR state as that
        # will cause the other process to fail to set the job to WAITING.
        logger.error('Job %i cannot be fetched because it is not missing',
                     job_id)
        return

    # Get the list of files.
    input_files = db.get_input_files(job_id)

    # Assemble the data.
    input_file_list = assemble_input_data_for_job(job_id, input_files)

    # Advance the state of the job to 'Waiting'.
    db.change_state(
        job_id, JSAProcState.WAITING,
        'Data has been assembled for job and job can now be executed',
        state_prev=JSAProcState.FETCHING)

    logger.info('Done fetching data for job %i', job_id)

    return job_id


def run_job(job_id=None, db=None):
    """
    Run the JSA processing of the next job. This will select the highest
    priority job in state 'WAITING' with location 'JAC'.

    Optionally an integer job_id can be given isntead to specify a specific job

    By default it will look in the database determined by the JSA_proc config.
    Optionally a database object can be given for testing purposes.

    Any errors raised will be logged in the 'log' table for the job_id.
    """

    # Get a link to the database.
    if not db:
        db = get_database()

    # Get next job if a job id is not specified
    if not job_id:
        logger.debug('Looking for a job to run')

        jobs = db.find_jobs(state=JSAProcState.WAITING, location='JAC',
                            prioritize=True, number=1, sort=True)

        if jobs:
            job_id = jobs[0].id

        else:
            logger.warning('Did not find a job to run!')
            return

    run_a_job(job_id, db=db)


@ErrorDecorator
def run_a_job(job_id, db=None):
    """
    Run the JSA processing of the given job_id (integer).

    By default it will look in the database determined by the JSA_proc
    config. Optionally a database object can be given for testing
    purposes.

    """

    if not db:
        # Get link to database
        db = get_database()

    logger.info('About to run job %i', job_id)

    try:
        # Change status of job to Running, raise an error if not currently in
        # WAITING state.
        db.change_state(job_id, JSAProcState.RUNNING,
                        'Job is about to be run on host {0}'.format(
                            gethostname()),
                        state_prev=JSAProcState.WAITING)

    except NoRowsError:
        # If the job was not in the WAITING state, it is likely that another
        # process is also trying to run it.  Trap the error so that the
        # ErrorDecorator does not put the job into the ERROR state as that
        # will cause the other process to fail to set the job to PROCESSED.
        logger.error('Job %i cannot be run because it is not waiting',
                     job_id)
        return

    # Input file_list -- this should be better? or in jsawrapdr?
    input_dir = get_input_dir(job_id)
    input_file_list = os.path.join(input_dir, 'input_files_job.lis')
    if not os.path.exists(input_file_list):
        raise JSAProcError('Input file list %s not found for job_id %i'
                           % (input_file_list, job_id))

    # Get the mode and drparameters of the job.
    job = db.get_job(id_=job_id)
    mode = job.mode
    drparameters = job.parameters

    # Run the processing job.
    logger.debug('Launching jsawrapdr: mode=%s, parameters=%s',
                 mode, drparameters)
    log = jsawrapdr_run(
        job_id, input_file_list, mode,
        drparameters,
        cleanup='cadc', location='JAC', persist=True,
        logscreen=False)

    # Create list of output files.
    logger.debug('Preparing list of output files')
    output_files = get_output_files(job_id)

    # write output files to table
    logger.debug('Storing list of output files')
    db.set_output_files(job_id, output_files)

    # Change state.
    db.change_state(
        job_id, JSAProcState.PROCESSED,
        'Job has been sucessfully processed',
        state_prev=JSAProcState.RUNNING)

    logger.info('Done running job %i', job_id)

    return job_id
