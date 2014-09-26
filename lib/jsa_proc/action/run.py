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
import re
from socket import gethostname

from jsa_proc.admin.directories import get_input_dir
from jsa_proc.config import get_database
from jsa_proc.state import JSAProcState
from jsa_proc.error import JSAProcError, NoRowsError
from jsa_proc.action.decorators import ErrorDecorator
from jsa_proc.action.datafile_handling import get_output_files
from jsa_proc.action.job_running import jsawrapdr_run
from jsa_proc.jac.file import hpx_tiles_from_filenames

logger = logging.getLogger(__name__)

# Regular expression to match tasks that produce hpx output
hpx_task = re.compile('^hpx-')

def run_job(job_id=None, db=None, force=False):
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
        force = False

        logger.debug('Looking for a job to run')

        jobs = db.find_jobs(state=JSAProcState.WAITING, location='JAC',
                            prioritize=True, number=1, sort=True)

        if jobs:
            job_id = jobs[0].id

        else:
            logger.warning('Did not find a job to run!')
            return

    run_a_job(job_id, db=db, force=force)


@ErrorDecorator
def run_a_job(job_id, db=None, force=False):
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
                            gethostname().partition('.')[0]),
                        state_prev=(None if force else JSAProcState.WAITING))

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
    input_file_list_path = os.path.join(input_dir, 'input_files_job.lis')
    if not os.path.exists(input_file_list_path):
        raise JSAProcError('Input file list %s not found for job_id %i'
                           % (input_file_list_path, job_id))



    # Check every file on input_file list exists.
    inputfl = open(input_file_list_path, 'r')

    for input_file in inputfl:
        input_file = input_file.strip()
        if os.path.isfile(input_file)  is False:

            # If a file is missing, get log.
            logstring = 'Input file %s for job %i has gone missing' % (input_file, job_id)
            logger.error(logstring)
            logs = db.get_logs(job_id)
            states = [i.state_new for i in logs]

            # If it has only been in the state MISSING twice before, then try again.
            if states.count(JSAProcState.MISSING) <= 2:
                logstring += ': moving to missing.'
                logger.warning('Moving job %i to state MISSING due to missing file(s) %s',
                            job_id, input_file)
                db.change_state(job_id, JSAProcState.MISSING,
                                logstring, state_prev = JSAProcState.RUNNING)
                return job_id

            else:
                # If it has been in the missing STATE more than two times, give up and
                # move it into ERROR state to be fixed manually.
                logstring += ': moving to error.'
                logger.info('Moving job %s to state ERROR due to missing file(s).', job_id)
                inputfl.close()
                raise JSAProcError('Input file %s for job %i has gone missing.'
                                       % (input_file, job_id))

    inputfl.close()
    logger.debug('All input files found for job %s.', job_id)


    # Get the mode and drparameters of the job.
    job = db.get_job(id_=job_id)
    mode = job.mode
    drparameters = job.parameters

    # Run the processing job.
    logger.debug('Launching jsawrapdr: mode=%s, parameters=%s',
                 mode, drparameters)
    log = jsawrapdr_run(
        job_id, input_file_list_path, mode,
        drparameters,
        cleanup='cadc', location='JAC', persist=True)

    # Create list of output files.
    logger.debug('Preparing list of output files')
    output_files = get_output_files(job_id)

    # write output files to table
    logger.debug('Storing list of output files')
    db.set_output_files(job_id, output_files)

    # If task begins with hpx, get tiles from list of output_files
    # and write to tile table in db.
    if hpx_task.search(job.task):
        logger.debug('Storing list of output tiles for HPX job ' + str(job_id))
        tiles = hpx_tiles_from_filenames(output_files)
        db.set_tilelist(job_id, tiles)
        logger.debug('Job ' + str(job_id) + ' produced output on tiles ' + \
                     ', '.join(str(i) for i in tiles))

    # Change stateof job.
    db.change_state(
        job_id, JSAProcState.PROCESSED,
        'Job has been sucessfully processed',
        state_prev=JSAProcState.RUNNING)

    logger.info('Done running job %i', job_id)

    return job_id
