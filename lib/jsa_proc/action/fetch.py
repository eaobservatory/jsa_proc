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

from jsa_proc.action.decorators import ErrorDecorator
from jsa_proc.action.datafile_handling \
    import assemble_input_data_for_job, filter_file_list, \
    assemble_parent_data_for_job, write_input_list
from jsa_proc.admin.directories import get_output_dir
from jsa_proc.cadc.fetch import fetch_cadc_file
from jsa_proc.config import get_config, get_database
from jsa_proc.state import JSAProcState
from jsa_proc.files import get_input_dir_space, get_output_dir_space, \
    get_md5sum
from jsa_proc.error import JSAProcError, NoRowsError

logger = logging.getLogger(__name__)


def fetch(job_id=None, db=None, force=False, replaceparent=False, task=None):
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

    # Check we have sufficient disk space for fetching to occur.
    input_space = get_input_dir_space()
    required_space = float(get_config().get('disk_limit', 'fetch_min_space'))

    if input_space < required_space and not force:
        logger.warning('Insufficient disk space: %f / %f GiB required',
                       input_space, required_space)
        return

    # Get the database.
    if not db:
        db = get_database()

    # Get next job if a job_id is not specified.
    if not job_id:
        force = False

        logger.debug('Looking for a job for which to fetch data')

        jobs = db.find_jobs(state=JSAProcState.MISSING, location='JAC',
                            prioritize=True, number=1, sort=True, task=task)

        if jobs:
            job_id = jobs[0].id

        else:
            logger.warning('Did not find a job to fetch!')
            return

    fetch_a_job(job_id, db=db, force=force,replaceparent=replaceparent)


@ErrorDecorator
def fetch_a_job(job_id, db=None, force=False, replaceparent=False):
    """
    Assemble the files required to process a job.

    Requires an integer job_id.

    Optionally allows a db to be given, for testing purposes. Otherwise
    uses usual database from config file.

    Option 'replace' will force it to overwrite parent data already in the
    input directory.

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
                        state_prev=(None if force else JSAProcState.MISSING))

    except NoRowsError:
        # If the job was not in the MISSING state, it is likely that another
        # process is also trying to fetch it.  Trap the error so that the
        # ErrorDecorator does not put the job into the ERROR state as that
        # will cause the other process to fail to set the job to WAITING.
        logger.error('Job %i cannot be fetched because it is not missing',
                     job_id)
        return

    # Assemble any files listed in the input files tree
    try:
        input_files = db.get_input_files(job_id)
        input_files_with_paths = assemble_input_data_for_job(job_id,
                                                             input_files)
    except NoRowsError:
        input_files_with_paths = []

    # Assemble any files from the parent jobs
    try:
        parents = db.get_parents(job_id)
        parent_files_with_paths = []
        for p, f in parents:
            outputs = db.get_output_files(p)
            parent_files = filter_file_list(outputs, f)
            parent_files_with_paths += assemble_parent_data_for_job(
                job_id, p, parent_files, force_new=replaceparent)
    except NoRowsError:
        parent_files_with_paths = []

    # Write out list of all input files with full path list
    files_list = input_files_with_paths + parent_files_with_paths
    list_name_path = write_input_list(job_id, files_list)

    # Advance the state of the job to 'Waiting'.
    db.change_state(
        job_id, JSAProcState.WAITING,
        'Data has been assembled for job and job can now be executed',
        state_prev=JSAProcState.FETCHING)

    logger.info('Done fetching data for job %i', job_id)

    return job_id


def fetch_output(job_id=None, location=None, task=None,
                 dry_run=False, force=False):
    """Output fetch function for use from scripts."""

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    # Find next job if not specified.
    if job_id is None:
        force = False

        logger.debug('Looking for a job to fetch output data for')

        jobs = db.find_jobs(
            state=JSAProcState.INGEST_QUEUE, location=location, task=task,
            prioritize=True, number=1, sort=True)

        if jobs:
            job_id = jobs[0].id

        else:
            logger.warning('Did not find a job to fetch output data for!')
            return

    _fetch_job_output(job_id, db=db, force=force, dry_run=dry_run)


@ErrorDecorator
def _fetch_job_output(job_id, db, force=False, dry_run=False):
    """Private function to perform retrieval of job output files from CADC.
    """

    # Check we have sufficient disk space for fetching to occur.
    output_space = get_output_dir_space()
    required_space = float(get_config().get('disk_limit', 'fetch_min_space'))

    if output_space < required_space and not force:
        logger.warning('Insufficient disk space: %f / %f GiB required',
                       output_space, required_space)
        return

    logger.info('About to retreive output data for job %i', job_id)

    # Change state from INGEST_QUEUE to INGEST_FETCH.
    if not dry_run:
        try:
            db.change_state(
                job_id, JSAProcState.INGEST_FETCH,
                'Output data are being retrieved',
                state_prev=(None if force else JSAProcState.INGEST_QUEUE))
        except NoRowsError:
            logger.error('Job %i cannot have output data fetched'
                         ' as it not waiting for reingestion', job_id)
            return

    # Check state of output files.
    output_dir = get_output_dir(job_id)
    output_files = db.get_output_files(job_id, with_info=True)
    missing_files = []

    for file in output_files:
        filename = file.filename
        filepath = os.path.join(output_dir, filename)

        if os.path.exists(filepath):
            # If we still have the file, check its MD5 sum is correct.
            if file.md5 == get_md5sum(filepath):
                logger.debug('PRESENT: %s', filename)
            else:
                raise JSAProcError('MD5 sum mismatch for existing file {0}'
                                   .format(filename))

        else:
            # Otherwise add it to the list of missing files.
            logger.debug('MISSING: %s', filename)
            missing_files.append(file)

    # Are there any files we need to retrieve?
    if missing_files:
        for file in missing_files:
            filename = file.filename
            filepath = os.path.join(output_dir, filename)

            if not dry_run:
                if os.path.exists(output_dir):
                    logger.debug('Directory %s already exists', output_dir)
                else:
                    logger.debug('Making directory %s', output_dir)
                    os.makedirs(output_dir)

                logger.info('Fetching file %s', filename)
                fetch_cadc_file(filename, output_dir, suffix='')

                if file.md5 == get_md5sum(filepath):
                    logger.debug('MD5 sum OK: %s', filename)
                else:
                    raise JSAProcError('MD5 sum mismatch for fetched file {0}'
                                       .format(filename))
            else:
                logger.info('Skipping fetch of %s (DRY RUN)', filename)

    else:
        logger.info('All output files are already present')

    # Finally set the state to INGESTION.
    if not dry_run:
        db.change_state(
            job_id, JSAProcState.INGESTION,
            'Output data have been retrieved',
            state_prev=JSAProcState.INGEST_FETCH)
