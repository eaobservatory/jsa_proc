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

from __future__ import print_function, division, absolute_import

import logging
import os.path
import subprocess

from jsa_proc.action.decorators import ErrorDecorator
from jsa_proc.admin.directories import get_output_dir, \
    open_log_file, make_temp_scratch_dir
from jsa_proc.config import get_database
from jsa_proc.error import JSAProcError, CommandError, NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.util import restore_signals

logger = logging.getLogger(__name__)


def ingest_output(
        job_id, location=None, task=None, dry_run=False, force=False):
    """High-level output ingestion function for use from scripts."""

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    if job_id is not None:
        jobs = [db.get_job(id_=job_id)]

    else:
        jobs = db.find_jobs(
            state=JSAProcState.INGESTION,
            location=location,
            task=task, prioritize=True)

    # Get full list of tasks.
    task_info = db.get_task_info()

    for job in jobs:
        job_task_info = task_info.get(job.task)

        command_ingest = None
        description = 'into CAOM-2'

        if ((job_task_info is not None)
                and (job_task_info.command_ingest is not None)):
            command_ingest = job_task_info.command_ingest
            description = 'via custom process'

        if not dry_run:
            try:
                # Change the state from INGESTION to INGESTING, raising an
                # error if the job was not already in that state.
                db.change_state(
                    job.id, JSAProcState.INGESTING,
                    'Job output is being ingested {}'.format(description),
                    state_prev=(None if force else JSAProcState.INGESTION))

            except NoRowsError:
                # This would normally be a "logger.error", but we routinely
                # run multiple copies of this ingestion routine simultaneously
                # and it is therefore expected that a lot of jobs will have
                # already been moved out of the INGESTION state by other
                # processes.  Therefore a warning or error should not be
                # logged as these lead to unnecessary warnings in the cron
                # job monitor.
                logger.debug('Job %i can not be ingested as it is not ready',
                             job.id)

                continue

            _perform_ingestion(job_id=job.id, db=db, command_ingest=command_ingest)

        else:
            logger.info(
                'Skipping ingestion %s of job %i (DRY RUN)',
                description, job.id)


@ErrorDecorator
def _perform_ingestion(job_id, db, command_ingest=None):
    """Private function to peform the ingestion.

    Runs under the ErrorDecorator to capture errors.  Sets the job state
    to COMPLETE if it finishes successfully, or ERROR otherwise.
    """

    logger.debug('Preparing to ingest ouput for job {0}'.format(job_id))

    output_dir = get_output_dir(job_id)

    logger.debug('Checking that output files are present for ingestion')
    try:
        output_files = db.get_output_files(job_id)
        for filename in output_files:
            if not os.path.exists(os.path.join(output_dir, filename)):
                raise JSAProcError(
                    'Output file {0} is missing'.format(filename))
    except NoRowsError:
        raise JSAProcError('Job has no output files to ingest')

    with open_log_file(job_id, 'ingestion') as log:
        try:
            if command_ingest is None:
                scratch_dir = make_temp_scratch_dir(job_id)
                logger.debug('Using scratch directory %s', scratch_dir)

                logger.debug('Invoking jsaingest, log file: %s', log.name)

                subprocess.check_call(
                    [
                        'jsaingest',
                        '--ingest',
                        '--collection', 'JCMT',
                        '--indir', output_dir,
                    ],
                    shell=False,
                    cwd=scratch_dir,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=restore_signals)

            else:
                logger.debug(
                    'Invoking custom ingestion script %s, log file: %s',
                    command_ingest, log.name)

                subprocess.check_call(
                    [
                        command_ingest,
                        '--transdir', output_dir,
                    ],
                    shell=False,
                    cwd='/tmp',
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=restore_signals)

            db.change_state(job_id, JSAProcState.COMPLETE,
                            'Ingestion completed successfully',
                            state_prev=JSAProcState.INGESTING)

            logger.info('Done ingesting ouput for job {0}'.format(job_id))

        except subprocess.CalledProcessError as e:
            # Attempt to get the first message beginning with ERROR from
            # the log file.

            # Go back to the start of the log and read in the data.
            log.seek(0)
            content = '\n'.join(log.readlines())
            errorline = content[content.find('\nERROR '):].split('\n')[1]

            db.change_state(job_id, JSAProcState.ERROR,
                            'Ingestion failed\n' + errorline)

            logger.exception('Error during ingestion of job %i', job_id)
