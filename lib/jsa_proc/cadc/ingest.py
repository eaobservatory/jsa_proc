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
import subprocess

from jsa_proc.action.decorators import ErrorDecorator
from jsa_proc.admin.directories import get_output_dir, \
    open_log_file, make_temp_scratch_dir
from jsa_proc.config import get_database
from jsa_proc.error import CommandError, NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.util import restore_signals

logger = logging.getLogger(__name__)


def ingest_output(job_id, location=None, task=None, dry_run=False):
    """High-level output ingestion function for use from scripts."""

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    if job_id is not None:
        job_ids = [job_id]

    else:
        job_ids = [x.id for x in db.find_jobs(state=JSAProcState.INGESTION,
                                              location=location,
                                              task=task)]

    for job_id in job_ids:
        if not dry_run:
            try:
                # Change the state from INGESTION to INGESTING, raising an
                # error if the job was not already in that state.
                db.change_state(job_id, JSAProcState.INGESTING,
                                'Job output is being ingested into CAOM-2',
                                state_prev=JSAProcState.INGESTION)

            except NoRowsError:
                logger.error('Job %i can not be ingested as it is not ready',
                             job_id)

                continue

            _perform_ingestion(job_id=job_id, db=db)

        else:
            logger.info('Skipping ingestion of job %i (DRY RUN)', job_id)


@ErrorDecorator
def _perform_ingestion(job_id, db):
    """Private function to peform the CAOM-2 ingestion.

    Runs under the ErrorDecorator to capture errors.  Sets the job state
    to COMPLETE if it finishes successfully, or ERROR otherwise.
    """

    logger.debug('Preparing to ingest ouput for job {0}'.format(job_id))

    output_dir = get_output_dir(job_id)
    scratch_dir = make_temp_scratch_dir(job_id)
    logger.debug('Using scratch directory %s', scratch_dir)

    try:
        with open_log_file(job_id, 'ingestion') as log:
            logger.debug('Invoking jcmt2caom2ingest, log file: %s', log.name)

            subprocess.check_call(
                [
                    'jcmt2caom2ingest',
                    '--ingest',
                    '--collection', 'JCMT',
                    '--major', output_dir,
                ],
                shell=False,
                cwd=scratch_dir,
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=restore_signals)

            db.change_state(job_id, JSAProcState.COMPLETE,
                            'CAOM-2 ingestion completed successfully',
                            state_prev=JSAProcState.INGESTING)

            logger.info('Done ingesting ouput for job {0}'.format(job_id))

    except subprocess.CalledProcessError as e:
        db.change_state(job_id, JSAProcState.ERROR,
                        'CAOM-2 ingestion failed')

        logger.exception('Error during CAOM-2 ingestion of job %i', job_id)
