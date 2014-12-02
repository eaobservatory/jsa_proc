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
import os.path
import shutil

from jsa_proc.admin.directories import get_input_dir, get_scratch_dir
from jsa_proc.config import get_database
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


def clean_input(count=None, dry_run=False):
    """Delete input directories for processed jobs."""

    logger.debug('Beginning input clean')

    _clean_job_directories(
        get_input_dir,
        [
            JSAProcState.INGESTION,
            JSAProcState.COMPLETE,
            JSAProcState.DELETED,
        ],
        count=count,
        dry_run=dry_run)

    logger.debug('Done cleaning input directories')


def clean_scratch(count=None, dry_run=False, include_error=False):
    """Delete scratch directories for processed jobs."""

    logger.debug('Beginning scratch clean')

    states = [
            JSAProcState.COMPLETE,
            JSAProcState.DELETED,
    ]

    if include_error:
        states.append(JSAProcState.ERROR)

    logger.debug('Cleaning jobs in states: %s', ', '.join(states))

    _clean_job_directories(
        get_scratch_dir,
        states,
        count=count,
        dry_run=dry_run)

    logger.debug('Done cleaning scratch directories')


def _clean_job_directories(dir_function, state, count=None, dry_run=False):
    """Generic directory deletion function."""

    db = get_database()
    jobs = db.find_jobs(location='JAC', state=state)

    n = 0
    for job in jobs:
        directory = dir_function(job.id)

        if not os.path.exists(directory):
            logger.debug('Directory for job %i does not exist', job.id)
            continue

        logger.info('Removing directory for job %i: %s', job.id, directory)

        if not dry_run:
            shutil.rmtree(directory)

        n += 1
        if (count is not None) and not (n < count):
            break
