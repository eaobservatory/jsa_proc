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
import shutil

from jsa_proc.admin.directories import get_input_dir, get_output_dir, \
    get_scratch_dir
from jsa_proc.cadc.fetch import fetch_cadc_file_info
from jsa_proc.cadc.tap import CADCTap
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


def clean_output(count=None, dry_run=False, task=None):
    """Delete files other than previews from job output directories."""

    logger.debug('Beginning output clean')

    _clean_job_directories(
        get_output_dir,
        [
            JSAProcState.COMPLETE,
        ],
        task=task,
        count=count,
        clean_function=_clean_output_dir,
        dry_run=dry_run)

    logger.debug('Done cleaning output directories')


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


def _clean_job_directories(dir_function, state, task=None, count=None,
                           clean_function=None, dry_run=False):
    """Generic directory deletion function.

    If a clean_function is given, it should return True when it is
    able to clean a directory and False otherwise.
    """

    db = get_database()
    jobs = db.find_jobs(location='JAC', state=state, task=task)

    n = 0
    for job in jobs:
        directory = dir_function(job.id)

        if not os.path.exists(directory):
            logger.debug('Directory for job %i does not exist', job.id)
            continue

        if clean_function is None:
            logger.info('Removing directory for job %i: %s', job.id, directory)

            if not dry_run:
                shutil.rmtree(directory)

            n += 1

        else:
            if clean_function(directory, job_id=job.id, db=db, dry_run=dry_run):
                n += 1

        if (count is not None) and not (n < count):
            break


def _clean_output_dir(directory, job_id, db, dry_run):
    """Clean non-previews from an output file after double-checking
    everything is present at CADC.

    As required for _clean_job_directories, returns True if it cleaned up
    the directory, and False otherwise.
    """

    # Get a list of the non-preview files in the directory.
    non_preview = [x for x in os.listdir(directory) if not x.endswith('.png')]

    if not non_preview:
        # There is nothing to do, so issue a debugging log message and
        # return False.

        logger.debug('Directory for job %i has no non-preview files', job_id)
        return False

    # Prepare CADC tap client.
    caom2 = CADCTap()
    deletable = []

    for file in db.get_output_files(job_id, with_info=True):
        if file.filename not in non_preview:
            # The file is either a preview or not under consideration
            # for deletion.
            continue

        # Check whether the file is identical to what CADC have in AD.
        cadc_md5 = fetch_cadc_file_info(file.filename)['content-md5']

        if file.md5 != cadc_md5:
            logger.warning('File %s has MD5 mismatch', file.filename)
            break

        # Check whether the file has been ingested into CAOM-2.
        if not caom2.check_file(file.filename):
            logger.warning('File %s is not in CAOM-2', file.filename)
            break

        deletable.append(file.filename)

    else:
        logger.info('Removing output for job %i from %s', job_id, directory)

        for file in deletable:
            filepath = os.path.join(directory, file)
            logger.debug('Deleting file %s', filepath)

            if not dry_run:
                os.unlink(filepath)

        return True

    return False
