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
            JSAProcState.WONTWORK,
        ],
        count=count,
        dry_run=dry_run)

    logger.debug('Done cleaning input directories')


def clean_output(count=None, dry_run=False, task=None, **kwargs):
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
        dry_run=dry_run,
        clean_function_kwargs=kwargs)

    logger.debug('Done cleaning output directories')


def clean_scratch(count=None, dry_run=False,
                  include_error=False, include_ingestion=False,
                  include_processed=False):
    """Delete scratch directories for processed jobs."""

    logger.debug('Beginning scratch clean')

    states = [
        JSAProcState.COMPLETE,
        JSAProcState.DELETED,
        JSAProcState.WONTWORK,
    ]

    if include_error:
        states.append(JSAProcState.ERROR)

    if include_ingestion:
        states.append(JSAProcState.INGESTION)

    if include_processed:
        states.append(JSAProcState.PROCESSED)

    logger.debug('Cleaning jobs in states: %s', ', '.join(states))

    _clean_job_directories(
        get_scratch_dir,
        states,
        count=count,
        dry_run=dry_run)

    logger.debug('Done cleaning scratch directories')


def _clean_job_directories(dir_function, state, task=None, count=None,
                           clean_function=None, dry_run=False,
                           clean_function_kwargs={}):
    """Generic directory deletion function.

    If a clean_function is given, it should return True when it is
    able to clean a directory and False otherwise.  It will
    be passed the extra clean_function_kwargs keyword arguments.
    """

    db = get_database()
    jobs = db.find_jobs(location='JAC', state=state, task=task)

    n = 0
    for job in jobs:
        directory = dir_function(job.id)

        if not os.path.exists(directory):
            logger.debug('Directory for job %i does not exist', job.id)
            continue

        try:
            if clean_function is None:
                logger.info('Removing directory for job %i: %s',
                            job.id, directory)

                if not dry_run:
                    shutil.rmtree(directory)

                n += 1

            else:
                if clean_function(directory, job_id=job.id, db=db,
                                  dry_run=dry_run,
                                  **clean_function_kwargs):
                    n += 1

            if (count is not None) and not (n < count):
                break

        except:
            logger.exception('Error removing directory for job %i: %s',
                             job.id, directory)


def _clean_output_dir(directory, job_id, db, dry_run, no_cadc_check=False):
    """Clean non-previews from an output file after double-checking
    everything is present at CADC.

    As required for _clean_job_directories, returns True if it cleaned up
    the directory, and False otherwise.

    CADC checking can be skipped by setting no_cadc_check=True.  Note that
    this is potentially dangerous and removes the check that the output files
    really are at CADC and have been ingested into CAOM-2.
    """

    # Get a list of the non-preview files in the directory.
    non_preview = [x for x in os.listdir(directory) if not x.endswith('.png')]

    if not non_preview:
        # There is nothing to do, so issue a debugging log message and
        # return False.

        logger.debug('Directory for job %i has no non-preview files', job_id)
        return False

    deletable = []

    # Consider files other than those which are either a preview or not under
    # consideration for deletion.
    output_files = filter(
        (lambda x: x.filename in non_preview),
        db.get_output_files(job_id, with_info=True))

    if no_cadc_check:
        # When skipping CADC checks, assume files are all in CAOM-2.
        files_in_caom2 = [True for x in output_files]
    else:
        # Prepare CADC tap client.
        caom2 = CADCTap()

        # Query for files in CAOM-2.
        files_in_caom2 = caom2.check_files([x.filename for x in output_files])

    for (file, in_caom2) in zip(output_files, files_in_caom2):
        # Check whether the file has been ingested into CAOM-2.
        if not in_caom2:
            logger.warning('File %s is not in CAOM-2', file.filename)
            break

        if not no_cadc_check:
            # Check whether the file is identical to what CADC have in AD.
            cadc_md5 = fetch_cadc_file_info(file.filename)['content-md5']

            if file.md5 != cadc_md5:
                logger.warning('File %s has MD5 mismatch', file.filename)
                break

        deletable.append(file.filename)

    else:
        logger.info('Removing output for job %i from %s', job_id, directory)

        for file in deletable:
            filepath = os.path.join(directory, file)

            if not dry_run:
                logger.debug('Deleting file %s', filepath)
                os.unlink(filepath)

            else:
                logger.debug(
                    'Skipping deletion of file %s (DRY RUN)', filepath)

        return True

    return False
