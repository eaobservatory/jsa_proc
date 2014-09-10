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

from codecs import latin_1_encode
import grp
import os
import pwd
import shutil
from socket import gethostname
import logging

from jsa_proc.cadc.files import CADCFiles
from jsa_proc.config import get_config, get_database
from jsa_proc.error import CommandError, NoRowsError
from jsa_proc.job_run.directories import get_output_dir
from jsa_proc.job_run.decorators import ErrorDecorator
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


def etransfer_poll_output(dry_run):
    """High level polling function to use from scripts."""

    logger.debug('Preparing to poll the e-transfer system for job output')

    # When not in dry run mode, check that etransfer is being
    # run on the correct machine by the correct user.
    if not dry_run:
        _etransfer_check_config()

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    logger.debug('Preparing CADC files object')
    ad = CADCFiles()

    n_err = 0

    for job in self.db.find_jobs(location='JAC',
                                 state=JSAProcState.TRANSFERRING):
        job_id = job.id
        logger.debug('Checking state of job %i', job_id)

        logger.debug('Retrieving list of output files')
        try:
            files = db.get_output_files(job_id)

        except NoRowsError:
            logger.error('Did not find output files for job %i', job_id)
            n_err += 1
            continue

    logger.debug('Done polling the e-transfer system')

    if n_err:
        raise CommandError('Errors were encountered polling e-transfer')


def etransfer_send_output(job_id, dry_run):
    """High level e-transfer function for use from scripts.

    This function makes some basic checks and then launches
    the private function _etransfer_send under the control
    of the ErrorDecorator so that any subsequent errors
    are captured.
    """

    logger.debug('Preparing to e-transfer output for job {0}'.format(job_id))

    # When not in dry run mode, check that etransfer is being
    # run on the correct machine by the correct user.
    if not dry_run:
        _etransfer_check_config()

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    job = db.get_job(id_=job_id)

    if job.state != JSAProcState.PROCESSED:
        message = 'Job {0} cannot be e-transferred as it is in ' \
                  'state {1}'.format(job_id, JSAProcState.get_name(job.state))
        logger.error(message)
        raise CommandError(message)

    _etransfer_send(job_id, dry_run=dry_run, db=db)

    logger.debug('Done adding output for job {0} to e-transfer'.format(job_id))


@ErrorDecorator
def _etransfer_send(job_id, dry_run, db):
    """Private function to copy job output into the e-transfer
    directories.

    Runs under the ErrorDecorator so that errors are captured.
    """

    config = get_config()
    scratchdir = config.get('etransfer', 'scratchdir')
    transdir = config.get('etransfer', 'transdir')
    group_id = grp.getgrnam(config.get('etransfer', 'group')).gr_gid

    logger.debug('Preparing CADC files object')
    ad = CADCFiles()

    logger.debug('Retrieving list of output files')
    try:
        files = db.get_output_files(job_id)

    except NoRowsError:
        raise CommandError('No output files found for job {0}'.format(job_id))

    logger.debug('Checking that all files are present')
    outdir = get_output_dir(job_id)
    for file in files:
        if not os.path.exists(os.path.join(outdir, file)):
            raise CommandError('File {0} not in directory {1}'.
                               format(file, outdir))

    logger.debug('Checking that files are not in the scratch directory')
    scratchfiles = os.listdir(scratchdir)
    for file in files:
        if file in scratchfiles:
            raise CommandError('File {0} is in e-transfer scratch directory'.
                               format(file))

    logger.debug('Checking whether the files are already in e-transfer')
    etransfer_status = etransfer_file_status(files)
    if any(etransfer_status):
        for (file, status) in zip(files, etransfer_status):
            if status:
                (ok, dir) = status
                logger.error('File {0} already in e-transfer directory {1}'.
                             format(file, dir))
        raise CommandError('Some files are already in e-transfer directories')

    logger.debug('Checking which files are already at CADC')
    present = ad.check_files(files)

    for (file, replace) in zip(files, present):
        if replace:
            target_type = 'replace'
        else:
            target_type = 'new'

        logger.info('Placing file %s in "%s" directory', file, target_type)

        source_file = os.path.join(outdir, file)
        scratch_file = os.path.join(scratchdir, file)
        target_file = os.path.join(transdir, target_type, file)

        if not dry_run:
            # Copy the file into the scratch directory and prepare its
            # file permissions.
            shutil.copyfile(source_file, scratch_file)
            os.chown(scratch_file, -1, group_id)
            os.chmod(scratch_file, 0o664)

            # Move the file to the target directory.  This is done so that
            # the file appears atomically in the target directory in order
            # to prevent the e-transfer system seeing only part of the file.
            os.rename(scratch_file, target_file)

        else:
            logger.debug('Skipping e-transfer (DRY RUN)')

    # Finally set the state of the job to TRANSFERRING
    if not dry_run:
        db.change_state(
            job_id, JSAProcState.TRANSFERRING,
            'Output files have been copied into the e-transfer directories',
            state_prev=JSAProcState.PROCESSED)


def etransfer_file_status(files):
    """Determine the current e-transfer status of a given file.

    Essentially this looks for the files in the e-transfer directory
    structure and tells you which directory it is in.

    Parameters:
        files: list of file names to check.

    Return:
        List with entries corresponding to those in the input file list.
        the value will be None if the file is not found in the
        e-transfer directories, or a tuple which is one of:

            (True, 'new')
            (True, 'replace')
            (False, rejection_reason)

        i.e. None means the file is not found, (True, ...) means that it is
        in progress and (False, ...) indicates an error.
    """

    config = get_config()
    transdir = config.get('etransfer', 'transdir')

    new = set(os.listdir(os.path.join(transdir, 'new')))
    replace = set(os.listdir(os.path.join(transdir, 'replace')))
    reject = dict((
        (file, reason)
        for reason in os.listdir(os.path.join(transdir, 'reject'))
        for file in os.listdir(os.path.join(transdir, 'reject', reason))))

    return map((lambda file: (False, reject[file]) if file in reject
               else (True, 'new') if file in new
               else (True, 'replace') if file in replace
               else None), files)


def _etransfer_check_config():
    """Check the configuration is good for for e-transfer.

    Raises a CommandError if a problem is detected.
    """

    config = get_config()
    etransfermachine = config.get('etransfer', 'machine')
    etransferuser = config.get('etransfer', 'user')

    if pwd.getpwuid(os.getuid()).pw_name != etransferuser:
        raise CommandError('etransfer should only be run as {0}'.
                           format(etransferuser))
    if gethostname() != etransfermachine:
        raise CommandError('etransfer should only be run on {0}'.
                           format(etransfermachine))
