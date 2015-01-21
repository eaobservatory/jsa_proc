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

from jsa_proc.action.decorators import ErrorDecorator
from jsa_proc.action.util import yes_or_no_question
from jsa_proc.admin.directories import get_output_dir
from jsa_proc.cadc.fetch import fetch_cadc_file_info, check_cadc_files
from jsa_proc.config import get_config, get_database
from jsa_proc.error import CommandError, NoRowsError
from jsa_proc.files import get_space
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)

status_cache = None


class ETransferError(Exception):
    pass


def etransfer_poll_output(dry_run):
    """High level polling function to use from scripts."""

    logger.debug('Preparing to poll the e-transfer system for job output')

    # When not in dry run mode, check that etransfer is being
    # run on the correct machine by the correct user.
    if not dry_run:
        etransfer_check_config(any_user=True)

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    n_err = 0

    for job in db.find_jobs(location='JAC', state=JSAProcState.TRANSFERRING):
        job_id = job.id
        logger.debug('Checking state of job %i', job_id)

        logger.debug('Retrieving list of output files')
        try:
            file_info = db.get_output_files(job_id, with_info=True)
            files = [x.filename for x in file_info]

        except NoRowsError:
            logger.error('Did not find output files for job %i', job_id)
            n_err += 1
            continue

        try:
            logger.debug('Checking if files are in the e-transfer directories')
            etransfer_status = etransfer_file_status(files)
            if any(etransfer_status):
                rejection = []
                for (file, status) in zip(files, etransfer_status):
                    if status is not None:
                        (ok, dir) = status
                        if not ok:
                            logger.error('File {0} was rejected, reason: {1}'.
                                         format(file, dir))
                            rejection.append('{0} ({1})'.format(file, dir))

                if rejection:
                    raise ETransferError('files rejected: {0}'.format(
                                         ', '.join(rejection)))

                # Otherwise we found files in the "in progress" directories
                # so proceed to the next job.
                continue

            logger.debug('Checking if all files are at CADC')
            lost = []
            for info in file_info:
                cadc_file_info = fetch_cadc_file_info(info.filename)

                if cadc_file_info is None:
                    logger.error('Job %i file %s gone from e-transfer '
                                 'but not at CADC', job_id, info.filename)
                    lost.append(info.filename)

                if cadc_file_info['content-md5'] != info.md5:
                    logger.error('Job %i file %s gone from e-transfer '
                                 'but MD5 sum does not match',
                                 job_id, info.filename)
                    lost.append(info.filename)

            if lost:
                raise ETransferError('files lost or corrupt: {0}'.format(
                                     ', '.join(lost)))
            else:
                # All files present and with correct MD5 sums.
                logger.info('Job %i appears to have all files at CADC',
                            job_id)
                if not dry_run:
                    db.change_state(job_id, JSAProcState.INGESTION,
                                    'Output files finished e-transfer',
                                    state_prev=JSAProcState.TRANSFERRING)

        except ETransferError as e:
            logger.error('Job %i failed e-transfer: %s', job_id, e.message)
            if not dry_run:
                db.change_state(
                    job_id, JSAProcState.ERROR,
                    'Job failed e-transfer: {0}'.format(e.message),
                    state_prev=JSAProcState.TRANSFERRING)

    logger.debug('Done polling the e-transfer system')

    if n_err:
        raise CommandError('Errors were encountered polling e-transfer')


def etransfer_send_output(job_id, dry_run=False, force=False):
    """High level e-transfer function for use from scripts.

    This function makes some basic checks and then launches
    the private function _etransfer_send under the control
    of the ErrorDecorator so that any subsequent errors
    are captured.
    """

    logger.debug('Preparing to e-transfer output for job {0}'.format(job_id))

    # When not in dry run mode, check that etransfer is being
    # run on the correct machine by the correct user and with
    # sufficient available disk space.
    if not dry_run:
        etransfer_check_config()
        _etransfer_check_space()

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    if not force:
        job = db.get_job(id_=job_id)

        if job.state != JSAProcState.PROCESSED:
            message = 'Job {0} cannot be e-transferred as it is in ' \
                      'state {1}'.format(job_id, JSAProcState.get_name(job.state))
            logger.error(message)
            raise CommandError(message)

    _etransfer_send(job_id, dry_run=dry_run, db=db, force=force)

    logger.debug('Done adding output for job {0} to e-transfer'.format(job_id))


@ErrorDecorator
def _etransfer_send(job_id, dry_run, db, force):
    """Private function to copy job output into the e-transfer
    directories.

    Runs under the ErrorDecorator so that errors are captured.
    """

    config = get_config()
    scratchdir = config.get('etransfer', 'scratchdir')
    transdir = config.get('etransfer', 'transdir')
    group_id = grp.getgrnam(config.get('etransfer', 'group')).gr_gid

    logger.debug('Retrieving list of output files')
    try:
        file_info = db.get_output_files(job_id, with_info=True)
        files = [x.filename for x in file_info]

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
            if status is not None:
                (ok, dir) = status
                logger.error('File {0} already in e-transfer directory {1}'.
                             format(file, dir))
        raise CommandError('Some files are already in e-transfer directories')

    for info in file_info:
        file = info.filename
        cadc_file_info = fetch_cadc_file_info(file)

        if cadc_file_info is not None:
            # We need to check whether the file is not, in fact, different
            # from the current version, because in that case we are not
            # allowed to "replace" it.
            cadc_file_md5 = cadc_file_info['content-md5']

            if info.md5 == cadc_file_md5:
                logger.info('File %s in unchanged, skipping replacement',
                            file)
                continue

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
            state_prev=(None if force else JSAProcState.PROCESSED))


def etransfer_query_output(job_id):
    """Investigate the e-transfer status of the output of a job."""

    db = get_database()

    config = get_config()
    transdir = config.get('etransfer', 'transdir')

    files = db.get_output_files(job_id)

    problem_files = []

    print('{0:110} {1:5} {2:12} {3:5}'.format('File', 'ET', 'Directory', 'AD'))

    for file in zip(files, etransfer_file_status(files), check_cadc_files(files)):
        (filename, etransfer_status, ad_status) = file

        if etransfer_status is None:
            (ok, dir) = (None, '')
        else:
            (ok, dir) = etransfer_status

        print('{0:110} {1:5} {2:12} {3:5}'.format(
            filename, repr(ok), dir, repr(ad_status)))

        if ok is False:
            problem_files.append(os.path.join(transdir, 'reject', dir, filename))

    if problem_files:
        if yes_or_no_question('Delete rejected files from e-transfer directories?'):
            for file in problem_files:
                logger.debug('Deleting file %s', file)
                os.unlink(file)

            if yes_or_no_question('Re-try e-transfer?'):
                # Clear cache before attempting to e-transfer since we just
                # removed the files from the e-transfer directories.
                _etransfer_clear_cache()

                etransfer_send_output(job_id, dry_run=False, force=True)


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

    global status_cache

    if status_cache is None:
        status_cache = _etransfer_find_files()

    return [status_cache.get(file, None) for file in files]


def _etransfer_clear_cache():
    """Clears the e-transfer status cache."""

    global status_cache

    status_cache = None


def etransfer_check_config(any_user=False):
    """Check the configuration is good for for e-transfer.

    Raises a CommandError if a problem is detected.
    """

    config = get_config()
    etransfermachine = config.get('etransfer', 'machine')
    etransferuser = config.get('etransfer', 'user')

    if pwd.getpwuid(os.getuid()).pw_name != etransferuser and not any_user:
        raise CommandError('etransfer should only be run as {0}'.
                           format(etransferuser))
    if gethostname().partition('.')[0] != etransfermachine:
        raise CommandError('etransfer should only be run on {0}'.
                           format(etransfermachine))


def _etransfer_check_space():
    """Check that sufficient space is available for e-transfer.

    Raises a CommandError if a problem is detected.
    """

    config = get_config()
    required_space = float(config.get('disk_limit', 'etransfer_min_space'))
    etransfer_space = get_space(config.get('etransfer', 'transdir'))

    if etransfer_space < required_space:
        raise CommandError(
            'Insufficient disk space: {0} / {1} GiB required'.format(
                etransfer_space, required_space))


def _etransfer_find_files():
    """Find files in the e-transfer directories."""

    config = get_config()
    transdir = config.get('etransfer', 'transdir')

    filestatus = {}

    for (dirpath, dirnames, filenames) in os.walk(transdir):
        if not dirpath.startswith(transdir):
            raise Exception('os.walk returned dirpath outside transdir')

        dirs = dirpath[len(transdir) + 1:].split(os.path.sep)

        if not (filenames and dirs):
            continue

        if dirs[0] == 'reject':
            for file in filenames:
                filestatus[file] = (False, dirs[1])
        else:
            for file in filenames:
                filestatus[file] = (True, dirs[0])

    return filestatus
