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
import os
import pwd
from socket import gethostname
import logging

from jsa_proc.cadc.files import CADCFiles
from jsa_proc.config import get_config, get_database
from jsa_proc.error import CommandError, NoRowsError
from jsa_proc.job_run.directories import get_output_dir
from jsa_proc.job_run.decorators import ErrorDecorator

logger = logging.getLogger(__name__)


def etransfer_send_output(job_id, dry_run):
    """High level e-transfer function for use from scripts.

    This function makes some basic checks and then launches
    the private function _etransfer_send under the control
    of the ErrorDecorator so that any subsequent errors
    are captured.
    """

    logger.debug('Preparing to e-transfer output for job {0}'.format(job_id))

    config = get_config()

    if not dry_run:
        # When not in dry run mode, check that etransfer is being
        # run on the correct machine by the correct user.
        etransfermachine = config.get('etransfer', 'machine')
        etransferuser = config.get('etransfer', 'user')

        # Method of obtaining the user name as recommended in the "os"
        # section of the Python standard library documentation.
        if pwd.getpwuid(os.getuid())[0] != etransferuser:
            raise CommandError('etransfer should only be run as {0}'.
                               format(etransferuser))
        if gethostname() != etransfermachine:
            raise CommandError('etransfer should only be run on {0}'.
                               format(etransfermachine))

    _etransfer_send(job_id, dry_run=dry_run)


@ErrorDecorator
def _etransfer_send(job_id, dry_run):
    """Private function to copy job output into the e-transfer
    directories.

    Runs under the ErrorDecorator so that errors are captured.
    """

    config = get_config()
    scratchdir = config.get('etransfer', 'scratchdir')
    transdir = config.get('etransfer', 'transdir')

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    logger.debug('Preparing CADC files object')
    ad = CADCFiles()

    logger.debug('Retrieving list of output files')
    try:
        files = db.get_output_files(job_id)

    except NoRowsError:
        message = 'No output files found for job {0}'.format(job_id)
        logger.error(message)
        raise CommandError(message)

    logger.debug('Checking that all files are present')
    outdir = get_output_dir(job_id)
    for file in files:
        if not os.path.exists(os.path.join(outdir, file)):
            message = 'File {0} not in directory {1}'.format(file, outdir)
            logger.error(message)
            raise CommandError(message)

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
            logger.info('Placing file %s in "replace" directory', file)
        else:
            logger.info('Placing file %s in "new" directory', file)


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
