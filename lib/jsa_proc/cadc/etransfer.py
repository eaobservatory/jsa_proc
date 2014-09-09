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

logger = logging.getLogger(__name__)


def etransfer_send_output(dry_run, job_id):
    logger.debug('Preparing to e-transfer output for job {0}'.format(job_id))

    config = get_config()
    scratchdir = config.get('etransfer', 'scratchdir')
    transdir = config.get('etransfer', 'scratchdir')

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

    logger.debug('Checking which files are already at CADC')
    present = ad.check_files(files)

    for (file, replace) in zip(files, present):
        if replace:
            logger.info('Placing file %s in "replace" directory', file)
        else:
            logger.info('Placing file %s in "new" directory', file)
