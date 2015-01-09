# Copyright (C) 2015 Science and Technology Facilities Council.
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

from __future__ import absolute_import, division, print_function

from collections import namedtuple
import logging
import os
import tempfile
from time import sleep

from jsa_proc.action.datafile_handling import valid_hds
from jsa_proc.action.fitsverify import valid_fits
from jsa_proc.action.verify_png import valid_png
from jsa_proc.cadc.etransfer import etransfer_check_config
from jsa_proc.cadc.fetch import fetch_cadc_file_info, put_cadc_file
from jsa_proc.cadc.namecheck import check_file_name
from jsa_proc.config import get_config
from jsa_proc.error import CommandError, JSAProcError
from jsa_proc.files import get_md5sum

logger = logging.getLogger(__name__)

FileInfo = namedtuple('FileInfo', 'name stream')


class PTransferException(Exception):
    """Class for p-transfer exceptions.

    Objects in this class have a "reject_code" attribute corresponding
    to the p-transfer reject subdirectory the file should be moved into.
    """

    def __init__(self, code):
        Exception.__init__(
            self, 'file rejected for p-transfer ({0})'.format(code))
        self.reject_code = code


class PTransferFailure(Exception):
    """Class for p-transfer failures.

    Objects in this class represent a failure to transfer a file to
    CADC.  The transfer should be re-tried later.
    """

    pass


def ptransfer_poll(stream=None, dry_run=False):
    """Attempt to put files into the archive at CADC.

    This function is controlled by the configuration file
    entries etransfer.transdir and etransfer.maxfiles.
    It looks in the "new" and "replace" directories inside
    "transdir" for at most  "max_files" files.  The files
    are moved to a temporary processing directory and then
    either moved to a reject directory or deleted on
    completion.  In the event of failure to transfer, the files
    are put back in either the "new" or "replace" directory.

    The stream argument can be given to select only files in the
    "new" or "replace" directory.  It must be given in the
    dry_run case since then no "proc" directory is created.
    """

    if not dry_run:
        etransfer_check_config()

    config = get_config()

    trans_dir = config.get('etransfer', 'transdir')
    max_files = int(config.get('etransfer', 'max_files'))

    files = []

    # Select transfer streams.
    streams = ('new', 'replace')
    if stream is None:
        if dry_run:
            raise CommandError('Stream must be specified in dry run mode')
    else:
        if stream not in streams:
            raise CommandError('Unknown stream {0}'.format(stream))

        streams = (stream,)

    # Search for files to transfer.
    for stream in streams:
        for file in os.listdir(os.path.join(trans_dir, stream)):
            logger.debug('Found file %s (%s)', file, stream)
            files.append(FileInfo(file, stream))

    if not files:
        logger.info('No files found for p-transfer')
        return

    if dry_run:
        # Work in the stream directory.

        proc = files[:max_files]
        proc_dir = os.path.join(trans_dir, stream)

    else:
        # Create working directory.

        proc = []
        proc_dir = tempfile.mkdtemp(dir=os.path.join(trans_dir, 'proc'))
        logger.info('Working directory: %s', proc_dir)

        # Move some files into the working directory to prevent
        # multiple p-transfer processes trying to transfer them
        # simultaneously.
        for file in files:
            try:
                os.rename(
                    os.path.join(trans_dir, file.stream, file.name),
                    os.path.join(proc_dir, file.name))
                proc.append(file)
                logger.debug('Processing file %s', file.name)

            except:
                # Another process may have started processing the file,
                # so skip it.
                logger.debug('Cannot move file %s, skipping', file.name)

            # Did we get enough files already?
            if len(proc) >= max_files:
                break

    # Attempt to process all the files in our working directory.
    for file in proc:
        proc_file = os.path.join(proc_dir, file.name)

        try:
            # Check the file.
            md5sum = get_md5sum(proc_file)
            ptransfer_check(proc_dir, file.name, file.stream, md5sum)

            if dry_run:
                logger.info('Accepted file %s (DRY RUN)', file.name)

            else:
                # Transfer the file.
                ptransfer_put(proc_dir, file.name, md5sum)

                # Check it was transferred correctly.
                cadc_file_info = fetch_cadc_file_info(file.name)
                if cadc_file_info is None:
                    # File doesn't seem to be there?
                    logger.error('File transferred but has no info')
                    raise PTransferFailure('No file info')

                elif md5sum != cadc_file_info['content-md5']:
                    # File corrupted on transfer?  Put it back but in
                    # the replace directory for later re-transfer.
                    logger.error('File transferred but MD5 sum wrong')
                    file = file._replace(stream='replace')
                    raise PTransferFailure('MD5 sum wrong')

                # On success, delete the file.
                logger.info('Transferred file %s', file.name)
                os.unlink(proc_file)

        except PTransferException as e:
            # In the event of an error generated by one of the pre-transfer
            # checks, move the file into a reject directory.
            code = e.reject_code
            logger.error('Rejecting file %s (%s)', file.name, code)

            if not dry_run:
                reject_dir = os.path.join(trans_dir, 'reject', code)
                if not os.path.exists(reject_dir):
                    logger.debug('Making reject directory: %s', reject_dir)
                    os.makedirs(reject_dir)

                logger.debug('Moving file to: %s', reject_dir)
                os.rename(proc_file, os.path.join(reject_dir, file.name))

        except PTransferFailure:
            # In the event of failure to transfer, put the file back into
            # its original stream directory.
            logger.error('Failed to transfer file %s', file.name)

            if not dry_run:
                os.rename(
                    proc_file,
                    os.path.join(trans_dir, file.stream, file.name))

    # Finally clean up the processing directory.  It should have nothing
    # left in it by this point.
    if not dry_run:
        os.rmdir(proc_dir)


def ptransfer_check(proc_dir, filename, stream, md5sum):
    """Check if a file is suitable for transfer to CADC.

    Given the directory, file name and stream ("new" or "replace"), determine
    if a file is acceptable.  This function aims to replicate the checks which
    would have been made by the CADC e-transfer process.  Checking for
    decompressibility is not implemented as it is not expected that we will be
    transferring compressed files.

    Raises a PTransferException (including a rejection code) if a problem
    is detected.  No changes to the filesystem should be made, so this
    function should be safe to call in dry run mode.
    """

    proc_file = os.path.join(proc_dir, filename)

    # Check for permission to read the file.
    if not os.access(proc_file, os.R_OK):
        raise PTransferException('permission')

    # Check if file size is zero.
    if os.stat(proc_file).st_size == 0:
        raise PTransferException('empty')

    # Check extension and validity.
    (root, ext) = os.path.splitext(filename)
    if ext == '.sdf':
        if not valid_hds(proc_file):
            raise PTransferException('corrupt')

    elif ext == '.fits':
        if not valid_fits(proc_file):
            raise PTransferException('fitsverify')

    elif ext == '.png':
        if not valid_png(proc_file):
            raise PTransferException('corrupt')

    else:
        raise PTransferException('filetype')

    # Name-check.
    if not check_file_name(filename):
        raise PTransferException('name')

    # Check correct new/replacement stream.
    cadc_file_info = fetch_cadc_file_info(filename)
    if stream == 'new':
        if cadc_file_info is not None:
            raise PTransferException('not_new')

    elif stream == 'replace':
        if ((cadc_file_info is None)
                or (md5sum == cadc_file_info['content-md5'])):
            raise PTransferException('not_replace')

    else:
        raise Exception('unknown stream {0}'.format(stream))


def ptransfer_put(proc_dir, filename, md5sum):
    """Attempt to put the given file into the archive at CADC.

    Retries settings are given by the configuration file entries
    etransfer.max_tries and etransfer.retry_delay (in seconds).
    """

    config = get_config()

    max_retries = int(config.get('etransfer', 'max_tries'))
    retry_delay = int(config.get('etransfer', 'retry_delay'))

    for i in range(0, max_retries):
        try:
            put_cadc_file(filename, proc_dir)

            return

        except JSAProcError:
            logger.exception('Failed to put file {0} (try {1} of {2})'
                             .format(filename, i + 1, max_retries))

        sleep(retry_delay)

    raise PTransferFailure('Transfer failed')
