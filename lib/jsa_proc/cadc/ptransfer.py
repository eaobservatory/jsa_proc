# Copyright (C) 2015 Science and Technology Facilities Council.
# Copyright (C) 2016 East Asian Observatory.
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
from ConfigParser import SafeConfigParser
from datetime import datetime, timedelta
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

allowed_streams = ('new', 'replace')


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
    n_err = 0

    # Select transfer streams.
    streams = allowed_streams
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
        use_sub_dir = False
        stamp_file = None

    else:
        # Create working directory.

        proc = []
        proc_dir = tempfile.mkdtemp(prefix='proc',
                                    dir=os.path.join(trans_dir, 'proc'))
        logger.info('Working directory: %s', proc_dir)

        # Create stream-based subdirectories.
        use_sub_dir = True
        for stream in streams:
            os.mkdir(os.path.join(proc_dir, stream))

        # Write stamp file to allow automatic clean-up.
        stamp_file = os.path.join(proc_dir, 'ptransfer.ini')

        config = SafeConfigParser()
        config.add_section('ptransfer')
        config.set('ptransfer', 'pid', str(os.getpid()))
        config.set('ptransfer', 'start',
                   datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

        with open(stamp_file, 'wb') as f:
            config.write(f)

        # Move some files into the working directory to prevent
        # multiple p-transfer processes trying to transfer them
        # simultaneously.
        for file in files:
            try:
                os.rename(
                    os.path.join(trans_dir, file.stream, file.name),
                    os.path.join(proc_dir, file.stream, file.name))
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
        # Determine path to the directory containing the file and the
        # file itself.
        if use_sub_dir:
            proc_sub_dir = os.path.join(proc_dir, file.stream)
        else:
            proc_sub_dir = proc_dir

        proc_file = os.path.join(proc_sub_dir, file.name)

        try:
            # Check the file.
            md5sum = get_md5sum(proc_file)
            ad_stream = ptransfer_check(
                proc_sub_dir, file.name, file.stream, md5sum)

            if dry_run:
                logger.info('Accepted file %s (%s) (DRY RUN)',
                            file.name, ad_stream)

            else:
                # Transfer the file.
                ptransfer_put(proc_sub_dir, file.name, ad_stream, md5sum)

                # Check it was transferred correctly.
                try:
                    cadc_file_info = fetch_cadc_file_info(file.name)
                except JSAProcError:
                    raise PTransferFailure('Unable to check CADC file info')

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
                logger.info('Transferred file %s (%s)', file.name, ad_stream)
                os.unlink(proc_file)

        except PTransferException as e:
            # In the event of an error generated by one of the pre-transfer
            # checks, move the file into a reject directory.
            n_err += 1
            code = e.reject_code
            logger.error('Rejecting file %s (%s)', file.name, code)

            if not dry_run:
                reject_dir = os.path.join(trans_dir, 'reject', code)
                if not os.path.exists(reject_dir):
                    logger.debug('Making reject directory: %s', reject_dir)
                    os.makedirs(reject_dir)

                logger.debug('Moving file to: %s', reject_dir)
                os.rename(proc_file, os.path.join(reject_dir, file.name))

        except PTransferFailure as e:
            # In the event of failure to transfer, put the file back into
            # its original stream directory.
            n_err += 1
            logger.error(
                'Failed to transfer file %s (%s)', file.name, e.message)

            if not dry_run:
                os.rename(
                    proc_file,
                    os.path.join(trans_dir, file.stream, file.name))

        except:
            # Catch any other exception and also put the file back.
            n_err += 1
            logger.exception('Error while transferring file %s', file.name)

            if not dry_run:
                os.rename(
                    proc_file,
                    os.path.join(trans_dir, file.stream, file.name))

    # Finally clean up the processing directory.  It should have nothing
    # left in it by this point other than the stream subdirectories and
    # stamp file.
    if not dry_run:
        os.unlink(stamp_file)

        for stream in streams:
            os.rmdir(os.path.join(proc_dir, stream))

        os.rmdir(proc_dir)

    # If errors occurred, exit with bad status.
    if n_err:
        raise CommandError('Errors occurred during p-transfer poll'
                           ' ({0} error(s))'.format(n_err))


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

    Returns the CADC AD stream to be used for the file.  This is determined by
    a mapping from namecheck section to stream name in the configuration file
    entry etransfer.ad_stream.
    """

    config = get_config()

    ad_streams = dict(map(
        lambda x: x.split(':'),
        config.get('etransfer', 'ad_stream').split(' ')))

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
    namecheck_section = check_file_name(filename, True)
    if namecheck_section is None:
        raise PTransferException('name')
    if namecheck_section in ad_streams:
        ad_stream = ad_streams[namecheck_section]
    else:
        raise PTransferException('stream')

    # Check correct new/replacement stream.
    try:
        cadc_file_info = fetch_cadc_file_info(filename)
    except JSAProcError:
        raise PTransferFailure('Unable to check CADC file info')

    if stream == 'new':
        if cadc_file_info is not None:
            raise PTransferException('not_new')

    elif stream == 'replace':
        if cadc_file_info is None:
            raise PTransferException('not_replace')
        elif md5sum == cadc_file_info['content-md5']:
            raise PTransferException('unchanged')

    else:
        raise Exception('unknown stream {0}'.format(stream))

    return ad_stream


def ptransfer_put(proc_dir, filename, ad_stream, md5sum):
    """Attempt to put the given file into the archive at CADC.

    Retries settings are given by the configuration file entries
    etransfer.max_tries and etransfer.retry_delay (in seconds).
    """

    config = get_config()

    max_retries = int(config.get('etransfer', 'max_tries'))
    retry_delay = int(config.get('etransfer', 'retry_delay'))

    for i in range(0, max_retries):
        try:
            put_cadc_file(filename, proc_dir, ad_stream)

            return

        except JSAProcError:
            logger.exception('Failed to put file {0} (try {1} of {2})'
                             .format(filename, i + 1, max_retries))

        sleep(retry_delay)

    raise PTransferFailure('Transfer failed')


def ptransfer_clean_up(dry_run=False):
    """Attempt to clean up orphaned p-tranfer "proc" directories.
    """

    if not dry_run:
        etransfer_check_config()

    config = get_config()

    trans_dir = config.get('etransfer', 'transdir')

    # Determine latest start time for which we will consider cleaning up
    # a proc directory.
    start_limit = datetime.utcnow() - timedelta(
        minutes=int(config.get('etransfer', 'cleanup_minutes')))

    start_limit_hard = datetime.utcnow() - timedelta(
        minutes=int(config.get('etransfer', 'cleanup_hard_minutes')))

    # Look for proc directories.
    proc_base_dir = os.path.join(trans_dir, 'proc')

    for dir_ in os.listdir(proc_base_dir):
        # Consider only directories with the expected name prefix.
        proc_dir = os.path.join(proc_base_dir, dir_)
        if not (dir_.startswith('proc') and os.path.isdir(proc_dir)):
            continue

        logger.debug('Directory %s found', dir_)

        # Check for and read the stamp file.
        stamp_file = os.path.join(proc_dir, 'ptransfer.ini')
        config = SafeConfigParser()
        config_files_read = config.read(stamp_file)
        if not config_files_read:
            logger.debug('Directory %s has no stamp file', dir_)
            continue

        # Check if the transfer started too recently to consider.
        start = datetime.strptime(config.get('ptransfer', 'start'),
                                  '%Y-%m-%d %H:%M:%S')

        if start > start_limit:
            logger.debug('Directory %s is too recent to clean up', dir_)
            continue

        # Check if the transfer process is still running (by PID).
        pid = int(config.get('ptransfer', 'pid'))
        is_running = True
        try:
            os.kill(pid, 0)
        except OSError:
            is_running = False

        if is_running:
            logger.debug('Directory %s corresponds to running process (%i)',
                         dir_, pid)

            if start > start_limit_hard:
                continue

            logger.debug(
                'Directory %s is older than hard limit, killing process %i',
                dir_, pid)

            if not dry_run:
                try:
                    os.kill(pid, 15)
                except OSError:
                    pass

                # Check whether the process did exit.
                sleep(5)
                try:
                    os.kill(pid, 0)
                except OSError:
                    is_running = False

                if is_running:
                    logger.warning('Could not kill process %i', pid)
                    continue

        # All checks are complete: move the files back to their initial
        # stream directories.
        n_moved = 0
        n_skipped = 0

        for stream in allowed_streams:
            stream_has_skipped_files = False

            proc_stream_dir = os.path.join(proc_dir, stream)
            if not os.path.exists(proc_stream_dir):
                continue

            orig_stream_dir = os.path.join(trans_dir, stream)
            if (not os.path.exists(orig_stream_dir)) and (not dry_run):
                os.mkdir(orig_stream_dir)

            for file_ in os.listdir(proc_stream_dir):
                logger.debug('Directory %s has file %s (%s)',
                             dir_, file_, stream)

                proc_file = os.path.join(proc_stream_dir, file_)
                orig_file = os.path.join(orig_stream_dir, file_)

                if os.path.exists(orig_file):
                    logger.warning(
                        'File %s present in %s and %s directories',
                        file_, dir_, stream)
                    n_skipped += 1
                    stream_has_skipped_files = True

                else:
                    if dry_run:
                        logger.info('Would move %s %s back to %s (DRY RUN)',
                                    dir_, file_, stream)
                    else:
                        os.rename(proc_file, orig_file)
                    n_moved += 1

            if (not stream_has_skipped_files) and (not dry_run):
                os.rmdir(proc_stream_dir)

        logger.info(
            'Proc directory %s: %i file(s) cleaned up, %i skipped',
            dir_, n_moved, n_skipped)

        # If we didn't skip any files, remove the stamp file and now-empty
        # proc directory.  (Unless in dry run mode.)
        if n_skipped or dry_run:
            continue

        os.unlink(stamp_file)
        os.rmdir(proc_dir)
