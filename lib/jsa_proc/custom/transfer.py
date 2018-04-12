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

import errno
import logging
import os
import sys

from docopt import docopt
import vos

from jsa_proc.files import get_md5sum

logger = logging.getLogger(__name__)

program_usage = """
{0} - Custom data transfer script

Usage:
    {0} [-v | -q] [-n] --transdir <transdir>

Options:
    --help, -h              Show usage information.
    --verbose, -v           Show debugging information.
    --quiet, -q             Do not show general logging information.
    --dry-run, -n           Skip actual transfer step.
    --transdir <transdir>   Specify target directory for final products.
"""


class CustomJobTransfer(object):
    def __init__(self, vos_base, program_name='custom_xfer'):
        """
        Constructor for custom transfer script class.
        """

        self.vos_base = vos_base
        self.program_name = program_name

    def run(self):
        """
        Main routine for transient transfer script.

        Handles command line arguments, checks that the transfer directory
        exists and reads a listing of its contents.
        """

        args = docopt(program_usage.format(self.program_name))

        # Configure logging.
        logging.basicConfig(
            level=logging.DEBUG if args['--verbose'] else (
                logging.WARNING if args['--quiet'] else logging.INFO))

        # Check that the transfer directory exists.
        logger.debug('Checking transfer directory exists')
        transdir = args['--transdir']

        if not os.path.isdir(transdir):
            logger.error('Specified transdir does not exist (or is a file)')
            sys.exit(1)

        # Get transfer directory listing.
        filenames = []
        logger.debug('Reading transfer directory listing')
        for file_ in os.listdir(transdir):
            if os.path.isfile(os.path.join(transdir, file_)):
                filenames.append(file_)
            else:
                logger.warning(
                    'Transfer directory contains non-file {0}'.format(file_))

        # Run the data transfer function.
        logger.debug('About to begin transfer')
        try:
            self.run_transfer(transdir, filenames, dry_run=args['--dry-run'])

        except:
            logger.exception('Exception during transfer')
            sys.exit(1)

        logger.debug('Transfer complete')

    def run_transfer(self, transdir, filenames, dry_run=False):
        """
        Function to launch data transfer.

        :param transdir: directory containing files to transfer.
        :param filenames: list of files to transfer.
        :param dry_run: specifies dry run mode.
        """

        logger.debug('Constructing VOS client')
        vos_client = vos.Client()
        if not vos_client.isdir(self.vos_base):
            raise Exception(
                'VOS base directory does not exist (or is file)')

        vos_cache = {}

        for file_ in (filenames):
            # Determine local file information.
            file_path = os.path.join(transdir, file_)
            file_md5 = get_md5sum(file_path)

            # Determine VO space information.
            vos_sub_dir = self.determine_vos_directory(transdir, file_)

            if vos_sub_dir is None:
                # This indicates that the file should be skipped.
                continue

            logger.debug(
                'VOS directory for {0}: {1}'.format(file_path, vos_sub_dir))

            vos_dir = '/'.join([self.vos_base, vos_sub_dir])
            vos_file = '/'.join([vos_dir, file_])

            # Get directory listing -- this creates the directory
            # if not in dry-run mode.
            if vos_dir in vos_cache:
                vos_dir_info = vos_cache[vos_dir]

            else:
                vos_dir_info = self.get_vos_directory_entries(
                    vos_client, vos_dir, dry_run=dry_run)

                vos_cache[vos_dir] = vos_dir_info

            # Perform storage, if file changed (and not in dry-run mode).
            vos_md5 = vos_dir_info.get(file_, ())

            if (vos_md5 != ()) and (vos_md5 == file_md5):
                logger.info(
                    'Skipped storing {0} as {1} [UNCHANGED]'.format(
                        file_path, vos_file))

            elif dry_run:
                logger.info(
                    'Skipped storing {0} as {1} [DRY-RUN]'.format(
                        file_path, vos_file))

            else:
                if vos_md5 != ():
                    logger.debug('Deleting existing file {0}'.format(vos_file))
                    vos_client.delete(vos_file)

                logger.info('Storing {0} as {1}'.format(file_path, vos_file))
                vos_client.copy(file_path, vos_file)

    def get_vos_directory_entries(self, vos_client, vos_dir, dry_run=False):
        """
        Get a list of a directory's content, or make it if it doesn't
        already exist.

        :return: a dictionary of MD5 sums by filename
        """

        result = {}

        try:
            logger.debug('Getting VO space node: %s', vos_dir)

            nodes = vos_client.get_node(
                vos_dir, limit=None, force=True).node_list

        except OSError as e:
            if e.errno == errno.ENOENT:
                if dry_run:
                    logger.info('DRY-RUN: would have made: %s', vos_dir)

                else:
                    self.make_vos_directory(vos_client, vos_dir)

            else:
                logger.exception('Error getting VO space node')
                raise

        else:
            for node in nodes:
                if node.isdir():
                    continue

                if 'MD5' in node.props:
                    result[node.name] = node.props['MD5']
                    continue

                elif 'length' in node.props:
                    # VO space seems to fail to return MD5 sums
                    # for empty files.
                    if node.props['length'] == '0':
                        logger.debug('Got no MD5 sum for length-0 file %s', node.name)
                        result[node.name] = 'd41d8cd98f00b204e9800998ecf8427e'
                        continue

                    else:
                        logger.warning('Unexpectedly got no MD5 sum for file %s', node.name)

                else:
                    logger.warning('Got no MD5 sum or length for file %s', node.name)

                result[node.name] = None

        return result

    def make_vos_directory(self, vos_client, vos_dir):
        """
        Recursively make a VOS directory, doing nothing if it already
        exists.
        """

        if vos_client.isdir(vos_dir):
            logger.debug('VOS directory {0} exists'.format(vos_dir))
        else:
            # Get parent directory and ensure it exists.
            dir_parts = vos_dir.rsplit('/', 1)
            if len(dir_parts) != 2:
                raise Exception('Cannot make top level VOS directory')

            self.make_vos_directory(vos_client, dir_parts[0])

            # Now create the requested directory.
            logger.info('Making VOS directory {0}'.format(vos_dir))
            vos_client.mkdir(vos_dir)

    def determine_vos_directory(self, transdir, filename):
        # This method must be overridden by subclasses.
        raise Exception('Custom transfer directory routine not defined')
