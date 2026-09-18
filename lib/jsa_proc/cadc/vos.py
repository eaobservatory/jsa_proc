# Copyright (C) 2016-2026 East Asian Observatory.
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

from cadcutils.exceptions import NotFoundException

from jsa_proc.util import retry

logger = logging.getLogger(__name__)


class VOSClient(object):
    def transfer_file(
            self, vos_client,
            file_dir, file_name, file_md5, vos_dir,
            vos_cache=None,
            dry_run=False):
        file_path = os.path.join(file_dir, file_name)
        vos_file = '/'.join([vos_dir, file_name])

        # Get directory listing -- this creates the directory
        # if not in dry-run mode.
        if (vos_cache is not None) and (vos_dir in vos_cache):
            vos_dir_info = vos_cache[vos_dir]

        else:
            vos_dir_info = self.get_vos_directory_entries(
                vos_client, vos_dir, dry_run=dry_run)

            if vos_cache is not None:
                vos_cache[vos_dir] = vos_dir_info

        # Perform storage, if file changed (and not in dry-run mode).
        vos_md5 = vos_dir_info.get(file_name, ())

        if vos_md5 is None:
            vos_md5 = self.get_vos_file_md5(vos_client, vos_file)

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
                retry(lambda: vos_client.delete(vos_file))

            logger.info('Storing {0} as {1}'.format(file_path, vos_file))
            retry(lambda: vos_client.copy(file_path, vos_file))

    def get_vos_directory_entries(self, vos_client, vos_dir, dry_run=False):
        """
        Get a list of a directory's content, or make it if it doesn't
        already exist.

        :return: a dictionary of MD5 sums by filename
        """

        result = {}

        try:
            logger.debug('Getting VO space directory node: %s', vos_dir)

            nodes = retry(lambda: vos_client.get_node(
                vos_dir, limit=None, force=True),
                raise_=(NotFoundException,)).node_list

        # New error in the case of it not being there?
        except NotFoundException:
            # For now do the same as below...

            if dry_run:
                logger.info('DRY-RUN: would have made: %s', vos_dir)

            else:
                self.make_vos_directory(vos_client, vos_dir)

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
                        logger.debug('Unexpectedly got no MD5 sum for file %s', node.name)

                else:
                    logger.warning('Got no MD5 sum or length for file %s', node.name)

                result[node.name] = None

        return result

    def get_vos_file_md5(self, vos_client, vos_file):
        """
        Get MD5 sum of a file.
        """

        logger.debug('Getting VO space file node: %s', vos_file)

        node = retry(lambda: vos_client.get_node(
            vos_file, limit=None, force=True))

        if 'MD5' in node.props:
            return node.props['MD5']

        else:
            logger.debug('Unexpectedly got no MD5 sum for specific file %s', node.name)

        return None

    def make_vos_directory(self, vos_client, vos_dir):
        """
        Recursively make a VOS directory, doing nothing if it already
        exists.
        """

        if retry(lambda: vos_client.isdir(vos_dir)):
            logger.debug('VOS directory {0} exists'.format(vos_dir))

        else:
            # Get parent directory and ensure it exists.
            dir_parts = vos_dir.rsplit('/', 1)
            if len(dir_parts) != 2:
                raise Exception('Cannot make top level VOS directory')

            self.make_vos_directory(vos_client, dir_parts[0])

            # Now create the requested directory.
            logger.info('Making VOS directory {0}'.format(vos_dir))
            retry(lambda: vos_client.mkdir(vos_dir))
