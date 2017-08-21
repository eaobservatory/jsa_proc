# Copyright (C) 2017 East Asian Observatory
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

from collections import namedtuple
import logging

from tools4caom2.tapclient import tapclient_ad

from jsa_proc.error import JSAProcError

logger = logging.getLogger(__name__)

ADFileInfo = namedtuple('ADFileInfo', ('id_', 'md5'))


class ADTap():
    """
    Class for interaction with CADC's AD TAP service.
    """

    def __init__(self):
        self.tap = tapclient_ad()

    def search_file(self, pattern, archive='JCMT', timeout=300):
        result = []

        table = self.tap.query(
            'SELECT fileID, contentMD5 '
            'FROM archive_files '
            'WHERE ('
            'archiveName = \'{}\' '
            'AND fileID LIKE \'{}\''
            ')'.format(archive, pattern),
            timeout=timeout)

        if table is None:
            raise JSAProcError(
                'Failed TAP query for AD files like {}'.format(pattern))

        for (id_, md5) in table:
            result.append(ADFileInfo(id_, md5))

        return result
