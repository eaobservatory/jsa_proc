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

from tools4caom2.artifact_uri import extract_artifact_uri_filename
from tools4caom2.tapclient import tapclient_luskan

from jsa_proc.error import JSAProcError

logger = logging.getLogger(__name__)

LuskanFileInfo = namedtuple('LuskanFileInfo', ('filename', 'md5', 'size'))


class Luskan():
    """
    Class for interaction with CADC's luskan TAP service.
    """

    def __init__(self):
        self.tap = tapclient_luskan()

    def search_file(self, pattern, archive='JCMT', timeout=300, cookies=None):
        result = []

        table = self.tap.query(
            'SELECT uri, contentChecksum, contentLength '
            'FROM inventory.Artifact '
            'WHERE uri LIKE \'cadc:{}/{}\''.format(
                archive, pattern),
            timeout=timeout, cookies=cookies)

        if table is None:
            raise JSAProcError(
                'Failed luskan TAP query for files like {}'.format(pattern))

        for (uri, md5, size) in table:
            result.append(LuskanFileInfo(
                extract_artifact_uri_filename(uri, archive),
                extract_luskan_md5_sum(md5),
                size))

        return result


def extract_luskan_md5_sum(md5):
    if md5.startswith('md5:'):
        return md5[4:]

    raise JSAProcError(
        'MD5 "{}" not of expected format'.format(md5))
