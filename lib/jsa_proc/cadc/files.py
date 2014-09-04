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
import re

import requests
from requests.exceptions import HTTPError

from jsa_proc.error import JSAProcError
from jsa_proc.util import identifier_to_pattern


class CADCFiles():
    """Class to access the "jcmtInfo" service to determine whether files
    are at CADC or not.
    """

    jcmt_info_url = \
        'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/cadcbin/jcmtInfo'

    patterns = [
        (re.compile('^(s[48][abcd][0-9]{8}_[0-9]{5}_)[0-9]{4}$'),
         '{0}%'),
    ]

    def __init__(self):
        """Construct CADC file query object.

        This creates a cache of the patterns which have been queried
        and the resulting file lists.
        """

        self.found = {}

    def files_by_pattern(self, pattern):
        """Retrieve list of files matching a given pattern.
        """

        try:
            r = requests.get(self.jcmt_info_url, params={'file': pattern})
            r.raise_for_status()
            return latin_1_encode(r.text)[0].strip().split('\n')

        except HTTPError as e:
            raise JSAProcError('Error fetching CADC file list: ' + str(e))

    def check_files(self, files):
        """Check whether the given files are at CADC.

        Returns a boolean list of the same length as the input list,
        with true values corresponding only to the files which
        are present.
        """

        result = []
        patterns = {}

        for file in files:
            pattern = self._filename_pattern(file)

            if pattern in self.found:
                found = self.found[pattern]

            else:
                found = self.found[pattern] = self.files_by_pattern(pattern)

            result.append(file in found)

        return result

    def _filename_pattern(self, filename):
        """Convert a filename into a wildcarded pattern which should match
        that file.

        The pattern should match a reasonable number of files to query
        simultaneously, as this should be more efficient than
        querying a large number of similar files individually.
        """

        return identifier_to_pattern(filename, self.patterns)
