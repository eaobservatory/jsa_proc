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

import logging
import os.path
import re

from tools4caom2.tapclient import tapclient

from jsa_proc.error import JSAProcError
from jsa_proc.util import identifier_to_pattern

logger = logging.getLogger(__name__)

valid_fileid = re.compile('^[-_a-z0-9]+$')


class LogCompatException():
    """Exception class for exceptions raised by LogCompat."""


class LogCompat():
    """Compatability class to provide a tools4caom2.logger-like
    interface."""

    def file(self, message, loglevel=logging.INFO):
        logger.log(loglevel, message)

        if loglevel >= logging.ERROR:
            raise LogCompatException(message)

    def console(self, message, loglevel=logging.INFO, raise_exception=True):
        logger.log(loglevel, message)

        if raise_exception and loglevel >= logging.ERROR:
            raise LogCompatException(message)


class CADCTap():
    """Class for interaction with CADC's TAP service.
    """

    patterns = [
        (re.compile('^(scuba2_)[0-9]{5}(_[0-9]{8}t)[0-9]{6}$'),
         '{0}%{1}%'),
    ]

    def __init__(self):
        self.tap = tapclient(log=LogCompat())

        self.obsids_found = {}

    def obsids_by_pattern(self, pattern):
        """Retrueve list of obsids matching a given pattern.

        The pattern should be in lower case.
        """

        result = []

        table = self.tap.query(
            'SELECT lower(Observation.observationID) '
            'FROM caom2.Observation as Observation '
            'JOIN caom2.Plane as Plane ON Observation.obsID = Plane.obsID '
            'WHERE ( Observation.collection = \'JCMT\' '
            'AND lower(Observation.observationID) LIKE \'{0}\' '
            'AND Plane.calibrationLevel = 0 '
            ')'.format(pattern))

        if table is None:
            raise JSAProcError(
                'Failed TAP query for observation ID like {0}'.format(pattern))

        for (obsid,) in table:
            result.append(obsid)

        return result

    def check_obsids(self, obsids):
        """Test whether the given observation IDs can be found in CAOM-2.

        Returns a boolean list corresponding to the input list.
        """

        result = []

        for obsid in obsids:
            obsid = obsid.lower()

            pattern = self._obsid_pattern(obsid)

            if pattern in self.obsids_found:
                found = self.obsids_found[pattern]

            else:
                found = self.obsids_found[pattern] = \
                    self.obsids_by_pattern(pattern)

            result.append(obsid in found)

        return result

    def _obsid_pattern(self, obsid):
        """Return a search pattern to use for a given observation ID.

        This is to allow multiple observation IDs to be fetched at once
        for efficiency.
        """

        return identifier_to_pattern(obsid, self.patterns)

    def check_files(self, filenames):
        """Check whether the given files have been ingested into CAOM-2.

        Returns a boolean list corresponding to the input list.
        """

        uris = {}

        for filename in filenames:
            # CADC uses file IDs without extension in the JCMT archive.
            fileid = os.path.splitext(filename)[0]

            if not valid_fileid.match(fileid):
                raise JSAProcError('Invalid file ID {0}'.format(fileid))

            uris[filename] = 'ad:JCMT/{0}'.format(fileid)

        logger.debug(            'SELECT uri, COUNT(*) FROM caom2.Artifact '
            'WHERE uri IN ('
                + ', '.join(['\'{0}\''.format(x) for x in uris.values()])
                + ') GROUP BY uri')

        table = self.tap.query(
            'SELECT uri, COUNT(*) FROM caom2.Artifact '
            'WHERE uri IN ('
                + ', '.join(['\'{0}\''.format(x) for x in uris.values()])
                + ') GROUP BY uri')

        if table is None:
            raise JSAProcError(
                'Failed TAP query for files in CAOM-2')

        counts = {}

        for row in table:
            counts[row[0]] = row[1]


        result = []

        for filename in filenames:
            uri = uris[filename]

            if uri not in counts:
                result.append(False)
                continue

            count = counts[uri]

            if count == 0:
                result.append(False)

            elif count == 1:
                result.append(True)

            elif count > 1:
                logger.warning('Received unexpected artifact count')
                result.append(True)

            else:
                raise JSAProcError('Received unexpected artifact count')

        return result
