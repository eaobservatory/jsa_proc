# Copyright (C) 2014-2015 Science and Technology Facilities Council.
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

from collections import namedtuple
from datetime import datetime
from keyword import iskeyword

import Sybase
from pytz import UTC

from jsa_proc.config import get_config
from omp.siteconfig import get_omp_siteconfig

from jsa_proc.db.sybase import JSAProcSybaseLock
from jsa_proc.error import NoRowsError, ExcessRowsError


class OMPDB:
    """OMP and JCMT database access class.
    """

    CommonInfo = None

    OBS_JUNK = 4

    def __init__(self, write_access=None):
        """Construct new OMP and JCMT database object.

        Connects to the JAC Sybase server.

        Write access can either be None (the default), "omp"
        or "jcmt".  Read-only and OMP credentials come from
        the OMP siteconfig file.  JCMT database write permissions
        come from the JSA Proc configuration system.
        """

        # Connect using the "hdr_database" set of credentials, which is
        # the "staff" user (supposed to be read only) at the time of
        # writing, unless the write_access option is specified.
        if write_access is None:
            config = get_omp_siteconfig()
            credentials = 'hdr_database'
        elif write_access == 'omp':
            config = get_omp_siteconfig()
            credentials = 'database'
        elif write_access == 'jcmt':
            config = get_config()
            credentials = 'database_jcmt'
        else:
            raise JSAProcError('Unknown write_access request {0}'
                               .format(write_access))

        conn = Sybase.connect(
            config.get(credentials, 'server'),
            config.get(credentials, 'user'),
            config.get(credentials, 'password'),
            auto_commit=0)

        self.db = JSAProcSybaseLock(conn)

    def get_common(self, obsid):
        """Retrieve information for a given obsid from the COMMON table.
        """

        query = 'SELECT * FROM COMMON WHERE obsid=@o'
        args = {'@o': obsid}

        with self.db as c:
            c.execute('USE jcmt')
            c.execute(query, args)

            rows = c.fetchall()
            cols = c.description

        if not rows:
            raise NoRowsError('COMMON', query, args)

        elif len(rows) > 1:
            raise ExcessRowsError('COMMON', query, args)

        if self.CommonInfo is None:
            self.CommonInfo = namedtuple(
                'CommonInfo',
                ['{0}_'.format(x[0]) if iskeyword(x[0]) else x[0]
                 for x in cols])

        return self.CommonInfo(*rows[0])

    def get_status(self, obsid):
        """Retrieve the last comment status for a given obsid.

        Returns None if no status was found.
        """

        query = 'SELECT commentstatus FROM ompobslog ' \
                'WHERE obslogid = ' \
                '(SELECT MAX(obslogid) FROM ompobslog ' \
                'WHERE obsid=@o AND obsactive=1)'
        args = {'@o': obsid}

        with self.db as c:
            c.execute('USE omp')
            c.execute(query, args)

            rows = c.fetchall()

        if not rows:
            return None

        if len(rows) > 1:
            raise ExcessRowsError('omp', query, args)

        return rows[0][0]

    def parse_datetime(self, dt):
        """Parse a datetime value returned by Sybase and return a
        datetime object.
        """

        return UTC.localize(datetime.strptime(str(dt), '%b %d %Y %I:%M%p'))
