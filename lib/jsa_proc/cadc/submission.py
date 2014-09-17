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

from __future__ import absolute_import, division, print_function

import logging

from taco import Taco
from taco.error import TacoError

from jsa_proc.error import JSAProcError

logger = logging.getLogger(__name__)


class CADCDPSubmission():
    def __init__(self):
        """Construct CADC DP submission object.

        Connection to "starperl" and the CADC DP database
        is deferred until needed.
        """

        self.taco = None
        self.dbh = None

    def __del__(self):
        """Destroy CADC DP submission object.

        Any open CADC DP database connection is closed.
        """

        self._disconnect_cadcdp()

    def _connect_starperl(self):
        """Connect to "starperl" and load the necessary Perl
        modules."""

        if self.taco is not None:
            return

        logger.debug('Connecting to starperl')
        self.taco = Taco(lang='starperl')
        self.taco.import_module('JAC::Setup', 'omp', 'sybase')
        self.taco.import_module('JSA::CADC_DP',
                                'connect_to_cadcdp',
                                'create_recipe_instance',
                                'remove_recipe_instance',
                                'disconnect_from_cadcdp')

    def _connect_cadcdp(self):
        """Connect to the CADC DP database."""

        if self.dbh is not None:
            return

        self._connect_starperl()

        logger.debug('Connecting to the CADC DP database')
        self.dbh = self.taco.call_function('connect_to_cadcdp')

    def _disconnect_cadcdp(self):
        """Disconnect from the CADC DP database."""

        if self.dbh is None:
            return

        logger.debug('Disconnecting from the CADC DP database')
        self.taco.call_function('disconnect_from_cadcdp', self.dbh)
        self.dbh = None

    def remove_rc_instances(self, ids):
        """Remove recipe instances from the CADC DP database."""

        self._connect_cadcdp()

        logger.debug('Removing %i job(s) from the CADC DP database', len(ids))
        self.taco.call_function('remove_recipe_instance', self.dbh, *ids)
