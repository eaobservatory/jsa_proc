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

from datetime import datetime

from jsa_proc.action.unauthorized import IdentifiedProblem, UTC, \
    investigate_unauthorized_errors

from .compat import TestCase


class UnauthorizedTestCase(TestCase):
    def test_utc(self):
        # Test using the UTC object the same way as
        # investigate_unauthorized_errors does.
        now = datetime.now(UTC)
        self.assertIsInstance(now, datetime)
