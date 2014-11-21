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


def yes_or_no_question(question, default=False):
    while True:
        reply = raw_input('{0} ({1}): '.format(question,
                                               'Y/n' if default else 'y/N'))

        if reply == '':
            return default
        elif reply.lower() == 'y':
            return True
        elif reply.lower() == 'n':
            return False
