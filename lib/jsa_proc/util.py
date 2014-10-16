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

import signal

from jsa_proc.error import JSAProcError


def identifier_to_pattern(identifier, patterns):
    """Look for a suitable pattern for an identifier.

    Takes a list of (regexp, pattern) pairs.  Returns the
    pattern substituted with the regexp match groups
    for the first matching regexp.
    """

    for (regexp, pattern) in patterns:
        match = regexp.match(identifier)

        if match:
            return pattern.format(*match.groups())

    raise JSAProcError('Pattern for "{0}" not recognised'.format(identifier))


def restore_signals():
    """Restore signals which Python otherwise ignores.

    This is designed to be given as the preexec_fn keyword
    argument to subprocess calls.

    For more information about this issue, please see:
    http://bugs.python.org/issue1652"""

    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
