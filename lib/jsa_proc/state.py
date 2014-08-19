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

from collections import namedtuple

from jsa_proc.error import JSAProcError

# Information about each state:
#     name: Human-readable name of the state.
#     phase: General phase of processing (for display purposes).
#     active: True if the state represents a long-running process.
StateInfo = namedtuple('StateInfo', 'name phase active')


class JSAProcState:
    """Class for handling data processing states.
    """

    UNKNOWN = '?'
    QUEUED = 'Q'
    FETCHING = 'F'
    WAITING = 'W'
    RUNNING = 'S'
    PROCESSED = 'P'
    TRANSFERRING = 'X'
    INGESTION = 'I'
    COMPLETE = 'Y'
    ERROR = 'E'

    PHASE_QUEUE = 'Q'
    PHASE_FETCH = 'F'
    PHASE_RUN = 'S'
    PHASE_COMPLETE = 'Y'
    PHASE_ERROR = 'E'

    _info = {
        UNKNOWN:      StateInfo('Unknown',      PHASE_QUEUE,    False),
        QUEUED:       StateInfo('Queued',       PHASE_QUEUE,    False),
        FETCHING:     StateInfo('Fetching',     PHASE_FETCH,    True),
        WAITING:      StateInfo('Waiting',      PHASE_FETCH,    False),
        RUNNING:      StateInfo('Running',      PHASE_RUN,      True),
        PROCESSED:    StateInfo('Processed',    PHASE_RUN,      False),
        TRANSFERRING: StateInfo('Transferring', PHASE_RUN,      False),
        INGESTION:    StateInfo('Ingestion',    PHASE_RUN,      False),
        COMPLETE:     StateInfo('Complete',     PHASE_COMPLETE, False),
        ERROR:        StateInfo('Error',        PHASE_ERROR,    False),
    }

    STATE_ALL = set(_info.keys())

    @classmethod
    def get_name(cls, state):
        """Return the human-readable name of the state.

        Raises JSAProcError if the state does not exist.
        """

        try:
            return cls._info[state].name
        except KeyError:
            raise JSAProcError('Unknown state code {0}'.format(state))

    @classmethod
    def get_info(cls, state):
        """Return a StateInfo object describing the state.

        Raises JSAProcError if the state does not exist.
        """

        try:
            return cls._info[state]
        except KeyError:
            raise JSAProcError('Unknow state code {0}'.format(state))

    @classmethod
    def is_valid(cls, state):
        """Check whether a state is valid.

        Returns true if the state exists.
        """

        return state in cls._info
