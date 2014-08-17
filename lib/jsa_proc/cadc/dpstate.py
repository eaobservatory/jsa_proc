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

from ..state import JSAProcState
from jsa_proc.error import JSAProcError


class CADCDPState:
    """Class for handling CADC data processing states.
    """

    QUEUED = 'Q'
    DRM_QUEUED = 'D'
    RETRIEVE_STARTED = 'S'
    RETRIEVE_ENDED = 'N'
    CAPTURE_STARTED = 'C'
    CAPTURE_ENDED = 'P'
    COMPLETE = 'Y'
    ERROR = 'E'
    DO_AGAIN = 'R'
    UNDOABLE = 'U'

    STATE_QUEUED = set((QUEUED, DRM_QUEUED, DO_AGAIN))
    STATE_RUNNING = set((RETRIEVE_STARTED, RETRIEVE_ENDED,
                         CAPTURE_STARTED, CAPTURE_ENDED))
    STATE_COMPLETE = set((COMPLETE,))
    STATE_ERROR = set((ERROR, UNDOABLE))

    STATE_ALL = STATE_QUEUED | STATE_RUNNING | STATE_COMPLETE | STATE_ERROR

    _info = {
        QUEUED: 'Queued',
        DRM_QUEUED: 'DRM Queued',
        RETRIEVE_STARTED: 'Retrieve started',
        RETRIEVE_ENDED: 'Retrieve ended',
        CAPTURE_STARTED: 'Capture started',
        CAPTURE_ENDED: 'Capture ended',
        COMPLETE: 'Complete',
        ERROR: 'Error',
        DO_AGAIN: 'Do again',
        UNDOABLE: 'Un-doable',
    }

    @classmethod
    def jsaproc_state(cls, state):
        if state in cls.STATE_QUEUED:
            return JSAProcState.QUEUED

        elif state in cls.STATE_RUNNING:
            return JSAProcState.RUNNING

        elif state in cls.STATE_COMPLETE:
            return JSAProcState.COMPLETE

        elif state in cls.STATE_ERROR:
            return JSAProcState.ERROR

        else:
            raise JSAProcError('Unknown CADC DP state: ' + state)

    @classmethod
    def get_name(cls, state):
        try:
            return cls._info[state]
        except KeyError:
            raise JSAProcError('Unknown CADC state code {0}'.format(state))
