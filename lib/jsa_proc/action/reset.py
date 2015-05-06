# Copyright (C) 2015 East Asian Observatory.
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

from jsa_proc.config import get_database
from jsa_proc.db.db import Range
from jsa_proc.error import CommandError
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


def reset_jobs(task, date_start, date_end, instrument=None,
               state=None, force=False, dry_run=False):
    """Change the state of the specified jobs back to "Unknown".

    If a state is specified, select only that state.
    Active jobs are skipped unless the force argument is set.
    """

    db = get_database()

    obsquery = {}

    if date_start is not None and date_end is not None:
        obsquery['utdate'] = Range(date_start, date_end)
    elif date_start is None and date_end is None:
        pass
    else:
        raise CommandError('only one of start and end date specified')

    if instrument is not None:
        obsquery['instrument'] = instrument

    if state is not None:
        state = JSAProcState.lookup_name(state)

    n_active = 0

    for job in db.find_jobs(location='JAC', task=task, obsquery=obsquery,
                            state=state):
        state_info = JSAProcState.get_info(job.state)

        # Check if the job is in an "active" state.
        if state_info.active and not force:
            logger.warning('Skipping active job %i (%s)',
                           job.id, state_info.name)
            n_active += 1
            continue

        logger.info('Resetting status of job %i (was %s)',
                    job.id, state_info.name)

        if not dry_run:
            db.change_state(job.id, JSAProcState.UNKNOWN,
                            'Resetting job', state_prev=job.state)

    if n_active:
        raise CommandError(
            'Could not reset {0} active jobs'.format(n_active))
