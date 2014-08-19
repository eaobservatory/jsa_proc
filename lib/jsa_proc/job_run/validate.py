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

import re

from jsa_proc.error import NoRowsError
from jsa_proc.state import JSAProcState

valid_modes = ('obs', 'mode', 'project', 'public')
valid_file = re.compile('^[_a-z0-9]+$')


def validate_job(job_id, db):
    """Attempt to validate a job.

    Given the job ID of a job which should be in the UNKNOWN state,
    check the job and move it to the QUEUED state.  If the job
    did not validate, move it to the ERROR state.
    """

    job = db.get_job(id_=job_id)

    try:
        input = db.get_input_files(job_id)

    except NoRowsError:
        db.change_state(job_id,
                        JSAProcState.ERROR,
                        'Job failed validation: no input files found',
                        state_prev=JSAProcState.UNKNOWN)

        return

    try:
        # Check that the job has a mode string which jsawrapdr will
        # acccept.
        assert job.mode in valid_modes

        # Check that we have some input files.
        assert input

        # Ensure input filenames are plain names without path or
        # extension.
        for file in input:
            assert valid_file.match(file)

    except AssertionError:
        db.change_state(job_id,
                        JSAProcState.ERROR,
                        'Job failed validation',
                        state_prev=JSAProcState.UNKNOWN)

    else:
        db.change_state(job_id,
                        JSAProcState.QUEUED,
                        'Job passed validation',
                        state_prev=JSAProcState.UNKNOWN)
