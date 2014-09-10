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
from jsa_proc.job_run.decorators import ErrorDecorator

valid_modes = ('obs', 'mode', 'project', 'public')
valid_file = re.compile('^[_a-z0-9]+$')

valid_preview_sizes = set([64, 256, 1024])
valid_preview_file = re.compile('^jcmt_[-_a-z0-9]+_preview_([0-9]{2,4})\.png$')
valid_product_file = re.compile('^jcmt[hs][-_a-z0-9]+\.fits$')


class ValidationError(Exception):
    pass


@ErrorDecorator
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


def validate_output(job_id, db):
    """Attempt to validate a job's output file list.

    On failure: set the job to the ERROR state and returns
    False.  Otherwise returns True.
    """

    try:
        try:
            files = db.get_output_files(job_id)

        except NoRowsError:
            raise ValidationError('no output files')

        products = []
        preview_sizes = set()

        for file in files:
            if valid_product_file.match(file):
                products.append(file)
                continue

            match = valid_preview_file.match(file)
            if match:
                preview_sizes.add(int(match.group(1)))
                continue

            raise ValidationError('invalid file output file name: {0}'.
                                  format(file))

        if not products:
            raise ValidationError('no product files captured')

        if not preview_sizes:
            raise ValidationError('no preview files captured')

        if preview_sizes != valid_preview_sizes:
            raise ValidationError('wrong preview sizes: ' +
                                  repr(preview_sizes))

    except ValidationError as e:
        db.change_state(job_id,
                        JSAProcState.ERROR,
                        'Job failed output: ' + e.message)

        return False

    return True
