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

import logging
import re

from jsa_proc.action.decorators import ErrorDecorator
from jsa_proc.error import NoRowsError
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)

valid_modes = ('obs', 'night', 'project', 'public')
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
        # Ensure we can retrieve the list of input files.
        try:
            input = db.get_input_files(job_id)

        except NoRowsError:
            raise ValidationError('input file list could not be retrieved')

        # Check that the job has a mode string which jsawrapdr will
        # acccept.
        if job.mode not in valid_modes:
            raise ValidationError('invalid mode: {0}'.format(job.mode))

        # Check that we have some input files.
        if not input:
            raise ValidationError('input file list is empty')

        # Ensure input filenames are plain names without path or
        # extension.
        for file in input:
            if not valid_file.match(file):
                raise ValidationError('invalid input file: {0}'.format(file))

    except ValidationError as e:
        logger.error('Job %i failed validation: %s', job_id, e.message)
        db.change_state(job_id,
                        JSAProcState.ERROR,
                        'Job failed validation: ' + e.message,
                        state_prev=JSAProcState.UNKNOWN)

    else:
        logger.debug('Job %i passed validation', job_id)
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
                                  ', '.join(str(i) for i in preview_sizes))

    except ValidationError as e:
        logger.error('Job %i failed output validation: %s', job_id, e.message)
        db.change_state(job_id,
                        JSAProcState.ERROR,
                        'Job failed output: ' + e.message)

        return False

    logger.debug('Job %i passed output validation', job_id)
    return True
