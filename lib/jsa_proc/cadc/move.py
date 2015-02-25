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

from jsa_proc.action.decorators import ErrorDecorator
from jsa_proc.config import get_database

logger = logging.getLogger(__name__)


def move_from_cadc(job_ids, dry_run=False):
    """Move a list of jobs from CADC to JAC.

    This sets up a database connection and then passes it to _move_job_from
    to move each job individually.
    """

    db = get_database()

    logger.info('Starting move of jobs from CADC to JAC')

    for job_id in job_ids:
        _move_job_from(job_id, db=db, dry_run=dry_run)

    logger.info('Done moving jobs')


@ErrorDecorator
def _move_job_from(job_id, db, dry_run):
    """Move a single job from CADC to JAC."""

    logger.debug('Moving job %i', job_id)
    job = db.get_job(id_=job_id)

    assert job.location == 'CADC'
    assert job.foreign_id is not None

    logger.debug('Changing location in jsa_proc database')
    if not dry_run:
        db.set_location(job_id, 'JAC', None,
                        message='Location changed to JAC from CADC, '
                                'recipe instance was: ' + job.foreign_id)
