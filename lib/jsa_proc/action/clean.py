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
import os.path
import shutil

from jsa_proc.admin.directories import get_input_dir
from jsa_proc.config import get_database
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


def clean_input(count=None, dry_run=False):
    """Delete input directories for processed jobs."""

    logger.debug('Beginning input clean')

    db = get_database()
    jobs = db.find_jobs(location='JAC',
                        state=[JSAProcState.INGESTION, JSAProcState.COMPLETE])

    n = 0
    for job in jobs:
        directory = get_input_dir(job.id)

        if not os.path.exists(directory):
            logger.debug('Input directory for job %i does not exist', job.id)
            continue

        logger.info('Removing input for job %i: %s', job.id, directory)

        if not dry_run:
            shutil.rmtree(directory)

        n += 1
        if (count is not None) and not (n < count):
            break

    logger.debug('Done cleaning input directories')
