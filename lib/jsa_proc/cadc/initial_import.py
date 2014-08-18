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

from jsa_proc.cadc.dpdb import CADCDP
from jsa_proc.cadc.dpstate import CADCDPState
from jsa_proc.cadc.param import parse_cadc_param
from jsa_proc.config import get_database
from jsa_proc.error import JSAProcError

logger = logging.getLogger(__name__)


def import_from_cadcdp(dry_run=False, db=None, cadc=None):
    """Perform an initial import of existing jobs from CADC.

    Returns true on success.
    """

    # Prepare counters.
    n_ok = 0
    n_already = 0
    n_err = 0

    # Connect to databases, unless we are given specific access
    # objects to use.
    if cadc is None:
        cadc = CADCDP()
    if db is None:
        db = get_database()

    # Retrieve set of tags we already have.
    tags = set((x.tag for x in db.find_jobs(location='CADC')))

    logger.info('Beginning import from CADC' +
                (' (DRY RUN)' if dry_run else ''))

    for job in cadc.get_recipe_info():
        logger.debug('Importing recipe instance %s', job.id)

        if job.tag in tags:
            logger.warning('Tag %s already present (recipe instance %s)',
                           job.tag, job.id)
            n_already += 1
            continue

        try:
            state = CADCDPState.jsaproc_state(job.state)

            info = parse_cadc_param(job.parameters)

            logger.debug('Getting input files for recipe instance %s', job.id)
            input = cadc.get_recipe_input_files(job.id)

            if not dry_run:
                logger.debug('Inserting job for recipe instance %s', job.id)
                job_id = db.add_job(tag=job.tag,
                                    location='CADC',
                                    mode=info.mode,
                                    parameters=info.parameters,
                                    input_file_names=input,
                                    foreign_id=job.id,
                                    state=state)

                logger.debug('Recipe instance %s inserted as job %i',
                             job.id, job_id)

            else:
                logger.debug('Skipping job insert due to dry run mode')

            n_ok += 1

        except JSAProcError:
            logger.exception('Failed to import recipe instance %s', job.id)

            n_err += 1

    logger.info('Done Importing from CADC')
    logger.info('Imported:        %i', n_ok)
    logger.info('Already present: %i', n_already)
    logger.info('Errors:          %i', n_err)

    return False if n_err else True
