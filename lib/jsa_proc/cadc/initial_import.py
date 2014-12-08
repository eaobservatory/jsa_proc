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

from jsa_proc.admin.directories import get_output_dir
from jsa_proc.cadc.dpdb import CADCDP
from jsa_proc.cadc.dpstate import CADCDPState
from jsa_proc.cadc.param import parse_cadc_param
from jsa_proc.cadc.preview import fetch_cadc_previews
from jsa_proc.config import get_database
from jsa_proc.db.db import JSAProcFileInfo
from jsa_proc.error import JSAProcError

logger = logging.getLogger(__name__)


def import_from_cadcdp(dry_run=False, db=None, cadc=None, task='unknown',
                       fetch_previews=True, tag_pattern=None,
                       recipe_instance=None, prefix_rc_inst=False):
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

    for job in cadc.get_recipe_info(tag_pattern=tag_pattern,
                                    recipe_instance=recipe_instance):
        recipe_instance = job.id

        logger.debug('Importing recipe instance %s', recipe_instance)

        tag = job.tag

        if prefix_rc_inst:
            tag = '{0}-{1}'.format(recipe_instance, tag)

        if tag in tags:
            logger.warning('Tag %s already present (recipe instance %s)',
                           tag, recipe_instance)
            n_already += 1
            continue

        try:
            state = CADCDPState.jsaproc_state(job.state)

            info = parse_cadc_param(job.parameters)

            logger.debug('Getting input files for recipe instance %s',
                         recipe_instance)
            input = cadc.get_recipe_input_files(recipe_instance)

            # Some existing CADC recipe instances have repeated input file
            # names, which is an error in our system.  Therefore form a
            # set to eliminate the duplicates.
            input = set(input)

            # If the recipe instance is complete, also fetch the
            # list of output files.
            if job.state == CADCDPState.COMPLETE:
                logger.debug('Job is complete: fetching output files.')
                output = cadc.get_recipe_output_files(recipe_instance)

            else:
                output = None

            if not dry_run:
                logger.debug('Inserting job for recipe instance %s',
                             recipe_instance)
                job_id = db.add_job(tag=tag,
                                    location='CADC',
                                    mode=info.mode,
                                    parameters=info.parameters,
                                    task=task,
                                    input_file_names=input,
                                    foreign_id=recipe_instance,
                                    priority=job.priority,
                                    state=state)

                logger.debug('Recipe instance %s inserted as job %i',
                             recipe_instance, job_id)

                if output is not None:
                    logger.debug('Storing output file list.')
                    db.set_output_files(
                            job_id,
                            [JSAProcFileInfo(f.lower(), None) for f in output])

                    if fetch_previews:
                        logger.debug('Attempting to download preview files.')
                        fetch_cadc_previews(output, get_output_dir(job_id))

            else:
                logger.debug('Skipping job insert due to dry run mode')

            n_ok += 1

        except JSAProcError:
            logger.exception('Failed to import recipe instance %s',
                             recipe_instance)

            n_err += 1

    logger.info('Done Importing from CADC')
    logger.info('Imported:        %i', n_ok)
    logger.info('Already present: %i', n_already)
    logger.info('Errors:          %i', n_err)

    return False if n_err else True
