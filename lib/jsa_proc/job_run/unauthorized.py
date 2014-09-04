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

from __future__ import print_function, division, absolute_import

from codecs import latin_1_encode
from datetime import datetime
import logging

from pytz import UTC

from jsa_proc.config import get_database
from jsa_proc.omp.db import OMPDB
from jsa_proc.job_run.error_filter import JSAProcErrorFilter

logger = logging.getLogger(__name__)


class IdentifiedProblem(Exception):
    def __init__(self, category, message):
        Exception.__init__(self, message)
        self.category = category


def investigate_unauthorized_errors(location):
    logger.debug('Starting to investigate unauthorized errors')

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    logger.debug('Connecting to OMP/JCMT database')
    ompdb = OMPDB()

    logger.debug('Fetching list of jobs in the error state')
    job_logs = db.find_errors_logs(location=location)

    logger.debug('Filtering for jobs with unauthorized errors')
    filter = JSAProcErrorFilter('unauthorized')
    filter(job_logs)

    now = datetime.now(UTC)
    category = {
        'release': [],
        'unknown': [],
    }

    for job_id in job_logs.keys():
        logger.debug('Checking job %i', job_id)

        # Python doesn't let us break inner loops, so use exceptions to
        # signal when the cause of the problem is identified.
        try:
            # Find the observation IDs and use it to determine whether the
            # job uses any observations which are not yet public.

            logger.debug('Fetching observation info')
            obs_info = db.get_obs_info(job_id)

            if not obs_info:
                logger.warning('No observation info available for this job')
                continue

            obsids = set(latin_1_encode(x.obsid)[0] for x in obs_info)

            for obsid in obsids:
                logger.debug('Fetching COMMON info for %s', obsid)

                common = ompdb.get_common(obsid)
                release_date = ompdb.parse_datetime(common.release_date)

                if release_date > now:
                    raise IdentifiedProblem(
                        'release',
                        'future release date ' +
                        release_date.strftime('%Y-%m-%d'))

        except IdentifiedProblem as problem:
            logger.info('Job {0}: {1}'.format(job_id, problem.message))
            category[problem.category].append(job_id)

        else:
            logger.info('Job {0}: problem unknown'.format(job_id))
            category['unknown'].append(job_id)

    # Now go through the categories and output information about them.
    for (cat, jobs) in category.items():
        if jobs:
            print('Category {0}: {1} job(s)'.format(cat, len(jobs)))
