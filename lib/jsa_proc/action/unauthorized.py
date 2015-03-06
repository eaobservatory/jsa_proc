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

from jsa_proc.action.error_filter import JSAProcErrorFilter
from jsa_proc.action.util import yes_or_no_question
from jsa_proc.cadc.fetch import check_cadc_files
from jsa_proc.cadc.tap import CADCTap
from jsa_proc.config import get_database
from jsa_proc.omp.db import OMPDB
from jsa_proc.omp.state import OMPState
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


class IdentifiedProblem(Exception):
    def __init__(self, category, message):
        Exception.__init__(self, message)
        self.category = category


def investigate_unauthorized_errors(location, check_at_cadc=True):
    logger.debug('Starting to investigate unauthorized errors')

    logger.debug('Connecting to JSA processing database')
    db = get_database()

    logger.debug('Connecting to OMP/JCMT database')
    ompdb = OMPDB()

    logger.debug('Preparing CADC TAP object')
    caom2 = CADCTap()

    logger.debug('Fetching list of jobs in the error state')
    job_logs = db.find_errors_logs(location=location)

    logger.debug('Filtering for jobs with unauthorized errors')
    filter = JSAProcErrorFilter('unauthorized')
    filter(job_logs)

    now = datetime.now(UTC)
    category = {'unknown': []}
    job_info = {}

    for job_id in job_logs.keys():
        logger.debug('Checking job %i', job_id)

        # Python doesn't let us break inner loops, so use exceptions to
        # signal when the cause of the problem is identified.
        try:
            # Find the observation IDs and use it to determine whether the
            # job uses any observations which are not yet public.

            logger.debug('Fetching observation info')
            obs_info = db.get_obs_info(job_id)
            job_info[job_id] = {'obs': obs_info}

            if not obs_info:
                logger.warning('No observation info available for this job')
                continue

            obsids = set(latin_1_encode(x.obsid)[0] for x in obs_info)

            for obsid in obsids:
                logger.debug('Fetching COMMON info for %s', obsid)

                common = ompdb.get_common(obsid)
                release_date = ompdb.parse_datetime(common.release_date)

                # Keep the last release date inspected in the info dictionary
                # so that if it's the one that causes a problem, we see
                # it in the output.
                job_info[job_id]['release'] = release_date

                if release_date > now:
                    raise IdentifiedProblem(
                        'release',
                        'future release date ' +
                        release_date.strftime('%Y-%m-%d'))

                logger.debug('Fetching OMP obslog status for %s', obsid)
                status = ompdb.get_status(obsid)

                if status is not None:
                    logger.debug('Got obslog status: %i', status)

                    if status == OMPState.JUNK:
                        raise IdentifiedProblem('junk', 'observation is junk')

            # Check whether all of the files are at CADC.
            if check_at_cadc:
                logger.debug('Retrieving input file list')
                files = db.get_input_files(job_id)

                logger.debug('Checking for files at CADC')
                found = check_cadc_files(files)

                if not all(found):
                    raise IdentifiedProblem(
                        'missing',
                        'file {0} missing at CADC'.format(
                            files[found.index(False)]))

            # Check whether all the observations are in CAOM-2.
            if True:
                logger.debug('Checking for observatons in CAOM-2')
                obsid_list = list(obsids)
                found = caom2.check_obsids(obsid_list)

                if not all(found):
                    raise IdentifiedProblem(
                        'caom2',
                        'observation {0} missing from CAOM-2'.format(
                            obsid_list[found.index(False)]))

        except IdentifiedProblem as problem:
            logger.info('Job {0}: {1}'.format(job_id, problem.message))
            if problem.category in category:
                category[problem.category].append(job_id)
            else:
                category[problem.category] = [job_id]

        else:
            logger.info('Job {0}: problem unknown'.format(job_id))
            category['unknown'].append(job_id)

    # Now go through the categories and output information about them.
    for (cat, jobs) in category.items():
        if jobs:
            print('Category {0}: {1} job(s)'.format(cat, len(jobs)))

            if yes_or_no_question('Show detail?', False):
                for job in jobs:
                    info = job_info[job]
                    print(job,
                          info['obs'][0].instrument,
                          info['obs'][0].utdate,
                          info['obs'][0].obsnum,
                          info['obs'][0].project,
                          info['obs'][0].obstype,
                          info['obs'][0].scanmode,
                          info['release'])

                if yes_or_no_question('Resubmit jobs?', False):
                    for job in jobs:
                        db.change_state(
                            job, JSAProcState.QUEUED,
                            'Resubmitting job after unauthorized error',
                            state_prev=JSAProcState.ERROR)
