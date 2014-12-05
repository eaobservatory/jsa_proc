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

from __future__ import print_function

import logging

from jsa_proc.config import get_database
from jsa_proc.cadc.namecheck import check_file_name
from jsa_proc.error import NoRowsError
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


def namecheck_output(task, outfile):
    db = get_database()

    # Look for post-run or error states (as the error may be from namecheck!).
    states = JSAProcState.STATE_POST_RUN.copy()
    states.add(JSAProcState.ERROR)

    with open(outfile, 'w') as f:
        for job in db.find_jobs(task=task, location='JAC', state=states):
            job_id = job.id
            logger.info('Considering job %i', job_id)

            try:
                for file in db.get_output_files(job_id):
                    if check_file_name(file):
                        logger.debug('Job %i file %s OK', job_id, file)
                    else:
                        logger.warning('Job %i file %s FAILURE', job_id, file)
                        print(file, file=f)

            except NoRowsError:
                # Ignore jobs for which we have no output files.
                pass
