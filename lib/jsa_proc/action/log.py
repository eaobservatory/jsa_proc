# Copyright (C) 2017 East Asian Observatory.
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

import logging
import os
import re

from jsa_proc.admin.directories import get_log_dir
from jsa_proc.config import get_database
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


def search_log_files(pattern, filename_pattern, task, project=None):
    db = get_database()

    re_pattern = re.compile(pattern)
    re_filename = re.compile(filename_pattern)

    search_kwargs = {
        'task': task,
        'state': JSAProcState.COMPLETE,
    }

    if project is not None:
        search_kwargs['obsquery'] = {'project': project}

    jobs = [x.id for x in db.find_jobs(**search_kwargs)]

    for job_id in jobs:
        logger.debug('Checking log files for job %i', job_id)

        log_dir = get_log_dir(job_id)

        # Find the latest matching log by iterating through them in reverse
        # order and "breaking" after the first match.
        for filename in sorted(os.listdir(log_dir), reverse=True):
            if not re_filename.search(filename):
                continue

            logger.debug('Found log file for job %i: %s', job_id, filename)

            matched = False

            pathname = os.path.join(log_dir, filename)
            with open(pathname, 'r') as f:
                for line in f:
                    if re_pattern.search(line):
                        matched = True
                        break

            if matched:
                logger.info('Found match for job %i: %s', job_id, pathname)

            break
