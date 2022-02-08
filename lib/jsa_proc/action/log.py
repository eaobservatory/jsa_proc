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


def search_log_files(
        pattern, filename_pattern, task,
        project=None, state=None, after_context=None,
        notes=False):
    db = get_database()

    re_pattern = re.compile(pattern)
    re_filename = re.compile(filename_pattern)

    if state is None:
        state = JSAProcState.COMPLETE
    else:
        state = JSAProcState.lookup_name(state)

    if after_context is None:
        after_context = 0

    search_kwargs = {
        'task': task,
        'state': state,
    }

    if project is not None:
        search_kwargs['obsquery'] = {'project': project}

    jobs = [x.id for x in db.find_jobs(**search_kwargs)]

    for job_id in jobs:
        logger.debug('Checking log files for job %i', job_id)

        log_dir = get_log_dir(job_id)

        # Find the latest matching log by iterating through them in reverse
        # order and "breaking" after the first match.
        try:
            filenames = sorted(os.listdir(log_dir), reverse=True)
        except OSError:
            logger.debug('No log directory for job %i', job_id)
            continue

        for filename in filenames:
            if not re_filename.search(filename):
                continue

            logger.debug('Found log file for job %i: %s', job_id, filename)

            matched = 0
            matched_lines = []

            pathname = os.path.join(log_dir, filename)
            with open(pathname, 'r') as f:
                for line in f:
                    if matched or re_pattern.search(line):
                        matched += 1
                        matched_lines.append(line.rstrip())

                    if matched > after_context:
                        break

            if matched:
                logger.info(
                    'Found match for job %i: %s', job_id, matched_lines[0])

                for matched_line in matched_lines[1:]:
                    logger.info(
                        '...    continuation %i: %s', job_id, matched_line)

                if notes:
                    for note in db.get_notes(job_id):
                        logger.info('    Note: %s', note.message)

            break
