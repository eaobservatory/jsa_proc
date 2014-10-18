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

from collections import namedtuple
import os
import re

from jsa_proc.admin.directories import get_log_dir
from jsa_proc.web.util import url_for

log_types = {
    'ORAC-DR': re.compile('oracdr.*\.html'),
    'PICARD': re.compile('picard.*\.html'),
    'JSA Wrap DR': re.compile('jsawrapdr.*\.log'),
    'Ingestion': re.compile('ingestion.*\.log'),
}

LogInfo = namedtuple('LogInfo', ['name', 'url'])


def get_log_files(job_id):
    """Get a dictionary of recognised logs for a given job.

    Scans the log directory of the given job looking for log
    files which match one of the patterns in the log_types
    dictionary.

    Returns a dictionary where the keys are the log types
    for which logs were found, and the values are lists of
    logs of that type.
    """

    log_files = {}

    for file in sorted(os.listdir(get_log_dir(job_id)), reverse=True):
        for (type_, pattern) in log_types.items():
            if pattern.match(file):
                if file.endswith('.html'):
                    url = url_for('job_log_html', job_id=job_id, log=file)
                else:
                    url = url_for('job_log_text', job_id=job_id, log=file)

                if type_ in log_files:
                    log_files[type_].append(LogInfo(file, url))
                else:
                    log_files[type_] = [LogInfo(file, url)]

    return log_files
