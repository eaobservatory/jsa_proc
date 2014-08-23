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

from __future__ import absolute_import, division

import glob
import os

from jsa_proc.error import NoRowsError
from jsa_proc.job_run.directories import get_log_dir
from jsa_proc.state import JSAProcState
from jsa_proc.web.util import url_for, HTTPNotFound


def prepare_job_info(db, job_id):
    try:
        job = db.get_job(job_id)
    except NoRowsError:
        raise HTTPNotFound()

    try:
        input_files = db.get_input_files(job_id)
    except NoRowsError:
        input_files = ['in', 'in']

    previews=[]
    try:
        output_files = db.get_output_files(job.id)

        for i in output_files:
            s = i.find('preview_1024.png')
            if s != -1:
                previews.append(i)

    except NoRowsError:
        output_files = []

    if previews:
        previews = [url_for('job_preview', job_id=job.id, preview=i) for i in previews]

    # Logged entries in the database (newest first).
    log = db.get_logs(job_id)
    log.reverse()


    # Get the log files on disk (if any)
    logdir = get_log_dir(job_id)
    orac_logfiles =  glob.glob(os.path.join(logdir, 'oracdr*.html'))
    orac_logfiles = [os.path.split(i)[1] for i in orac_logfiles]
    orac_logfiles =[ url_for('job_log_html', job_id=job.id, log=i) for i in orac_logfiles]

    wrapdr_logfiles = glob.glob(os.path.join(logdir, 'jsawrapdr*.log'))
    wrapdr_logfiles = [os.path.split(i)[1] for i in wrapdr_logfiles]
    wrapdr_logfiles =[ url_for('job_log_text', job_id=job.id, log=i) for i in wrapdr_logfiles]

    return {
        'title': 'Job {}'.format(job_id),
        'info': job,
        'log': log,
        'input_files': input_files,
        'output_files': output_files,
        'orac_logs': orac_logfiles,
        'wrapdr_logs': wrapdr_logfiles,
        'previews': previews,
        'states': JSAProcState.STATE_ALL,
    }
