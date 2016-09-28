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

from jsa_proc.state import JSAProcState
from jsa_proc.jcmtobsinfo import ObsQueryDict
from jsa_proc.qa_state import JSAQAState
from jsa_proc.web.job_search import job_search
from jsa_proc.web.util import url_for, calculate_pagination


def prepare_job_list(db, page, **kwargs):
    # Generate query objects based on the parameters.
    (query, job_query) = job_search(**kwargs)

    # Identify number of jobs.
    count = db.find_jobs(count=True, **job_query)
    (number, page, pagination) = calculate_pagination(
        count, 24, page, 'job_list', query)

    # If no number in kwargs, add in default.
    if 'number' not in job_query:
        job_query['number'] = number

    jobs = []

    for job in db.find_jobs(outputs='%preview_64.png',
                            offset=(number * page),
                            **job_query):
        if job.outputs:
            preview = url_for('job_preview', job_id=job.id,
                              preview=job.outputs[0])
        else:
            preview = None
        jobs.append({
            'url': url_for('job_info', job_id=job.id),
            'qaurl': url_for('job_qa', job_id=job.id),
            'id': job.id,
            'state': job.state,
            'tag': job.tag,
            'location': job.location,
            'preview': preview,
            'qa_state': job.qa_state
        })

    return {
        'title': 'Job List',
        'jobs': jobs,
        'locations': ('JAC', 'CADC'),
        'states': JSAProcState.STATE_ALL,
        'qa_states': JSAQAState.STATE_ALL,
        'tasks': db.get_tasks(),
        'number': number,
        'pagination': pagination,
        'obsqueries': ObsQueryDict,
        'query': query,
        'mode': query['mode'],
        'count': count,
    }
