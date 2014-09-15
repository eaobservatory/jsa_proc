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

from jsa_proc.db.db import Fuzzy, Range
from jsa_proc.jcmtobsinfo import ObsQueryDict
from jsa_proc.state import JSAProcState
from jsa_proc.qastate import JSAQAState
from jsa_proc.web.util import url_for, calculate_pagination


def prepare_job_list(db, location, state, task, number, page,
                     date_min, date_max, qastate,
                     sourcename, obsquerydict={}, state_choice='JSAProc'):
    if location == '':
        location = None
    if state == '' or state == []:
        state = None
    if task == '':
        task = None
    if date_min == '':
        date_min = None
    if date_max == '':
        date_max = None
    if qastate == '':
        qastate = None

    job_query = {
        'location': location,
        'state': state,
        'task': task,
        'qa_state': qastate,
    }

    # Add dictionary of obs table requirements to send to find jobs
    # to the job query.
    url_query = job_query.copy()
    obsquery = job_query['obsquery'] = {}
    url_query.update({
        'date_min': date_min,
        'date_max': date_max,
        'name': sourcename,
    })

    if (date_min is not None) or (date_max is not None):
        obsquery['utdate'] = Range(date_min, date_max)

    if sourcename:
        obsquery['sourcename'] = Fuzzy(sourcename)

    # Get the values based on the strings passed to this.
    for key, value in obsquerydict.items():
        if value:
            # Add the filtering information to the obsquery dictionary.
            obsquery.update(ObsQueryDict[key][value].where)

            # Add the parameter to the URL (for pagination links).
            url_query[key] = value

    (number, page, pagination) = calculate_pagination(
        db.find_jobs(count=True,  **job_query),
        number, 24, page, 'job_list', url_query)

    jobs = []

    for job in db.find_jobs(sort=True, outputs='%preview_64.png',
                            number=number, offset=(number * page),
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
            'qastate': job.qa_state
        })

    # If state is None, ensure we return a list like object.
    if state is None:
        state = []
    return {
        'title': 'Job List',
        'jobs': jobs,
        'locations': ('JAC', 'CADC'),
        'selected_location': location,
        'states': JSAProcState.STATE_ALL,
        'selected_state': state,
        'qastates': JSAQAState.STATE_ALL,
        'selected_qastate': qastate,
        'tasks': db.get_tasks(),
        'selected_task': task,
        'selected_number': number,
        'pagination': pagination,
        'selected_obsoptions': obsquerydict,
        'obsoptions': ObsQueryDict,
        'date_min': date_min,
        'date_max': date_max,
        'name': sourcename,
        'state_choice': state_choice,
    }
