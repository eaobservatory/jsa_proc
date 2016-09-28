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

from __future__ import absolute_import, division, print_function

from jsa_proc.db.db import Fuzzy, Range
from jsa_proc.jcmtobsinfo import ObsQueryDict


def job_search(location, state, task,
               date_min, date_max, qa_state,
               sourcename, obsnum, project,
               mode, number, tau_min, tau_max, **kwargs):

    # If number is None, reset to default
    if not number or number is None:
        number = 24
    # check on keyword tiles
    tiles = kwargs.get('tiles', None)

    # Initialize entries which the job and URL queries have in common.
    job_query = {
        'location': location,
        'task': task,
        'qa_state': qa_state,
        'number': number,
        'tiles': tiles,
    }

    # Initialize the URL query / template context with a copy of the common
    # entries.
    query = job_query.copy()

    # Add non-common elements to the URL query.
    query.update({
        'mode': mode,
        'date_min': date_min,
        'date_max': date_max,
        'sourcename': sourcename,
        'obsnum': obsnum,
        'project': project,
        'state': state,
        'tau_min': tau_min,
        'tau_max': tau_max,
    })

    # Add non-common elements to job query:
    if state:
        # State should only be specified if it is not an empty list.
        job_query['state'] = state

    # Add dictionary of obs table requirements to send to find jobs
    # to the job query.
    obsquery = job_query['obsquery'] = {}

    if (date_min is not None) or (date_max is not None):
        obsquery['utdate'] = Range(date_min, date_max)

    if (tau_min is not None) or (tau_max is not None):
        obsquery['tau'] = Range(tau_min, tau_max)

    if sourcename:
        obsquery['sourcename'] = Fuzzy(sourcename)

    if obsnum:
        obsquery['obsnum'] = obsnum

    if project:
        obsquery['project'] = project

    # Get the values based on the strings passed to this.
    for key, info in ObsQueryDict.items():
        value = kwargs[key]
        if value is not None:
            # Add the filtering information to the obsquery dictionary.
            obsquery.update(info[value].where)

            # Add the parameter to the URL (for pagination links).
            query[key] = value

        else:
            query[key] = None

    # Enable sorting (ignored in count mode).
    job_query['sort'] = True

    return (query, job_query)
