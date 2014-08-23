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

from collections import OrderedDict

from jsa_proc.state import JSAProcState
from jsa_proc.web.util import url_for


def prepare_job_summary(db):

    """
    Prepare a summary of jobs.

    Needs to get:

           * Total jobs in db.
           * Number of jobs in JAC and CADC
           * Number of jobs in each state

    """

    states = JSAProcState.STATE_ALL
    state_names = [JSAProcState.get_name(i) for i in states]
    state_dict = dict(zip(states, state_names))
    locations = ['JAC', 'CADC']

    job_summary_dict = OrderedDict()
    for s in states:
        job_summary_dict[s] = OrderedDict()
        for l in locations:
            job_summary_dict[s][l] = db.find_jobs(location=l, state=s, count=True)


    total_count = sum([int(c) for j in job_summary_dict.values() for c in j.values()])

    return {
        'title': 'Summary of JSA Processing Jobs',
        'total_count': total_count,
        'job_summary_dict': job_summary_dict,
        'state_dict':state_dict,
        'locations':locations,
    }


