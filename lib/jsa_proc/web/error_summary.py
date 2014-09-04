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

from jsa_proc.job_run.error_filter import JSAProcErrorFilter
from jsa_proc.state import JSAProcState
from jsa_proc.web.util import url_for


def prepare_error_summary(db, filtering=None):

    """
    Prepare a summary of all jobs in error state.


    options:

    filter parameter will be applied to the latest log
    message for each job, and will only return the job if
    it matches the filter

    General options:
        * network: return errors containing

    """

    locations = ['JAC', 'CADC']

    # Prepare a filter based on last error messgae for each ID.
    if filtering is None or filtering == '':
        error_filter = None
    else:
        error_filter = JSAProcErrorFilter(filtering)

    # Dictionary to hold output. Keys are location, items are ordered dict
    error_dict = OrderedDict()
    for l in locations:
        error_dict[l] = db.find_errors_logs(location=l)

        if error_filter is not None:
            error_filter(error_dict[l])

    return {
        'title': 'Errors in JSA Processing Jobs',
        'job_summary_dict': error_dict,
        'states': JSAProcState.STATE_ALL,
        'filtering': filtering,
        'filtering_options': JSAProcErrorFilter.filter_names,
    }
