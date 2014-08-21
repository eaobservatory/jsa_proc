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

from jsa_proc.error import NoRowsError
from jsa_proc.web.util import url_for, HTTPNotFound


def prepare_job_info(db, job_id):
    try:
        job = db.get_job(job_id)
    except NoRowsError:
        raise HTTPNotFound()

    return {
        'title': 'Job {}'.format(job_id),
        'info': {
            'state': job.state,
        },
    }
