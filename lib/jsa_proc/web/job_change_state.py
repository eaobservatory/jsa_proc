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
from jsa_proc.qastate import JSAQAState
from jsa_proc.web.util import url_for


def prepare_change_state(db, job_ids, newstate, message):

    if not JSAProcState.get_name(newstate):
        raise Exception('Unknown state %s' % (newstate))
    if message == '':
        raise Exception('You must provide a message to change state!')

    for job_id in job_ids:
        state_prev = db.get_job(id_=job_id).state

        db.change_state(job_id, newstate, message, state_prev=state_prev)

def prepare_change_qa(db, job_ids, qastate, message, username):

    if not JSAQAState.get_name(qastate):
        raise Exception('Unknown state %s' % (newstate))

    for job_id in job_ids:
        db.add_qa_entry(job_id, qastate, message, username)
