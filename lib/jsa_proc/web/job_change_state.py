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
from jsa_proc.qa_state import JSAQAState
from jsa_proc.web.util import url_for, ErrorPage


def prepare_add_note(db, job_id, message, username):
    if message == '':
        raise ErrorPage('The note is empty!')

    db.add_note(job_id, message, username)


def prepare_change_state(db, job_ids, newstate, state_prev, message, username):
    if not JSAProcState.is_valid(newstate):
        raise ErrorPage('Unknown state %s' % (newstate))

    if not JSAProcState.is_valid(state_prev):
        raise ErrorPage('Unknown (previous) state %s' % (newstate))

    if message == '':
        raise ErrorPage('You must provide a message to change state!')

    for job_id in job_ids:
        db.change_state(job_id, newstate, message, state_prev=state_prev,
                        username=username)


def prepare_change_qa(db, job_ids, qa_state, message, username):
    if not JSAQAState.is_valid(qa_state):
        raise ErrorPage('Unknown state %s' % (newstate))

    if message == '' and qa_state in JSAQAState.STATE_IFFY:
        raise ErrorPage('You must provide a message to change QA state to ' +
                        ' or '.join((JSAQAState.get_name(x)
                                     for x in JSAQAState.STATE_IFFY)) + '.')

    for job_id in job_ids:
        db.add_qa_entry(job_id, qa_state, message, username)
