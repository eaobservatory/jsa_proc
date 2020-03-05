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

from collections import namedtuple, OrderedDict
import os

from jsa_proc.admin.directories import get_output_dir
from jsa_proc.error import NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.qa_state import JSAQAState
from jsa_proc.web.component.files import make_output_file_list
from jsa_proc.web.job_search import job_search
from jsa_proc.web.log_files import get_log_files
from jsa_proc.web.util import Pagination, url_for, HTTPNotFound


def prepare_job_qa_info(db, job_id, query):
    # Fetch job and qa information from the database.
    try:
        job = db.get_job(job_id)
    except NoRowsError:
        raise HTTPNotFound()

    # Convert the information to a dictionary so that we can augment it.
    info = job._asdict()
    if info['foreign_id'] is not None:
        if info['location'] == 'CADC':
            info['foreign_url'] = \
                'http://beta.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/' \
                'dp/recipe/{0}'.format(info['foreign_id'])
        else:
            info['foreign_url'] = None

    try:
        input_files = db.get_input_files(job_id)
    except NoRowsError:
        input_files = None

        # Try to get parent jobs (if any).
    # Dictionary with parent as key and filter as item.
    try:
        parents = db.get_parents(job_id)
        parents = dict(parents)
        parent_obs = OrderedDict()
        pjobs = list(parents.keys())
        pjobs.sort()
        for i in pjobs:
            obsinfo = db.get_obs_info(i)
            if obsinfo != []:
                obsinfo = [o._asdict() for o in db.get_obs_info(i)]
                qa_state = db.get_job(i).qa_state
                for o in obsinfo:
                    o['qa_state'] = qa_state

                parent_obs[i] = obsinfo
        if parent_obs.keys() == []:
            parent_obs = None
    except NoRowsError:
        parents = None
        parent_obs = None

    # See if there are any child jobs.
    try:
        children = db.get_children(job_id)
    except NoRowsError:
        children = None

    (output_files, previews1024, _) = \
        make_output_file_list(db, job.id)

    obs_info = db.get_obs_info(job.id)

    if obs_info:
        obs_info = [o._asdict() for o in obs_info]

    else:
        obs_info = None

    # Get the log files on disk (if any)
    log_files = get_log_files(job_id)

    # QA log (f any)
    qalog = db.get_qas(job_id)
    qalog.reverse()

    # If we know what the user's job query was (from the session information)
    # then set up pagination based on the previous and next job identifiers.
    if query is not None:
        (url_query, job_query) = job_search(**query)

        # prev_next query should not  contain kwarg 'number'.
        pnquery = job_query.copy()
        if 'number' in pnquery:
            pnquery.pop('number')

        (prev, next) = db.job_prev_next(job_id, **pnquery)
        count = db.find_jobs(count=True, **job_query)
        pagination = Pagination(
            None,
            None if prev is None else url_for('job_qa', job_id=prev),
            None if next is None else url_for('job_qa', job_id=next),
            None,
            url_for('job_list', **url_query),
            count,)
    else:
        pagination = None

    # In the case of task='*-cat' and there are no output preview
    # images, show the preview image from the 1st parent job.
    if '-cat' in info['task'] and previews1024 == []:
        (_, previews1024, _) = make_output_file_list(
            db, parents.keys()[0])
        nopreview = True
    else:
        nopreview = False

    # Get parent output .fits files.
    parent_fits = []
    if parents:
        for i in parents.keys():
            (parent_outputs, _, _) = make_output_file_list(db, i)
            # remove everything that isn't a .fits file from output list.
            [parent_fits.append(i)
             for i in parent_outputs if '.fits' in i.name]

    return {
        'title': 'Job {}'.format(job_id),
        'info': info,
        'qalog': qalog,
        'output_files': output_files,
        'parents': parents,
        'children': children,
        'log_files': log_files,
        'previews': list(zip(previews1024, previews1024)),
        'states': JSAProcState.STATE_ALL,
        'obsinfo': obs_info,
        'parent_obs': parent_obs,
        'qa_states': JSAQAState.STATE_ALL,
        'pagination': pagination,
        'nopreview': nopreview,
        'parent_fits': parent_fits,
    }
