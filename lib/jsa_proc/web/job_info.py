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

from jsa_proc.admin.directories import get_output_dir
from jsa_proc.error import NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.web.log_files import get_log_files
from jsa_proc.web.job_search import job_search
from jsa_proc.web.util import Pagination, url_for, HTTPNotFound

FileInfo = namedtuple('FileInfo', ['name', 'url'])


def prepare_job_info(db, job_id, query):
    # Fetch job information from the database.
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

    # Try to get tiles.
    try:
        tiles = db.get_tilelist(job_id)
    except NoRowsError:
        tiles = None
    if tiles == []:
        tiles = None

    # Try to get input files (if any)
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
        pjobs = parents.keys()
        pjobs.sort()
        for i in pjobs:
            parent_obs[i] = [o._asdict() for o in db.get_obs_info(i)]
    except NoRowsError:
        parents = None
        parent_obs = None

    # See if there are any child jobs.
    try:
        children = db.get_children(job_id)
    except NoRowsError:
        children = None
    previews256 = []
    previews1024 = []
    try:
        output_files = []

        for i in db.get_output_files(job.id):
            s = i.find('preview_256.png')
            if s != -1:
                previews256.append(i)
            s = i.find('preview_1024.png')
            if s != -1:
                previews1024.append(i)

            if i.endswith('.fits'):
                url = 'file://{0}/{1}'.format(get_output_dir(job_id), i)

            else:
                url = None

            output_files.append(FileInfo(i, url))

    except NoRowsError:
        output_files = []

    obs_info = db.get_obs_info(job.id)

    if obs_info:
        obs_info = [o._asdict() for o in obs_info]

    else:
        obs_info = None

    if previews256:
        previews256 = [url_for('job_preview', job_id=job.id, preview=i)
                       for i in previews256]
    if previews1024:
        previews1024 = [url_for('job_preview', job_id=job.id, preview=i)
                        for i in previews1024]

    # Logged entries in the database (newest first).
    log = db.get_logs(job_id)
    log.reverse()

    # Get the log files on disk (if any)
    log_files = get_log_files(job_id)

    # If we know what the user's job query was (from the session information)
    # then set up pagination based on the previous and next job identifiers.
    if query is not None:
        (url_query, job_query) = job_search(**query)
        (prev, next) = db.job_prev_next(job_id, **job_query)
        pagination = Pagination(
            None,
            None if prev is None else url_for('job_info', job_id=prev),
            None if next is None else url_for('job_info', job_id=next),
            None,
            url_for('job_list', **url_query))
    else:
        pagination = None

    return {
        'title': 'Job {}'.format(job_id),
        'info': info,
        'tiles': tiles,
        'log': log,
        'input_files': input_files,
        'parents': parents,
        'children': children,
        'output_files': output_files,
        'log_files': log_files,
        'previews': zip(previews256, previews1024),
        'states': JSAProcState.STATE_ALL,
        'obsinfo': obs_info,
        'parent_obs': parent_obs,
        'pagination': pagination,
    }
