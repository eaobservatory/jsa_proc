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


import os.path
import re

from jsa_proc.admin.directories import get_output_dir
from jsa_proc.web.util import HTTPError, HTTPNotFound

valid_preview_patterns = {
    'png': re.compile('^[-+_a-zA-Z0-9]+\.png$'),
    'pdf': re.compile('^[-+_a-zA-Z0-9]+\.pdf$'),
    'txt': re.compile('^[-+_a-zA-Z0-9]+\.txt$'),
}


def prepare_job_preview(job_id, preview, type_='png'):
    """
    Prepare a preview image for a job.

    Return the path to the preview image
    """

    valid_preview = valid_preview_patterns.get(type_)

    if valid_preview is None:
        raise HTTPError('Unexpected preview type requested')

    if not valid_preview.match(preview):
        raise HTTPError('Invalid preview filename')

    preview_path = os.path.join(get_output_dir(job_id), preview)

    if not os.path.exists(preview_path):
        raise HTTPNotFound()

    return preview_path
