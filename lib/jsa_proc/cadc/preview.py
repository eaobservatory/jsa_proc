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

from __future__ import absolute_import, print_function

import logging
import os
import re

from jsa_proc.cadc.fetch import fetch_cadc_file
from jsa_proc.error import JSAProcError
from jsa_proc.action.preview import scale_preview

logger = logging.getLogger(__name__)

preview_pattern = re.compile('_preview_\d+\.png$')


def fetch_cadc_previews(output_files, output_directory):
    """Fetch previews from a list of output files.

    Given a list of all of the output files of a CADC job,
    fetch those with filenames indicating that they
    are previews.  If the 64 pixel high preview is missing,
    attempt to create an approximation.

    Logs warnings on failure.
    """

    fetched = []

    for preview in output_files:
        if not preview_pattern.search(preview):
            continue

        try:
            logger.debug('Fetching preview file %s', preview)

            if not os.path.exists(output_directory):
                os.makedirs(output_directory)

            pathname = fetch_cadc_file(preview, output_directory)

            # Record those previews which were succesfully fetched.
            fetched.append(pathname)

        except JSAProcError:
            logger.warning('Could not retrieve preview file %s', preview)

    # Check whether there are any 64 pixel high previews missing.
    for preview_256 in filter((lambda x: x.endswith('_preview_256.png')),
                              fetched):
        preview_64 = preview_256[:-7] + '64.png'

        if preview_64 in fetched:
            logger.debug('%s has thumbnail %s', preview_256, preview_64)
        else:
            logger.debug('Creating thumbnail for %s', preview_256)
            scale_preview(preview_256, height=64)
