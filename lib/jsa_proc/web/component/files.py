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

from collections import namedtuple
import re

from jsa_proc.admin.directories import get_output_dir
from jsa_proc.error import NoRowsError
from jsa_proc.web.util import url_for

FileInfo = namedtuple('FileInfo', ['name', 'url', 'mtype'])
PreviewInfo = namedtuple('PreviewInfo', ['url', 'caption'])


def make_output_file_list(db, job_id, preview_filter=None):
    """Prepare output file lists for job information pages.
    """

    output_files = []
    previews1024 = []
    previews256 = []

    try:
        for i in sorted(db.get_output_files(job_id)):
            url = None
            mtype = None

            if i.endswith('.png'):
                url = url_for('job_preview', job_id=job_id, preview=i)

                if preview_filter is None or any((f in i for f in preview_filter)):
                    caption = i
                    caption = re.sub('^jcmt_', '', caption)
                    caption = re.sub('_(preview_)?\d+\.png', '', caption)

                    if '_256.png' in i:
                        previews256.append(PreviewInfo(url, caption))

                    if '_1024.png' in i:
                        previews1024.append(PreviewInfo(url, caption))

            elif i.endswith('.pdf'):
                url = url_for('job_preview_pdf', job_id=job_id, preview=i)

            elif i.endswith('.txt'):
                url = url_for('job_text_file', job_id=job_id, text_file=i)

            elif i.endswith('.fits'):
                url = 'file://{0}/{1}'.format(get_output_dir(job_id), i)

                if re.search('-cat[0-9]{6}', i):
                    mtype = 'table.load.fits'

                elif re.search('-moc[0-9]{6}', i):
                    # This should be "coverage.load.moc.fits" but neither GAIA
                    # nor Aladin appear to subscribe to that mtype yet.
                    # mtype = 'coverage.load.moc.fits'
                    mtype = 'image.load.fits'

                elif '_rsp_' in i:
                    # Prevent a broadcast button being shown for spectra
                    # for now.
                    mtype = None

                else:
                    mtype = 'image.load.fits'

                # Remove URL for types we can't broadcast.
                if mtype is None:
                    url = None

            output_files.append(FileInfo(i, url, mtype))

    except NoRowsError:
        pass

    return (output_files, previews1024, previews256)
