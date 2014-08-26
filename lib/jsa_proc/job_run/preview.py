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

import re

from PIL import Image


def scale_preview(filename, height=64):
    """Create a scaled version of a preview at the given vertical size."""

    filename_new = re.sub(
        '_preview_\d+\.png',
        '_preview_{0}.png'.format(height),
        filename)

    im = Image.open(filename)

    # The preview's "size" is the vertical size, and it may comprise one
    # or more adjacent square panels of this height.  Therefore scale the
    # width by the scale factor being applied to the height.
    (width_orig, height_orig) = im.size
    width = int(width_orig * height / height_orig)

    im.resize((width, height)).save(filename_new)
