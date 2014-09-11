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

import re
import os.path

from jsa_proc.error import JSAProcError

scuba2_file = re.compile('^(s[48][abcd])([0-9]{8})_([0-9]{5})_[0-9]{4}$')
acsis_file = re.compile('^a([0-9]{8})_([0-9]{5})_[0-9]{2}_[0-9]{4}$')

def get_jac_data_dir(filename):
    """Guess directory name for a given filename.

    Given a bare raw data filename, return the standardized
    directory name for where that file should be located.
    """

    m = scuba2_file.match(filename)
    if m:
        (subarray, date, obsnum) = m.groups()

        return os.path.join('/jcmtdata/raw/scuba2', subarray, date, obsnum)

    m = acsis_file.match(filename)
    if m:
        (date, obsnum) = m.groups()

        return os.path.join('/jcmtdata/raw/acsis/spectra', date, obsnum)

    raise JSAProcError('Filename {0} does not match '
                       'an expected pattern'.format(filename))


def file_in_dir(filename, dir):
    """
    Check whether a filename is present in an arbitrary directory.
    Checks for both .sdf and .sdf.gz files.

    If present, return full pathname to it.
    Otherwise return False

    filename:
    filename without extension

    dir: string
    directory to check for file
    """

    pathname = os.path.join(dir, filename) + '.sdf'

    if os.path.exists(pathname):
        return pathname

    # Try again with .gz suffix
    pathname += '.gz'

    if os.path.exists(pathname):
        return pathname

    return False


def file_in_jac_data_dir(filename):
    """Check whether a file is present in the JAC data directories.

    If it is present, return the full pathname to it.
    Otherwise return False.
    """

    dir = get_jac_data_dir(filename)

    return file_in_dir(filename, dir)
