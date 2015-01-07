# Copyright (C) 2015 Science and Technology Facilities Council.
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

import re
import logging
import subprocess

from jsa_proc.config import get_config
from jsa_proc.util import restore_signals

logger = logging.getLogger(__name__)

fitsverify_output = re.compile(' (\d+) warnings and (\d+) errors')


def valid_fits(filename, allow_warnings=True):
    """Check whether a given file is a valid FITS file.

    This uses fitsverify with the -q option to determine the number
    of errors and warnings.  Returns True unless there are errors,
    or there are warnings and the allow_warning option is not set.
    """

    fitsverify = get_config().get('utilities', 'fitsverify')

    # Fitsverify exits with bad status even if there are warnings, so we
    # can't just use subprocess.check_output.
    logger.debug("Running fitsverify on file %s", filename)
    p = subprocess.Popen([fitsverify, '-q', filename],
                         stdout=subprocess.PIPE,
                         preexec_fn=restore_signals)

    (out, _) = p.communicate()
    logger.debug(out.rstrip())

    if out.startswith('verification OK'):
        return True

    elif not allow_warnings:
        return False

    m = fitsverify_output.search(out)

    if not m:
        logger.error("Fitsverify output did not match expected pattern")
        return False

    # warnings = int(m.group(1))
    errors = int(m.group(2))

    # Already know we are in "allow_warnings" mode, so just check the
    # number of actual errors.
    return not errors
