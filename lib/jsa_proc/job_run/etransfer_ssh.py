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

from __future__ import print_function, division, absolute_import

import logging
import subprocess

from jsa_proc.config import get_config

logger = logging.getLogger(__name__)


def ssh_etransfer_send_output(job_id):
    """SSH to the e-transfer host to request the e-transfer of
    a job's output files."""

    config = get_config()

    logger.debug('Attempting e-transfer of job %i output', job_id)

    subprocess.check_call([
        '/usr/bin/ssh', '-x',  '-i',
        config.get('etransfer', 'key'),
        '{0}@{1}'.format(config.get('etransfer', 'user'),
                         config.get('etransfer', 'machine')),
        str(job_id)],
        shell=False)
