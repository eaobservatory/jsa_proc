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

from contextlib import contextmanager
from datetime import datetime
import os
import os.path
import tempfile

from jsa_proc.error import JSAProcError
from jsa_proc.config import get_config


def get_input_dir(job_id):
    """Get the data input directory for a given job."""

    return _get_dir('input', job_id)


def get_output_dir(job_id):
    """Get the data output directory for a given job."""

    return _get_dir('output', job_id)


def get_scratch_dir(job_id):
    """Get the scratch directory for a given job."""

    return _get_dir('scratch', job_id)


def get_log_dir(job_id):
    """Get the log directory for a given job."""

    return _get_dir('log', job_id)


def make_temp_scratch_dir(job_id):
    """Create and return path to a temporary working directory inside
    of a job's scratch directory."""

    scratch_base_dir = get_scratch_dir(job_id)
    if not os.path.exists(scratch_base_dir):
        os.makedirs(scratch_base_dir)

    scratch = os.path.join(scratch_base_dir, _get_timestamp())

    if os.path.exists(scratch):
        scratch = tempfile.mkdtemp(prefix=scratch)
    else:
        os.mkdir(scratch)

    return scratch


@contextmanager
def open_log_file(job_id, log_name):
    """Context manager which supplies a log file handle.

    Opens a new log file created in a job's log directory with
    a new unique file name based on the given log name and current
    timestamp.  The log file is automatically closed at the
    end of the context block.
    """

    log_dir = get_log_dir(job_id)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Make logfile name using timestamp
    timestamp = _get_timestamp()
    logfile = os.path.join(log_dir, '{0}_{1}.log'.format(log_name, timestamp))

    # Open logfile
    if os.path.exists(logfile):
        log = tempfile.NamedTemporaryFile(
            prefix='{0}_{1}'.format(log_name, timestamp),
            dir=log_dir, suffix='.log',
            delete=False)
    else:
        log = open(logfile, 'w')

    # Provide the log file handle to the calling with statement.
    try:
        yield log

    finally:
        log.close()


def _get_dir(type_, job_id):
    if not isinstance(job_id, int):
        raise JSAProcError('Cannot determine directory '
                           'for non-integer job identifier')

    config = get_config()
    basedir = config.get('directories', type_)

    # Turn the job ID into a decimal string of at least 9
    # digits, then create subdirectories by removing the last 6
    # and then the last 3 digits.  This means that we retain the
    # full length name in the final directory (unlike Git) to
    # try to prevent accidental collisions if the directories are
    # manipulated manually.  The digits are counted back from the
    # end of the decimal string so that any digits in excess of
    # the fixed 9 end up in the first component.
    decimal = '{0:09d}'.format(job_id)
    return os.path.join(basedir, decimal[:-6], decimal[:-3], decimal)


def _get_timestamp():
    return datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
