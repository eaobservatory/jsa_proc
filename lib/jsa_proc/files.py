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


"""
Functions for examining the files and disks.
"""

from __future__ import absolute_import, division, print_function

import os
import subprocess

from jsa_proc.config import get_config


def get_size(path):

    """
    Return the disk size of a given path is. This will include the
    size of all subdirectories if it is a folder.

    Uses units of GiB.

    Parameters:
    path, required, string.
    path of folder/disk to check size of.

    Returns:
    Size, float
    Disk usage of path (and any subidrectories) in units of gigabytes.
    """

    p = subprocess.Popen(['du', '-sk', path], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    sizek = float(stdout.split('\t')[0])
    sizeg = sizek/(1024**2)

    return sizeg


def get_space(path):
    """Determine the amout of available space on a given path.

    Return: available space in GiB."""

    s = os.statvfs(path)

    return s.f_bsize * s.f_bavail / (1024 ** 3)


def _get_config_dir_size(type_):
    """
    Return size of directorie tree described in config GiB.

    """
    config = get_config()
    dir_path = config.get('directories', type_)

    size = get_size(dir_path)

    return size


def _get_config_dir_space(type_):
    """Return space in a configured directory tree (GiB)."""

    config = get_config()
    return get_space(config.get('directories', type_))


def get_input_dir_size():
    """
    Get the size of the input data directory tree.

    Returns size in GiB.
    """
    return _get_config_dir_size('input')


def get_input_dir_space():
    """Determine space in input directory (GiB)."""

    return _get_config_dir_space('input')


def get_output_dir_size():
    """
    Get the size of the output data directory tree.

    Returns size in GiB.
    """
    return _get_config_dir_size('output')


def get_output_dir_space():
    """Determine space in output directory (GiB)."""

    return _get_config_dir_space('output')


def get_scratch_dir_size():
    """
    Get the size of the scratch data directory tree.

    Returns size in GiB.
    """
    return _get_config_dir_size('scratch')


def get_scratch_dir_space():
    """Determine space in scratch directory (GiB)."""

    return _get_config_dir_space('scratch')


def get_log_dir_size():
    """
    Get the size of the log data directory tree.

    Returns size in GiB.
    """
    return _get_config_dir_size('log')


def get_log_dir_space():
    """Determine space in log directory (GiB)."""

    return _get_config_dir_space('log')
