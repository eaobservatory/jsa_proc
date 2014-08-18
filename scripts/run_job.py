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

import argparse

from jsa_proc.job_run.scripts import run_job

description = """
Script which runs a jsa_processing job locally,
and marks the state appropriately.
"""
# Handle arguments
parser = argparse.ArgumentParser(description=description)
parser.add_argument('job_id', type=int,
                    help = 'Integer identifying the job in the JSA Job processing database')
args = parser.parse_args()
job_id = args.job_id

run_job(job_id)
