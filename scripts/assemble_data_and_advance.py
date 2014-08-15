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

from jsa_proc.job_run.scripts import fetch

description = """
Script which looks at a jsa processing job in the queued state,
assembles the data for it, and then advances the job to the waiting state.
"""


# Handle arguments
parser = argparse.ArgumentParser(description=description)
parser.add_argument('job_id', type=int,
                    help = 'Integer identifying the job in the JSA Job processing database')
args = parser.parse_args()
job_id = args.job_id

# Fetch the data.
@ErrorDecorator
fetch(job_id)
