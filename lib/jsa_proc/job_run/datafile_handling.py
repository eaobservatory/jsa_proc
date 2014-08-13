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


import os
import os.path


from jsa_proc.jac.file import file_in_dir, file_in_jac_data_dir
from jsa_proc.cadc.fetch import fetch_cadc_file

from jsa_proc.job_run.directories import get_input_dir

"""
Routines for handling input and output files when running jobs.

"""


def assemble_input_data_for_job(job_id, input_file_list):
    """
    This routine ensure that all the input data is available for running a job.

    It will check to see if the data is present in either a) the
    /jcmtdata tree, or b) the input directory for this job. If it is
    not present it will download the data from CADC into the input
    directory. It will create the input directory if not present.

    parameters;
    job_id: integer, id of job in job database.

    input_file_list: list of strings.
    iterable of strings, each string being the name of file.
    filenames must not include suffix.

    return: name of file output directory containing one filename per
    string, for every file.
    """

    # Get full path to input directory.
    input_directory = get_input_dir(job_id)

    # Name of .lis file containing each input file with full path.
    lis_name = 'input_files_job' + str(job_id) + '.lis'

    # Make the input directory if it doesn't exist. (Permissions?).
    if (not os.path.exists(input_directory)
            it and not os.path.isdir(input_directory)):
        os.mkdir(input_directory)

    # Create list of files to write to.
    list_name_path = os.path.join(input_directory, lis_name)
    avail_file_list = open(list_name_path, 'w')

    # For each file, check if its already in JAC data store, or input
    # directory. Download from CADC if its not.
    for f in input_file_list:

        filepath = file_in_jac_data_dir(f)

        if filepath:
            avail_file_list.write(jac_status+os.linesep)

        else:
            filepath = file_in_dir(filename, input_directory)

            if filepath:
                avail_file_list.write(status+os.linesep)
            else:
                filepath = fetch_cadc_file(filename, input_directory)

    # Currently this does not do any checking.
    avail_file_list.close()

    # Return filepath for .lis containing all files with paths.
    return list_name_path
