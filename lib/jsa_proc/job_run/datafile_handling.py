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
import subprocess
import shutil

from jsa_proc.jac.file import file_in_dir, file_in_jac_data_dir
from jsa_proc.cadc.fetch import fetch_cadc_file
from jsa_proc.job_run.directories import get_input_dir
from jsa_proc.error import JSAProcError
from jsa_proc.config import get_config

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
    lis_name = 'input_files_job.lis'

    # Make the input directory if it doesn't exist. (Permissions?).
    if (not os.path.exists(input_directory)
            and not os.path.isdir(input_directory)):
        os.mkdir(input_directory)

    # Create list of files to write to.
    list_name_path = os.path.join(input_directory, lis_name)
    avail_file_list = open(list_name_path, 'w')

    # For each file, check if its already in JAC data store, or input
    # directory. Download from CADC if its not. Check downloaded files
    # are valid hds.
    for f in input_file_list:

        filepath = file_in_jac_data_dir(f)

        if filepath:
            avail_file_list.write(filepath + os.linesep)

        else:
            filepath = file_in_dir(f, input_directory)

            if filepath:
                avail_file_list.write(filepath + os.linesep)
            else:
                filepath = fetch_cadc_file(f, input_directory)
                valid = valid_hds(filepath)

                if not valid:

                    # Move invalid file to different directory and raise an error.
                    invalid_dir = os.path.join(input_directory, 'invalid')
                    invalid_file = os.path.join(invalid_dir, os.path.split(filepath)[1])

                    if not os.path.exists(invalid_dir):
                        os.mkdir(invalid_dir)
                    shutil.move(filepath, invalid_file)
                    raise JSAProcErrror('Downloaded file %s fails hds validation'
                                        ' Moved to %s'%(filepath, invalid_file))
                else:
                    avail_file_list.write(filepath + os.linesep)

    avail_file_list.close()

    # Return filepath for .lis containing all files with paths.
    return list_name_path


def get_output_files(job_id):
    """
    Get the current list of output files from the output directory.

    This command trusts that whatever is in the output directory at the
    time it is called is the correct list of output files.

    parameter:
    job_id, integer

    returns: list of strings.
    Each string is an absolute path
    """

    # find output_dir
    output_dir = get_output_dir(job_id)

    # Check it exists and is a directory: raise error if not
    if not os.path.exists(output_dir) or not os.path.isdir:
        raise JSAProcError('The output directory %s for job %i does not exits' % (output_dir, job_id))

    # Get list of files in directory:
    contents = os.listdir(output_dir)

    # Get abspath for each item in directory
    contents = [os.path.join(output_dir, f) for f in contents]

    return contents


def valid_hds(filepath):
    """
    Checks to see if a given file is a valid hds file.

    This uses hdstrace, and assumes if it can provide a return
    code of 0 then the file is valid.
    It runs hdstrace from the starlink build defined in the
    run_job.starpath section of the config file.

    parameter:
    filepath: string
    full filename including path and suffix.

    returns Boolean
    True: file is valid hds
    False: file is not valid hds.
    """

    # Path to hdstrace.
    config = get_config()
    starpath = config.get('job_run', 'starpath')
    com_path = os.path.join(starpath, 'bin', 'hdstrace')


    # Environmental variables.
    myenv = os.environ.copy()
    myenv['ADAM_NOPROMPT'] = '1'
    myenv['ADAM_EXIT'] = '1'

    # Run hdstrace.
    with open('/dev/null', 'w') as null:
        returncode = subprocess.call([com_path, filepath, 'QUIET'],
                                     env=myenv,
                                     stdout=null, stderr=subprocess.STDOUT,
                                     shell=False)

    # Status is True for returncode=0, False otherwise.
    return returncode == 0
