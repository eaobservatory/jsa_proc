# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2018-2021 East Asian Observatory.
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

from __future__ import print_function

import logging
import os
import os.path
import re
import subprocess
import shutil

from jsa_proc.admin.directories import get_input_dir, get_output_dir, get_log_dir
from jsa_proc.db.db import JSAProcFileInfo
from jsa_proc.jac.file import file_in_dir, file_in_jac_data_dir
from jsa_proc.cadc.fetch import fetch_cadc_file
from jsa_proc.error import JSAProcError, NotAtJACError, NoRowsError, \
    ParentNotReadyError
from jsa_proc.config import get_config, get_database
from jsa_proc.files import get_md5sum, get_size
from jsa_proc.state import JSAProcState

"""
Routines for handling input and output files when running jobs.

"""

logger = logging.getLogger(__name__)

# Name of .lis file containing each input file with full path.
input_list_name = 'input_files_job.lis'


def is_file_in_a_dir(filename, directory):
    """
    Checks whether a filename is present in a given directory.
    If present, returns full path.
    Otherwise returns False

    filename: string
    filename with extension.

    directory: string
    full path of directory to check for file.
    """
    pathname = os.path.join(directory, filename)
    if os.path.exists(pathname):
        return pathname
    return False


def check_data_already_present(job_id, db):
    """
    Check if all data are present already on disk,
    outside of the input directory.

    This is intended to be run by the statemachine.

    If all data is present, it will return a list with
    each input file (from input table or parent table) and
    its full path.

    It does not check the input directory for this job
    for files.

    It does not copy any files into the input directory.
    """

    try:
        input_file_list = db.get_input_files(job_id)
        inputs = get_jac_input_data(input_file_list)
    except NoRowsError:
        inputs = []

    try:
        parents = db.get_parents(job_id, with_state=True)
        parent_files_with_paths = []
        for p, filts, parent_state in parents:
            if parent_state not in JSAProcState.STATE_POST_RUN:
                raise ParentNotReadyError('Parent job {} is not ready'.format(p))

            outputs = db.get_output_files(p)
            parent_files = filter_file_list(outputs, filts)
            for f in parent_files:
                filepath = is_file_in_a_dir(f, get_output_dir(p))
                if not filepath:
                    raise NotAtJACError(f)
                else:
                    inputs.append(filepath)
    except NoRowsError:
        pass
    return inputs


def get_jac_input_data(input_file_list):
    """
    Try and assemble data for job, if it is all in the JAC
    standard tree.

    Raise NotAtJACError if data is not  *all* present at JAC.

    Returns list of file paths if all files are present.
    """

    inputsfiles = []
    for f in input_file_list:
        filepath = file_in_jac_data_dir(f)
        if not filepath:
            raise NotAtJACError(f)
        else:
            inputsfiles.append(filepath)
    return inputsfiles


def write_input_list(job_id, input_file_list):
    """
    Write a textfile to list in the input directory
    with the full list of file names.

    Returns the name of the textfile.
    """

    input_directory = get_input_dir(job_id)
    if not os.path.exists(input_directory):
        os.makedirs(input_directory)
    fname = os.path.join(input_directory, input_list_name)
    f = open(fname, 'w')
    for i in input_file_list:
        logger.debug('Writing to input list: %s', i)
        f.write(i + os.linesep)
    f.close()
    return fname


def setup_input_directory(job_id):
    """
    Setup an input directory for a given job.

    Get the input directory path, create if it doesn't exist, return
    the full path.

    Parameters:
    *job_id*: int, mandatory
    Integer of processing job in system

    Returns:
    input_directory: string
    Full patht to input data directory.
    """
    # Get full path to input directory.
    input_directory = get_input_dir(job_id)
    # Make the input directory if it doesn't exist. (Permissions?).
    if (not os.path.exists(input_directory)
            and not os.path.isdir(input_directory)):
        os.makedirs(input_directory)

    return input_directory


def assemble_parent_data_for_job(
        job_id, parent_job_id, parent_files, force_new=False):
    """
    This routine ensures that all the input data from parent jobs is
    available for running a job.

    It takes in the current job_id, the job_id of the parent job
    it is assembling data for, and the list of parent_files it
    needs to find for that job.

    option 'force_new' will force this function to ignore data already in the
    input file directory

    It will first of look in the output data directory for the parent job,
    and if not there it will download the file from CADC.
    """

    input_directory = setup_input_directory(job_id)

    # Output directory for parent jobs.
    dirpath = get_output_dir(parent_job_id)

    # List to hold full paths to input files.
    files_list = []
    for f in parent_files:

        # First of all check if file is already in input directory.
        filepath = is_file_in_a_dir(f, input_directory)
        if filepath and not force_new:
            files_list.append(filepath)
        else:

            # Then check if file is in parent output directory, copy
            # it in if so.
            logger.debug('Parent file: %s', f)
            logger.debug('dirpath = %s', dirpath)
            filepath = is_file_in_a_dir(f, dirpath)
            logger.debug('filepath = %s', filepath)
            if filepath:
                shutil.copy(filepath, input_directory)
                filepath = os.path.join(input_directory,
                                        os.path.split(filepath)[1])
                files_list.append(filepath)

            else:
                filepath = fetch_cadc_file(f, input_directory)
                valid = valid_hds(filepath)
                # If downloaded file is not valid:
                if not valid:
                    # Move invalid file to different directory and raise an
                    # error.
                    invalid_dir = setup_invalid_dir(input_directory)
                    invalid_file = os.path.join(invalid_dir,
                                                os.path.split(filepath)[1])
                    shutil.move(filepath, invalid_file)
    return files_list


def assemble_input_data_for_job(job_id, input_file_list):
    """
    This routine ensure that all the input data in the input table is
    available for running a job.

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

    # Get full path to input directory and make it if it doesn't exist.
    input_directory = setup_input_directory(job_id)

    # For each file, check if its already in JAC data store, or input
    # directory. Download from CADC if its not. Check downloaded files
    # are valid hds.
    files_list = []
    for f in input_file_list:

        filepath = file_in_jac_data_dir(f)

        if filepath:
            files_list.append(filepath)

        else:
            filepath = file_in_dir(f, input_directory)

            if filepath:
                files_list.append(filepath)
            else:
                filepath = fetch_cadc_file(f, input_directory)
                valid = valid_hds(filepath)

                if not valid:

                    # Move invalid file to different directory and raise an
                    # error.
                    invalid_dir = setup_invalid_dir(input_directory)
                    invalid_file = os.path.join(invalid_dir,
                                                os.path.split(filepath)[1])
                    shutil.move(filepath, invalid_file)
                    raise JSAProcError(
                        'Downloaded file %s fails hds validation'
                        ' Moved to %s' % (filepath, invalid_file))
                else:
                    files_list.append(filepath)

    # Return list of files with full paths.
    return files_list


def setup_invalid_dir(input_directory):
    """
    Create a directory to hold invalid files.

    """
    invalid_dir = os.path.join(input_directory, 'invalid')
    if not os.path.exists(invalid_dir):
        os.mkdir(invalid_dir)
    return invalid_dir


def get_output_files(job_id):
    """
    Get the current list of output files from the output directory.

    This command trusts that whatever is in the output directory at the
    time it is called is the correct list of output files.

    parameter:
    job_id, integer

    returns: list of JSAProcFileInfo objects.
    Each object contains a plain filename, with no path attached.
    """

    # find output_dir
    output_dir = get_output_dir(job_id)

    # Check it exists and is a directory: raise error if not
    if not os.path.exists(output_dir) or not os.path.isdir:
        raise JSAProcError(
            'The output directory %s for job %i does not exist' %
            (output_dir, job_id))

    # Get list of files in directory:
    contents = os.listdir(output_dir)

    return [JSAProcFileInfo(x, get_md5sum(os.path.join(output_dir, x)))
            for x in contents]

def get_output_log_files(job_id):
    """
    Get the current list of output log.* files from the log directory.

    This command trusts whatever is in the log directory and starts
    with log.* is the correct list of output log files.

    Returns: list of bare file names
    """
    log_dir = get_log_dir(job_id)

    if not os.path.exists(log_dir) or not os.path.isdir:
        raise JSAProcError(
            'The log directory %s for job %i does not exist.' % (log_dir, job_id))

    pattern = re.compile('log.*')
    logs = [i for i in os.listdir(log_dir) if pattern.match(i)]

    return logs

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
    myenv['LD_LIBRARY_PATH'] = os.path.join(starpath, 'lib')

    # Run hdstrace.
    returncode = subprocess.call([com_path, filepath, 'QUIET'],
                                 env=myenv,
                                 stderr=subprocess.STDOUT,
                                 shell=False)

    # Status is True for returncode=0, False otherwise.
    return returncode == 0


def filter_file_list(filelist, filt):
    """
    Filter out only files that match a given filter.

    filelist: list of strings.
    Each string is a file name.

    filt: string.
    string for a regular expression search.
    Only files that match the re.search option
    will be returned.

    returns:
    filtered filelist
    list containing only files that match the filter.

    """

    match = re.compile(filt)

    filtered = []
    for f in filelist:
        if match.search(f):
            filtered.append(f)
    return filtered


def disk_usage_input(tasks):
    _disk_usage(get_input_dir, tasks=tasks)


def disk_usage_output(tasks):
    _disk_usage(get_output_dir, tasks=tasks)


def _disk_usage(dir_function, tasks=None):
    db = get_database()

    if not tasks:
        tasks = db.get_tasks()

    for task in tasks:
        jobs = db.find_jobs(
            location='JAC', task=task, state=JSAProcState.STATE_ALL)

        total = 0.0
        for job in jobs:
            directory = dir_function(job.id)

            if not os.path.exists(directory):
                continue

            total += get_size(directory)

        print('{:10.3f} {}'.format(total, task))
