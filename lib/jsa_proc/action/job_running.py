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
Routines for running the perl-JSA script 'jsawrapdr' from the JAC
processing system.
"""

import shutil
import subprocess
import os
import re

from jsa_proc.admin.directories \
    import make_temp_scratch_dir, get_log_dir, get_output_dir, open_log_file
from jsa_proc.config import get_config
from jsa_proc.error import JSAProcError
from jsa_proc.util import restore_signals


def jsawrapdr_run(job_id, input_file_list, mode, drparameters,
                  cleanup='cadc', location='JAC', persist=False,
                  jsawrapdr=None, debug=False):
    """
    Execute jsawrapdr script from python.

    This function calls jsawrapdr with following options:

    jsawrapdr --outdir=configbase/scratch/$job_id
              --inputs=input_file_list
              --id = jac-$job_id
              --mode=$mode
              --drparameters=$drparameters
              --cleanup=$cleanup (cadc by default)
              --location=$location (JAC by default)

         if persist is True, then it adds the flag:
              -persist

    Args:

      job_id (int): Job identifier from jsaproc database.

      input_file_list (str): List of files (with extensions and full
        path).

      mode (str): Can be 'night', 'obs', 'public' or 'project'.

      drparameters (str):

      cleanup (str, optional): Type of cleanup. Can be one of
        'cadc'|'none'|'all', defaults to 'cadc'.

      persist (bool, optional): Defaults to False If persist is turned
        on, then dpCapture will copy acceptable products to the
        default output directory. Otherwise it won't (used for
        debugging purposes). The output directory is determined by
        jsa_proc.admin.directories 'get_output_dir' for the given
        job_id.

      location (str, optional): One of |'cadc'|'JAC'| (NOT CURRENTLY
        IMPLEMENTED, default is 'JAC')


      jsawrapdr (str, optional): The path to jsawrapdr. If not given,
        the one in configured starlink will be used.

      debug (bool, optional): Turn on jsawrapdr debugging if true,
        default is False.

    Returns:
      str: The filename (including path) of the logfile.

    """

    # Get log directory.  Note that opening a log file in this
    # directory using open_log_file will ensure that it exists.
    log_dir = get_log_dir(job_id)

    # Prepare scratch directory.
    scratch_dir = make_temp_scratch_dir(job_id)

    # Get output directory name
    out_dir = get_output_dir(job_id)

    # If output dir currently exists, delete the directory.
    if os.path.exists(out_dir):
        # This will leave the parent directory, so dpCapture can re-create
        # the "transfer" directory.
        shutil.rmtree(out_dir)
    else:
        # The parent directory may not exist, so we must make the "transfer"
        # directory because dpCapture will not do so in that case.
        os.makedirs(out_dir)

    # Find path to jsawrapdr and orac_dr
    config = get_config()
    starpath = config.get('job_run', 'starpath')
    if not jsawrapdr:
        jsawrapdr = os.path.join(starpath, 'Perl', 'bin', 'jsawrapdr')
    orac_dir = os.path.join(starpath, 'bin', 'oracdr', 'src')

    # jac recipe id
    jacid = 'jac-'+str(job_id)

    # jsawrapdr arguments.
    jsawrapdrcom = [jsawrapdr,
                    '--debugxfer',
                    '--outdir='+scratch_dir,
                    '--inputs='+input_file_list,
                    '--id='+jacid,
                    '--mode='+mode,
                    '--cleanup='+cleanup,
                    '--drparameters='+drparameters]
    if persist:
        jsawrapdrcom.append('-persist')
        jsawrapdrcom.append('--transdir='+out_dir)

    if debug:
        jsawrapdrcom.append('-debug')

    # environment
    jsa_env = os.environ.copy()
    jsa_env['STARLINK_DIR'] = starpath
    jsa_env['PATH'] = os.path.join(starpath, 'bin') + os.pathsep + \
        jsa_env.get('PATH', '')
    jsa_env['LD_LIBRARY_PATH'] = os.path.join(starpath, 'lib') + os.pathsep + \
        jsa_env.get('LD_LIBRARY_PATH', '')
    jsa_env['ORAC_DIR'] = orac_dir
    jsa_env['ORAC_LOGDIR'] = log_dir
    jsa_env['STAR_LOGIN'] = '1'
    jsa_env['CONVERT_DIR'] = os.path.join(starpath, 'bin', 'convert')
    # Remainder of oracdr required environmental variables are set
    # inside WrapDR.pm->run_pipeline

    with open_log_file(job_id, 'jsawrapdr') as log:
        # Keep the log file name.
        log_name = log.name

        # Run jsawrapdr
        retcode = subprocess.call(jsawrapdrcom, env=jsa_env, bufsize=1,
                                  stdout=log, stderr=subprocess.STDOUT,
                                  preexec_fn=restore_signals)

    if retcode != 0:
        errormessage = 'jsawrapdr exited with Retcode %i ' % (retcode)

        # Find the first ORAC error message in the jsawrapdr log
        jsalogfile = open(log_name, 'r')
        lines = jsalogfile.read()
        jsalogfile.close()
        result = re.search(r'.*(STDERR:\s*.*)$', lines, re.DOTALL)
        if result:
            firsterror = result.group(1).split('\n')[1]

            # Insert the ORAC error at the start of the error message
            if firsterror:
                errormessage = 'ORAC ERROR: ' + firsterror + '.\n' + \
                               errormessage

        # Raise the error.
        raise JSAProcError(errormessage)
    # Need to return list of produced files in output directory?

    return log_name
