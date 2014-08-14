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

STARLINK_DIR and ORAC_DIR need to be set in environment?
"""

import subprocess
import tempfile

from jsa_proc.config import get_config
from jsa_proc.job_run.directories import get_scratch_dir, get_log_dir
from jsa_proc.error import JSAProcError

def jsawrapdr_run(job_id, input_file_list,  mode,
                  cleanup='cadc',location='JAC' ):
    """Routine to execute jsawrapdr from python.

    Takes in a job_id, input_file_list, and mode.

    Calls jsawrapdr with following options
    jsawrapdr --outdir=configbase/scratch/$job_id
              --inputs=input_file_list
              --id = jac-$job_id
              --mode=$mode 
              --cleanup=$cleanup (cadc by default)
              --location=$location (JAC by default)

    job_id, integer
    job identifier from jsaproc database

    input_file_list, string
    list of files (with extensions and full path).

    mode, string
    night, obs, public or project.

    cleanup, optional, string |'cadc'|'none'|'all'
    The jsawrapdr clean option, default is 'cadc'.

    # Location is not currently implemented!
    location, string |'cadc'|'JAC'|
    The default is 'JAC', others probably never used?

    Returns: (retcode, logfilename)
    This is the retcode from jsawrapdr (integer) and the path+name of
    the logfile (string).

    """

    # Get config information.
    scratch_base_dir = get_scratch_dir(job_id)
    log_dir = get_log_dir(job_id)

    # Get scratchdir and logfile name using timestamp
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%s')
    scratch_dir = os.path.join(scratch_base_dir, timestamp)
    if os.path.exists(scratch_dir):
        scratch_dir = tempfile.mkdtemp(prefix=scratch_dir)
    else:
        os.mkdir(scratch_dir)
    logfile = os.path.join(log_dir, 'jsawrapdr_'+timestamp+'.log')

    # Find path to jsawrapdr
    config = get_config()
    starpath = config.get('job_run', 'starpath')
    jsawrapdr_path = os.path.join(starpath, 'Perl', 'bin', 'jsawrapdr')
    orac_dir = os.path.join(starpath, 'bin', 'oracdr', 'src')


    # jac recipe id
    jacid = 'jac-'+str(job_id)

    # jsawrapdr arguments.
    jsawrapdrcom = [jsawrapdr_path,
                    '--outdir='+scratch_dir,
                    '--inputs='+input_file_list,
                    '--id='+jacid,
                    '--mode='+mode,
                    '--cleanup='+cleanup]

    # environment
    jsa_env = os.environ.copy()
    jsa_env['STARLINK_DIR'] = starpath
    jsa_env['ORAC_DIR'] = orac_dir
    # Remainder of oracdr required environmental variables are set
    # inside WrapDR.pm->run_pipeline

    # Run jsawrapdr and get the returncode and error information
    p = subprocess.Popen(jsawrapdrcom, env=jsa_env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    stdout, stderr = p.communicate()

    # Write out the stdout to file (stdout includes stderr).
    if os.path.exists(logfile):
        log = tempfile.NamedTemporaryFile(prefix='jsawrapdr_'+timestamp,
                                          dir=log_dir, suffix='.log', delete=False)
    else:
        log = open(logfile, 'w')
    log.write(stdout)
    log.close()

    retcode = p.returncode

    if retcode != 0:
        raise JSAProcError('jsawrapdr exited with non zero status. '
                           'Retcode was %i; job_id is %i.'
                           'stdin/stderr are written in %s'%(retcode, job_id, log.name),
                           retcode, job_id, log.name)

    return log.name

