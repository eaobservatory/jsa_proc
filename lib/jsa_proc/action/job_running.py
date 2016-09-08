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
                  jsawrapdr=None, starlink_dir=None, debug=False,
                  version=None):
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
              --fileversion=$version (if not None)

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

      starlink_dir (str, optional): The path of a starlink install to
        use. If not given, the one found in the configuration file will be
        used.

      debug (bool, optional): Turn on jsawrapdr debugging if true,
        default is False.

      fileversion: CADC file name "version" or None to use default.

    Returns:
      str: The filename (including path) of the logfile.

    """

    # Get log directory.  Note that opening a log file in this
    # directory using open_log_file will ensure that it exists.
    log_dir = get_log_dir(job_id)

    # Prepare scratch directory.
    scratch_dir = make_temp_scratch_dir(job_id)

    # Get output directory name.
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

    # Find paths to starlink, jsawrapdr and orac_dr.
    config = get_config()

    if starlink_dir is None:
        starpath = config.get('job_run', 'starpath')
    else:
        starpath = starlink_dir
    if not jsawrapdr:
        jsawrapdr = os.path.join(starpath, 'Perl', 'bin', 'jsawrapdr')
    orac_dir = os.path.join(starpath, 'bin', 'oracdr', 'src')

    # Set thejac recipe id.
    jacid = 'jac-'+str(job_id)

    # Collect the jsawrapdr arguments.
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

    if version is not None:
        jsawrapdrcom.append('--fileversion={0}'.format(version))

    # Set up the environment for running jsawrapdr.
    jsa_env = os.environ.copy()
    jsa_env = setup_starlink(starpath, jsa_env)

    # Add in the LOGDIR
    jsa_env['ORAC_LOGDIR'] = log_dir

    # Open a log file and run jsawrapdr while saving output to log.
    with open_log_file(job_id, 'jsawrapdr') as log:

        # Save the log file name.
        log_name = log.name

        # Run jsawrapdr.
        retcode = subprocess.call(jsawrapdrcom, env=jsa_env, bufsize=1,
                                  stdout=log, stderr=subprocess.STDOUT,
                                  preexec_fn=restore_signals)

    # Handle jsawrapdr errors.
    if retcode != 0:
        errormessage = 'jsawrapdr exited with Retcode %i ' % (retcode)

        # Find the first ORAC error message in the jsawrapdr log.
        jsalogfile = open(log_name, 'r')
        lines = jsalogfile.read()
        jsalogfile.close()
        result = re.search(r'.*(STDERR:\s*.*)$', lines, re.DOTALL)
        if result:
            firsterror = result.group(1).split('\n')[1]

            # Insert the ORAC error at the start of the error message.
            if firsterror:
                errormessage = 'ORAC ERROR: ' + firsterror + '.\n' + \
                               errormessage

        # Raise the error.
        raise JSAProcError(errormessage)

    return log_name



def setup_starlink(starpath, env):
    """
    Setup all the starlink paths form the profile script.

    Args:
      starpath (str) : path to STARLINK_DIR
      env (dict) : initial environemntal variables.
    Returns:
      dictionary : containing environmental variables
    """

    env['STARLINK_DIR'] = starpath
    if env.has_key('PATH'):
        env['PATH']= os.path.join(starpath, 'bin') + os.pathsep + env['PATH']
    else:
        env['PATH']= os.path.join(starpath, 'bin')

    env['PATH'] = os.path.join(starpath, 'java', 'jre', 'bin', os.pathsep,
                               starpath, 'java', 'bin', os.pathsep, env['PATH'])

    if env.has_key('LD_LIBRARY_PATH'):
        env['LD_LIBRARY_PATH'] = os.path.join(starpath, 'lib', os.pathsep, env['LD_LIBRARY_PATH'])
    else:
        env['LD_LIBRARY_PATH'] = os.path.join(starpath, 'lib')

    # ORAC_DR paths
    orac_dir = os.path.join(starpath, 'bin', 'oracdr', 'src')
    env['ORAC_DIR'] = orac_dir
    env['ORAC_PERL5LIB'] = os.path.join(orac_dir, 'lib', 'perl5')
    env['ORAC_CAL_ROOT'] = os.path.join(starpath, 'bin', 'oracdr', 'cal')

    # Miscellaneous Starlink stuff
    env['STAR_LOGIN'] = '1'
    env['HDS_64BIT'] = '1'
    env['ADAM_ABBREV'] = '1'

    # This overwrites any existing PERL5LIB environ. Duplicating what the
    # $STARLINK_DIR/etc/profile script does...
    env['PERL5LIB'] = os.path.join(starpath, 'Perl', 'lib', 'perl5', 'site_perl')
    env['PERL5LIB'] = os.path.join(starpath, 'Perl', 'lib', 'perl5', os.pathsep, env['PERL5LIB'])

    # Setup the <packagename>_DIR environ variables. Not really sure
    # why these are needed...
    env['ATOOLS_DIR'] = os.path.join(starpath, 'bin', 'atools')
    env['AUTOASTROM_DIR'] = os.path.join(starpath, 'Perl', 'bin')
    env['CCDPACK_DIR'] = os.path.join(starpath, 'bin', 'ccdpack')
    env['CONVERT_DIR'] = os.path.join(starpath, 'bin', 'convert')
    env['CUPID_DIR'] = os.path.join(starpath, 'bin', 'cupid')
    env['CURSA_DIR'] = os.path.join(starpath, 'bin', 'cursa')
    env['DAOPHOT_DIR'] = os.path.join(starpath, 'bin', 'daophot')
    env['DATACUBE_DIR'] = os.path.join(starpath, 'bin', 'datacube')
    env['ECHOMOP_DIR'] = os.path.join(starpath, 'bin', 'echomop')
    env['ESP_DIR'] = os.path.join(starpath, 'bin', 'esp')
    env['EXTRACTOR_DIR'] = os.path.join(starpath, 'bin', 'extractor')
    env['FIGARO_DIR'] = os.path.join(starpath, 'bin', 'figaro')
    env['FIGARO_FORMATS'] = "ndf,dst"
    env['FIGARO_PROG_N'] =os.path.join(starpath, 'bin', 'figaro')
    env['FIGARO_PROG_N'] =os.path.join(starpath, 'etc', 'figaro')
    env['FLUXES_DIR'] = os.path.join(starpath, 'bin', 'fluxes')
    env['GAIA_DIR'] = os.path.join(starpath, 'bin', 'gaia')
    env['HDSTOOLS_DIR'] = os.path.join(starpath, 'bin', 'hdstools')
    env['HDSTRACE_DIR'] = os.path.join(starpath, 'bin', 'hdstrace')
    # Skip ICL and -- not used by orac.
    env['JCMTDR_DIR'] = os.path.join(starpath, 'bin', 'jcmtdr')
    # skip JPL?
    env['KAPPA_DIR'] = os.path.join(starpath, 'bin', 'kappa')
    env['KAPRH_DIR'] = os.path.join(starpath, 'bin', 'kaprh')
    env['PAMELA_DIR'] = os.path.join(starpath, 'bin', 'pamela')
    env['PERIOD_DIR'] = os.path.join(starpath, 'bin', 'period')
    env['PGPLOT_DIR'] = os.path.join(starpath, 'bin')
    env['PHOTOM_DIR'] = os.path.join(starpath, 'bin', 'photom')
    env['PISA_DIR'] = os.path.join(starpath, 'bin', 'pisa')
    env['POLPACK_DIR'] = os.path.join(starpath, 'bin', 'polpack')
    env['PONGO_DIR'] = os.path.join(starpath, 'bin', 'pongo')
    env['SMURF_DIR'] = os.path.join(starpath, 'bin', 'smurf')
    env['SPECX_DIR'] = os.path.join(starpath, 'share', 'specx')
    env['SST_DIR'] = os.path.join(starpath, 'bin', 'sst')
    env['STARBENCH_DIR'] = os.path.join(starpath, 'bin', 'starbench')
    # skip starman
    env['SURF_DIR'] = os.path.join(starpath, 'bin', 'surf')
    env['TSP_DIR'] = os.path.join(starpath, 'bin', 'tsp')

    # Starjava
    env['STILTS_DIR'] = os.path.join(starpath, 'starjava', 'bin', 'stilts')
    env['SPLAT_DIR'] = os.path.join(starpath, 'starjava', 'bin', 'splat')

    return env
