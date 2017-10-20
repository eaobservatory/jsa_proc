# Copyright (C) 2015 Science and Technology Facilities Council.
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

from __future__ import print_function, division, absolute_import

import os
import logging
import re
import shutil
import subprocess

from jsa_proc.admin.directories import get_misc_log_dir, make_misc_scratch_dir
from jsa_proc.config import get_config
from jsa_proc.error import JSAProcError, CommandError, NoRowsError
from jsa_proc.omp.config import get_omp_database
from jsa_proc.util import restore_signals

logger = logging.getLogger(__name__)

obsid_date = re.compile('_(\d{8})T')


def poll_raw_ingestion(
        date_start, date_end, quick=False, no_transfer_check=False,
        dry_run=False):
    ignore_instruments = [x.strip() for x in get_config().get(
        'rawingest', 'ignore_instruments').split(',')]

    logger.debug('Connecting to database with read-only access')
    db = get_omp_database()

    logger.info('Searching for observations to ingest')
    obsids = db.find_obs_for_ingestion(
        date_start, date_end,
        no_status_check=quick,
        no_transfer_check=no_transfer_check,
        ignore_instruments=ignore_instruments)
    logger.info('Found %i observations', len(obsids))

    if not dry_run:
        logger.debug('Re-connecting to database with write access')
        db = get_omp_database(write_access='jcmt')

    n_ok = n_err = 0
    for obsid in obsids:
        if _ingest_raw_observation(obsid, db=db, dry_run=dry_run):
            n_ok += 1
        else:
            n_err += 1

    logger.info('Ingestion complete: %i successful, %i errors', n_ok, n_err)

    if n_err:
        raise CommandError('Errors encountered during ingestion')


def ingest_raw_observation(obsid, dry_run=False):
    """Perform raw ingestion of an observation.

    This function connects to Sybase and then performs the raw
    data ingestion via the _ingest_raw_observation private
    function.
    """

    if not dry_run:
        db = get_omp_database(write_access='jcmt')
    else:
        db = get_omp_database()

    info = db.get_obsid_common(obsid)
    if info is None:
        raise CommandError('Observation {0} does not exist'.format(obsid))

    if not _ingest_raw_observation(obsid, db=db, dry_run=dry_run):
        raise CommandError("Ingestion failed")


def _ingest_raw_observation(obsid, db, dry_run=False):
    """Perform raw ingestion of an observation.

    This internal function requires an OMP database object with write
    access to the JCMT database.  If the ingestion is successful then
    the "last_caom_mod" timestamp for the observation will be updated
    in the COMMON table of the JCMT database.

    Returns True on success, False on failure.
    """

    logger.debug('Starting raw ingestion of OBSID %s', obsid)

    # Determine the date components which we can then use to create the
    # log directory.
    m = obsid_date.search(obsid)
    if not m:
        logger.error('Cannot parser OBSID %s to obtain date', obsid)
        raise JSAProcError('Cannot find date in OBSID {0}'.format(obsid))
    date = m.group(1)
    year = date[0:4]
    month = date[4:6]
    day = date[6:]
    logger.debug('Parsed OBSID, date: %s/%s/%s', month, day, year)

    # Prepare scratch directory.
    if not dry_run:
        scratch_dir = make_misc_scratch_dir('rawingest')
        logger.info('Working directory: %s', scratch_dir)
    else:
        scratch_dir = None

    # Prepare log directory and file name.
    if not dry_run:
        log_dir = os.path.join(get_misc_log_dir('rawingest'), year, month, day)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        logger.info('Log directory: %s', log_dir)
        log_file = os.path.join(log_dir, '{0}.log'.format(obsid))
        logger.debug('Log file: %s', log_file)
    else:
        log_file = 'DRY_RUN_MODE'

    command = [
        'jsaraw',
        '--collection', 'JCMT',
        '--obsid', obsid,
        '--verbose',
    ]

    try:
        if not dry_run:
            # Use context-manager to open a log file to store the (console)
            # output from the jsaraw program.
            with open(log_file, 'w') as log:
                logger.info('Running %s for OBSID %s', command[0], obsid)
                subprocess.check_call(
                    command,
                    shell=False,
                    cwd=scratch_dir,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=restore_signals)

                # On success (check_call didn't raise an exception), set the
                # "last_caom_mod" timestamp in the database.
                logger.info('Updating ingestion timestamp in the database')
                db.set_last_caom_mod(obsid)

        else:
            logger.info('Would have run: "%s" (DRY RUN)', ' '.join(command))

    except subprocess.CalledProcessError as e:
        logger.exception('Error during CAOM-2 ingestion')

        try:
            logger.info('Anulling ingestion timestamp in the database')
            db.set_last_caom_mod(obsid, set_null=True)
        except:
            logger.exception('Error marking ingestion date as NULL')

        return False

    except:
        logger.exception('Error marking ingestion date')

        return False

    finally:
        if not dry_run:
            logger.debug('Deleting scratch directory')
            shutil.rmtree(scratch_dir)

    return True
