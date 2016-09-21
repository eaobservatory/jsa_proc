# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2016 East Asian Observatory.
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

import logging
import subprocess

from jsa_proc.action.etransfer_ssh import ssh_etransfer_send_output
from jsa_proc.action.validate import validate_output
from jsa_proc.admin.directories import get_output_dir, open_log_file

from jsa_proc.cadc.etransfer \
    import etransfer_check_config, etransfer_send_output
from jsa_proc.error import NoRowsError
from jsa_proc.state import JSAProcState
from jsa_proc.util import restore_signals

logger = logging.getLogger(__name__)


def transfer_poll(db):
    # Get full list of tasks.
    task_info = db.get_task_info()

    # Determine whether we are already the correct user
    # on the correct machine for e-transfer or not.
    try:
        etransfer_check_config()
        etransfer_needs_ssh = False
    except:
        etransfer_needs_ssh = True

    logger.info('Starting check for jobs to transfer')
    n_err = 0

    for job in db.find_jobs(location='JAC', state=JSAProcState.PROCESSED):
        try:
            job_task_info = task_info.get(job.task)

            if job_task_info is None:
                # Don't know if this should be e-transferred or not,
                # so do nothing for now.
                # Eventually this should probably raise an
                # error, if we wish to ensure all tasks are
                # entered in the task table.
                logger.debug('Processed job %i unchanged: ' +
                             'no etransfer option for task',
                             job.id)

            elif job_task_info.command_xfer is not None:
                # The job is transferred by a custom process.
                # Mark the job as transferring while this runs.
                db.change_state(job.id, JSAProcState.TRANSFERRING,
                                'Transferring via custom command',
                                state_prev=JSAProcState.PROCESSED)

                logger.debug('Running custom transfer command '
                             'for processed job %i',
                             job.id)
                out_dir = get_output_dir(job.id)

                try:
                    with open_log_file(job.id, 'transfer') as log:
                        subprocess.check_call(
                            [
                                job_task_info.command_xfer,
                                '--transdir', out_dir,
                            ],
                            shell=False,
                            cwd='/tmp',
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            preexec_fn=restore_signals)

                    db.change_state(job.id, JSAProcState.COMPLETE,
                                    'Custom transfer completed successfully',
                                    state_prev=JSAProcState.TRANSFERRING)

                except subprocess.CalledProcessError as e:
                    logger.exception('Custom transfer command failed '
                                     'for processed job %i',
                                     job.id)

                    db.change_state(job.id, JSAProcState.ERROR,
                                    'Custom transfer failed',
                                    state_prev=JSAProcState.TRANSFERRING)

                    n_err += 1

            elif job_task_info.etransfer is None:
                # If etransfer is set to None, don't etransfer
                # but also don't move to complete.
                logger.debug('Processed job %i unchanged: ' +
                             'task etransfer option is NULL',
                             job.id)

            elif not job_task_info.etransfer:
                # If e-transfer is not required, then the job is now
                # complete (only done if etransfer argument is False).
                if validate_output(job.id, db):
                    db.change_state(
                        job.id, JSAProcState.COMPLETE,
                        'Processed job is COMPLETE (no etransfer)',
                        state_prev=JSAProcState.PROCESSED)
                    logger.debug('Processed job %i moved to ' +
                                 'COMPLETE (no etransfer)',
                                 job.id)

            else:
                # If this task should be e-transferred, attempt to
                # add to e-transfer and move to TRANSFERRING.
                if validate_output(job.id, db):
                    # Only e-transfer via SSH if needed.
                    if etransfer_needs_ssh:
                        logger.debug('E-transferring output '
                                     'of job %i via SSH', job.id)
                        ssh_etransfer_send_output(job.id)
                    else:
                        logger.debug('E-transferring output '
                                     'of job %i directly', job.id)
                        etransfer_send_output(job.id)

        except Exception:
            logger.exception('Error while transferring job %i', job.id)
            n_err += 1

    logger.info('Done checking for jobs to transfer')

    return False if n_err else True
