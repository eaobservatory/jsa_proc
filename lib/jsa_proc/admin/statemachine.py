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

import logging

from jsa_proc.action.datafile_handling \
    import get_jac_input_data, write_input_list, check_data_already_present
from jsa_proc.action.etransfer_ssh import ssh_etransfer_send_output
from jsa_proc.action.validate import validate_job, validate_output
from jsa_proc.admin.directories import get_output_dir
from jsa_proc.cadc.etransfer \
    import etransfer_check_config, etransfer_send_output
from jsa_proc.cadc.preview import fetch_cadc_previews
from jsa_proc.db.db import Not
from jsa_proc.error import JSAProcError, NoRowsError, NotAtJACError
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


class JSAProcStateMachine:
    def __init__(self, db, cadc):
        self.db = db
        self.cadc = cadc

        # Get full list of tasks, create dictionary with etransfer
        # state as value and task name as key.
        tasks = db.get_tasks()
        self.task_info = {}
        for t in tasks:
            try:
                self.task_info[t] = db.get_task_info(t)

            except NoRowsError:
                pass

        # Determine whether we are already the correct user
        # on the correct machine for e-transfer or not.
        try:
            etransfer_check_config()
            self.etransfer_needs_ssh = False
        except:
            self.etransfer_needs_ssh = True

    def poll(self):
        self.poll_jac_jobs(self)

    def poll_jac_jobs(self, etransfer=True):
        """Try to update status of all JAC jobs.

        For all jobs to be run at JAC, look at the current status
        and move on to the next status if possible.

        Arguments:
            etransfer: True to enable e-transfer steps.

        Returns true if there were no errors.
        """

        logger.info('Starting update of JAC job status')
        n_err = 0

        for job in self.db.find_jobs(location='JAC',
                                     state=Not(JSAProcState.STATE_FINAL)):
            logger.debug('Checking state of job %i', job.id)

            try:
                if job.state == JSAProcState.UNKNOWN:
                    # Attempt to validate the job and move to QUEUED.
                    validate_job(job.id, db=self.db)

                elif job.state == JSAProcState.QUEUED:
                    # Check if all data are at JAC:
                    try:
                        inputs = check_data_already_present(job.id, self.db)
                        thelist = write_input_list(job.id, inputs)
                        self.db.change_state(job.id, JSAProcState.WAITING,
                                             'All files found at JAC',
                                             state_prev=JSAProcState.QUEUED)
                        logger.debug('Job %i has found data and been'
                                     'moved to WAITING', job.id)
                    except NotAtJACError:
                        # If the data are not present, change the state to
                        # MISSING so that a fetching process will
                        # initiate a download.
                        self.db.change_state(job.id, JSAProcState.MISSING,
                                             'Input files are not at JAC',
                                             state_prev=JSAProcState.QUEUED)
                        logger.debug('Input files for %i are not at JAC',
                                     job.id)

                    # Fetching the data could take a long time, so leave
                    # this to a separate process.
                    pass

                elif job.state == JSAProcState.MISSING:
                    # Wait for a separate process to fetch the input files.
                    pass

                elif job.state == JSAProcState.FETCHING:
                    # A separate process is fetching: do nothing.
                    # Add a time-out here?
                    pass

                elif job.state == JSAProcState.WAITING:
                    # Wait for a separate process to run the job.
                    pass

                elif job.state == JSAProcState.RUNNING:
                    # A separate process is running the job: do nothing.
                    # Add a time-out here?
                    pass

                elif job.state == JSAProcState.PROCESSED:
                    # Check if job's task has e-transfer state?
                    task_info = self.task_info.get(job.task)

                    if task_info is None:
                        # Don't know if this should be e-transferred or not,
                        # so do nothing for now.
                        # Eventually this should probably raise an
                        # error, if we wish to ensure all tasks are
                        # entered in the task table.
                        logger.debug('Processed job %i unchanged: ' +
                                     'no etransfer option for task',
                                     job.id)

                    elif task_info.etransfer is None:
                        # If etransfer is set to None, don't etransfer
                        # but also don't move to complete.
                        logger.debug('Processed job %i unchanged: ' +
                                     'task etransfer option is NULL',
                                     job.id)

                    elif not task_info.etransfer:
                        # If e-transfer is not required, then the job is now
                        #  complete (only done if etransfer argument is False).
                        if validate_output(job.id, self.db):
                            self.db.change_state(
                                job.id, JSAProcState.COMPLETE,
                                'Processed job is COMPLETE (no etransfer)',
                                state_prev=JSAProcState.PROCESSED)
                            logger.debug('Processed job %i moved to ' +
                                         'COMPLETE (no etransfer)',
                                         job.id)

                    else:
                        # If this task should be e-transferred, attempt to
                        # add to e-transfer and move to TRANSFERRING.
                        # (Only done if etransfer argument evaluates to True.)
                        if etransfer and validate_output(job.id, self.db):
                            # Only e-transfer via SSH if needed.
                            if self.etransfer_needs_ssh:
                                logger.debug('E-transferring output '
                                        'of job %i via SSH', job.id)
                                ssh_etransfer_send_output(job.id)
                            else:
                                logger.debug('E-transferring output '
                                        'of job %i directly', job.id)
                                etransfer_send_output(job.id)


                elif job.state == JSAProcState.TRANSFERRING:
                    # Check e-transfer status and move to INGESTION if done.
                    # TODO: implement check of e-transfer status
                    pass

                elif job.state == JSAProcState.INGEST_QUEUE:
                    # Wait for another process to fetch the data.
                    pass

                elif job.state == JSAProcState.INGEST_FETCH:
                    # Another process is fetching the data.
                    pass

                elif job.state == JSAProcState.INGESTION:
                    # Wait for ingestion by a separate process.
                    pass

                elif job.state == JSAProcState.INGESTING:
                    # A separate processes is performing an ingestion.
                    pass

                else:
                    logger.error('Job %i is in unknown state %s',
                                 job.id, job.state)

            except Exception:
                logger.exception('Error while updating state of job %i',
                                 job.id)

                n_err += 1

        logger.info('Done updating JAC job status')

        return False if n_err else True
