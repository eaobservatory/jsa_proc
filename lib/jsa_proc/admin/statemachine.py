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
from jsa_proc.cadc.dpstate import CADCDPState
from jsa_proc.cadc.preview import fetch_cadc_previews
from jsa_proc.error import JSAProcError, NoRowsError, NotAtJACError
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


class JSAProcStateMachine:
    def __init__(self, db, cadc):
        self.db = db
        self.cadc = cadc

        # Get full list of tasks
        tasks = db.get_tasks()
        self.task_etransfer = {}
        for t in tasks:
            try:
                self.task_etransfer[t] = db.get_etransfer_state(t)

            except NoRowsError:
                pass

    def poll(self):
        self.poll_jac_jobs(self)
        self.poll_cadc_jobs(self)

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

        for job in self.db.find_jobs(location='JAC'):
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

                    if job.task not in self.task_etransfer:
                        # Don't know if this should be e-transferred or not,
                        # so do nothing for now.
                        pass

                    elif self.task_etransfer[job.task]:
                        # If this task should be e-transferred, attempt to
                        # add to e-transfer and move to TRANSFERRING.
                        # (Only done if etransfer argument is True.)

                        if etransfer and validate_output(job.id, self.db):

                            ssh_etransfer_send_output(job.id)

                    else:
                        # If e-transfer is not required, then the job is now complete.

                        if validate_output(job.id, self.db):
                            self.db.change_state(job.id, JSAProcState.COMPLETE,
                                                 'Processed job is COMPLETE (no etransfer)',
                                                 state_prev=JSAProcState.PROCESSED)
                            logger.debug('Processed job %i moved to COMPLETE (no etransfer)',
                                         job.id)

                elif job.state == JSAProcState.TRANSFERRING:
                    # Check e-transfer status and move to INGESTION if done.
                    # TODO: implement check of e-transfer status
                    pass

                elif job.state == JSAProcState.INGESTION:
                    # Ingestion to be a separate process?
                    pass

                elif job.state == JSAProcState.COMPLETE:
                    # Job is in a final state: do nothing.
                    pass

                elif job.state == JSAProcState.ERROR:
                    # Job is in a final state: do nothing.
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

    def poll_cadc_jobs(self, fetch_previews=True):
        """Update status of all CADC jobs.

        Fetches the status of all relevant jobs from CADC.  Then loops
        through them and updates the status in our database.

        Returns true if there were no errors.
        """

        # Made a dictionary mapping tags to job identifiers.
        logger.info('Preparing list of job IDs for CADC jobs')
        id_ = {}
        states = {}
        for job in self.db.find_jobs(location='CADC'):
            recipe_instance = job.id
            id_[job.foreign_id] = recipe_instance
            states[job.foreign_id] = job.state

        # Check all CADC jobs (this call can be slow).
        logger.info('Retrieving recipe instance information from CADC')
        jobs = self.cadc.get_recipe_info()

        # Loop over jobs and update status.
        logger.info('Starting update of CADC job status')
        n_err = 0

        for job in jobs:
            recipe_instance = job.id
            logger.debug('Checking state of %s, tag: %s',
                         recipe_instance, job.tag)

            # If we do not have this job in our database, issue a warning
            # and continue to the next job.
            if recipe_instance not in id_:
                logger.warning('Foreign ID %s is unknown', recipe_instance)
                continue

            # Pop the job ID out of the dictionary so that we can tell if
            # any jobs were not updated.
            job_id = id_.pop(recipe_instance)

            try:
                # See if the state is the same, in which case we don't need
                # to do anything for this job.
                state = CADCDPState.jsaproc_state(job.state)
                if state == states[recipe_instance]:
                    logger.debug('Job state has not changed')
                    continue

                # Retrieve the job ID and use it to update the state.
                logger.debug('Changing state of job %i to %s', job_id, state)
                self.db.change_state(job_id, state,
                                     'State at CADC changed to {0}'.format(
                                         CADCDPState.get_name(job.state)))

                # Is the (new) state COMPLETE?  If so, fetch the list of
                # output files.
                if job.state == CADCDPState.COMPLETE:
                    logger.debug('Job is complete: fetching output files.')
                    output = self.cadc.get_recipe_output_files(recipe_instance)

                    # Ensure output filenames are lower case.
                    output = [f.lower() for f in output]

                    logger.debug('Storing list of output files.')
                    self.db.set_output_files(job_id, output)

                    if fetch_previews:
                        logger.debug('Attempting to download preview files.')
                        fetch_cadc_previews(output, get_output_dir(job_id))

            except Exception:
                logger.exception('Error while updating state of job %i',
                                 recipe_instance)

                n_err += 1

        # Issue warnings for any jobs which we did not find at CADC.
        for (ri, job_id) in id_.items():
            logger.warning('CADC recipe instance %s (job %i) '
                           'status was not received',
                           ri, job_id)

        # Log completion of the procedure.
        logger.info('Done updating CADC job status')

        return False if n_err else True
