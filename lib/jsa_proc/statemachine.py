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

from jsa_proc.cadc.dpstate import CADCDPState
from jsa_proc.error import JSAProcError
from jsa_proc.job_run.validate import validate_job
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


class JSAProcStateMachine:
    def __init__(self, db, cadc):
        self.db = db
        self.cadc = cadc

    def poll(self):
        self.poll_jac_jobs(self)
        self.poll_cadc_jobs(self)

    def poll_jac_jobs(self):
        """Try to update status of all JAC jobs.

        For all jobs to be run at JAC, look at the current status
        and move on to the next status if possible.

        Returns true if there were no errors.
        """

        logger.info('Starting update of JAC job status')
        n_err = 0

        for job in self.db.find_jobs(location='JAC'):
            logger.debug('Checking state of job %i', job.id)

            try:
                if job.state == JSAProcState.UNKNOWN:
                    # Attempt to validate the job and move to QUEUED.
                    validate_job(job.id, self.db)

                elif job.state == JSAProcState.QUEUED:
                    # Fetching the data could take a long time, so leave
                    # this to a separate process.
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
                    # Add to e-transfer and move to TRANSFERRING.
                    # TODO: implement addition of output to e-transfer
                    pass

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

            except JSAProcError:
                logger.exception('Error while updating state of job %i',
                                 job.id)

                n_err += 1

        logger.info('Done updating JAC job status')

        return False if n_err else True

    def poll_cadc_jobs(self):
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
            id_[job.foreign_id] = job.id
            states[job.foreign_id] = job.state

        # Check all CADC jobs (this call can be slow).
        logger.info('Retrieving recipe instance information from CADC')
        jobs = self.cadc.get_recipe_info()

        # Loop over jobs and update status.
        logger.info('Starting update of CADC job status')
        n_err = 0

        for job in jobs:
            logger.debug('Checking state of %s, tag: %s',
                         job.id_, job.tag)

            # If we do not have this job in our database, issue a warning
            # and continue to the next job.
            if job.id_ not in id_:
                logger.warning('Foreign ID %s is unknown', job.id_)
                continue

            # Pop the job ID out of the dictionary so that we can tell if
            # any jobs were not updated.
            job_id = id_.pop(job.id_)

            try:
                # See if the state is the same, in which case we don't need
                # to do anything for this job.
                state = CADCDPState.jsaproc_state(job.state)
                if state == states[job.id_]:
                    logger.debug('Job state has not changed')
                    continue

                # Retrieve the job ID and use it to update the state.
                logger.debug('Changing state of job %i to %s', job_id, state)
                self.db.change_state(job_id, state,
                                     'State at CADC changed to {0}'.format(
                                         CADCDPState.get_name(job.state)))

            except JSAProcError:
                logger.exception('Error while updating state of job %i',
                                 job.id)

                n_err += 1

        # Issue warnings for any jobs which we did not find at CADC.
        for (ri, job_id) in id_.items():
            logger.warning('CADC recipe instance %s (job %i) '
                           'status was not received',
                           ri, job_id)

        # Log completion of the procedure.
        logger.info('Done updating CADC job status')

        return False if n_err else True
