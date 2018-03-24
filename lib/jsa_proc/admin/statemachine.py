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
from jsa_proc.action.validate import validate_job
from jsa_proc.admin.directories import get_output_dir
from jsa_proc.cadc.preview import fetch_cadc_previews
from jsa_proc.db.db import Not
from jsa_proc.error import NotAtJACError, ParentNotReadyError
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


class JSAProcStateMachine:
    def __init__(self, db):
        self.db = db

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
                    except ParentNotReadyError:
                        # If the parent jobs are not ready, do nothing?
                        # (Alternative would be to set to missing, but for now
                        # fetching a job like this is an error.)
                        logger.debug('Parent jobs for %i are not ready',
                                     job.id)

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
                    # To be done in a separate process -- can be slow to
                    # send files for e-transfer if CADC access is slow, or
                    # if using a custom transfer command.
                    pass

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
