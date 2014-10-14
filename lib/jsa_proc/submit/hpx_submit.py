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

from jsa_proc.config import get_database
from jsa_proc.db.db import Not
from jsa_proc.error import JSAProcError, NoRowsError
from jsa_proc.omp_state import OMPState
from jsa_proc.qa_state import JSAQAState
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


def generate_hpx_coadd_task(inputtask):
    """
    Generate name of task for hpx coadds from
    an already existing task name that
    covers all the files being coadded.
    """
    return inputtask + '-coadd'

def generate_hpx_coadd_tag(tile, task):
    """
    Generate the unique tag for an hpx coadd.

    Parameters
    ---------
    tile, int, required.
    Integer tile number.

    task, string, required.
    task for coadding job.

    Return
    -------
    tag, string.
    unique string for this tile number and task combination.
    """
    tag = task + '-' + str(tile)
    return tag


def create_hpx_filter(tile, task):
    # Get subsys
    subsys = task.split('-')[2]
    tilestring = str(tile).rjust(6, '0')
    hpx_filter = "jcmts[0-9]{8}_[0-9]{5}_" + subsys + "_healpix" + tilestring + "_obs_[0-9]{3}.fits"
    return  hpx_filter

def submit_one_coadd_job(tile, parenttask, mode, parameters, location,
                         exclude_pointing_jobs=False,
                         science_obs_only=False,
                         never_update=False,
                         dryrun=True, priority=0):
    """
    Submit a single coadd job.

    """
    # Generate tag, task name, and filter.
    coadd_task = generate_hpx_coadd_task(parenttask)
    tag = generate_hpx_coadd_tag(tile, coadd_task)
    filt = create_hpx_filter(tile, parenttask)

    #TODO:Check if task already exists. Print a warning if it has not
    #yet been added to task table.

    # Check if job already exists in database.
    db = get_database()
    try:
        oldjob = db.get_job(tag=tag)
        oldparents = set(db.get_parents(oldjob.id))

        logger.debug('Coadd for tile %i is in already-existing job %i' % (tile, oldjob.id))
        if never_update:
            raise JSAProcError('Cannot add coadd for tile %i task %s. It  already exists in job %i and updating is turned off!'
                               % (tile, parenttask, oldjob.id))
    except NoRowsError:
        logger.debug('Coadd for tile %i task %s is not already in database' % (tile, parenttask))
        oldjob = None
        oldparents=None
    # Check what current parent values should be.
    parent_jobs = get_parents(tile, parenttask,
                                  exclude_pointing_jobs=exclude_pointing_jobs,
                                  science_obs_only=science_obs_only)

    parents = zip(parent_jobs, [filt]*len(parent_jobs))


    # If the job was previously there, check if the job list/filters are different, and rewrite if required.
    if oldparents:
        oldspars, oldfilts = zip(*oldparents)
        pars, filts = zip(*parents)
        if set(pars) != set(oldspars) or set(oldfilts) != set(filts):
            logger.info('Parent/filter list for job %i has changed from previous state' % oldjob.id)

            # Get lists of added and removed jobs.
            added_jobs = set(parents).difference(oldparents)
            removed_jobs = set(oldparents).difference(parents)
            logger.debug('Parent jobs %s have been removed from coadd.' % str(removed_jobs))
            logger.debug('Parent jobs %s have been added to coadd.' % str(added_jobs))

            # Replace the parent jobs with updated list
            pars, filts = zip(*parents)
            if not dryrun:
                db.replace_parents(oldjob.id, pars, filt=filts)
                db.change_state(oldjob.id, JSAProcState.QUEUED,
                                'Parent job list has been updated; job reset to QUEUED')
                job_id = oldjob.id
                logger.info('Coadd job %i updated and reset to QUEUED' % job_id)
            else:
                logger.info('DRYRUN: coadd for tile %i has not been updated nor status changed' % tile)
                job_id = 0
        else:
            logger.info('Parent/filter list for job %i is unchanged' % oldjob.id)
            job_id = oldjob.id
            # Check if last changed time of each parent job is < last
            # processed time of old job If nothing has changed, check
            # if the job needs redoing( if any of its parent jobs have
            # been redone since last time)
            pass
    else:
        # If the job is new, add the job to the database with the list
        # of parent jobs.
        pars, filts = zip(*parents)
        if not dryrun:
            job_id = db.add_job(tag, location, mode, parameters, coadd_task,
                                parent_jobs=pars, filters=filts, priority=priority,
                                tilelist=[tile])
        else:
            logger.info('DRYRUN: coadding job for tile %i has not been created'
                        % tile)
            job_id=0
    return job_id

def get_parents(tile, parenttask, exclude_pointing_jobs=False,
                science_obs_only=False):

    """
    get parent jobs for the requested tile and coaddtask,
    using the parettask to look for jobs.
    required parameters:

    Raises a  JSAProcError if there are no parent jobs that fit.
    tile (int)
    Tile number to perform coadd on.

    parenttask (string)
    input task name to look for jobs for.

    """
    # Find all jobs from the parent task which include the requested tile and
    #      1) Have a JSAQA State that is not BAD or INVALID
    #      2) Have not been marked as deleted.
    logger.debug('Finding all jobs in task %s that fall on tile %i' % (parenttask, tile))

    db = get_database()
    qa_state = [JSAQAState.GOOD,
                JSAQAState.QUESTIONABLE,
                JSAQAState.UNKNOWN]

    obsquery={'omp_status': Not(list(OMPState.STATE_NO_COADD))}
    if science_obs_only:
        obsquery['obstype'] = {'science'}

    # Get the parent jobs.
    parentjobs = db.find_jobs(tiles=[tile],
                              task=parenttask,
                              qa_state=qa_state,
                              state=Not([JSAProcState.DELETED]),
                              obsquery=obsquery)

    parentjobs = [p.id for p in parentjobs]

    # Do some other queries to give the user info about what is not being included.
    excludedjobs_ompstatus = db.find_jobs(tiles=[tile],
                                          task=parenttask,
                                          qa_state=qa_state,
                                          state=Not([JSAProcState.DELETED]),
                                          obsquery={'omp_status':OMPState.STATE_NO_COADD})
    if science_obs_only or exclude_pointing_jobs:
        excludedjobs_pointings = db.find_jobs(tiles=[tile],
                                              task=parenttask,
                                              qa_state=qa_state,
                                              state=Not([JSAProcState.DELETED]),
                                              obsquery={'obstype':'pointing',
                                                        'omp_status':Not(list(OMPState.STATE_NO_COADD))})

        # If it was requested to exclude entirely any job containing a pointing:
        if exclude_pointing_jobs and len(excludedjobs_pointings)> 0:
            logger.info('Tile %i contains pointing obs.' % tile)
            raise JSAProcError('Pointings fall on this tile.')

    # Log information about which tasks where excluded.
    # TODO: check what logger level is being used before going through for loops.
    logger.info('%i jobs in task %s fall on tile %i with appropriate QA States, OMP States and obstype states'
                 % (len(parentjobs),parenttask, tile))

    if len(excludedjobs_ompstatus) > 0:
        logger.info('%i jobs were excluded due to wrong OMP status' % (len(excludedjobs_ompstatus)))
        for i in excludedjobs_ompstatus:
            omp_status = db.get_obs_info(i.id)[0].omp_status
            logger.debug('Job %i NOT INCLUDED (omp status of %s)' % (i.id, OMPState.get_name(omp_status)))

    if science_obs_only:
        if len(excludedjobs_pointings) > 0:
            logger.info('%i additional jobs were excluded as pointings' % (len(excludedjobs_pointings)))
            for i in excludedjobs_pointings:
                logger.debug('Job %i NOT INCLUDED (pointing)' % (i.id))

    if len(parentjobs) == 0:
        logger.info('Tile %i has no acceptable parent jobs' % tile)
        raise JSAProcError('No acceptable observations.')

    # Return the parent jobs
    return parentjobs

