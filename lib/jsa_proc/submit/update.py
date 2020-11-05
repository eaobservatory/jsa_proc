# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2015-2017 East Asian Observatory.
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

from collections import namedtuple
from datetime import date, datetime
import logging

from ..error import JSAProcError, NoRowsError
from ..state import JSAProcState

# Type representing the different update actions we may perform.
UpdateAction = namedtuple(
    'UpdateAction',
    ('input_files', 'parents', 'mode', 'parameters', 'tilelist', 'obsidss'))

# Update actions which do not need to trigger resetting a job.
UPDATE_NO_RESET = ('tilelist', 'obsidss')

logger = logging.getLogger(__name__)


def add_upd_del_job(
        db,
        tag, location, mode, parameters, task, priority,
        parent_jobs=None, filters=None, tilelist=None,
        input_file_names=None, obsidss=None,
        allow_add=True, allow_upd=True, allow_del=True,
        description=None, dry_run=False, force=False):

    """
    General function for updating jobs in the processing system database.

    This function will add a job if it does not exist, update it if its
    inputs changed, or delete it if it no longer has any inputs
    --- i.e. perform an upsert (plus delete) operation.

    :param allow_add: allow addition of new jobs.
    :param allow_upd: allow job updates.
    :param allow_del: allow deletion of old jobs.

    :param description: description of the job to include in log messages.
    :param dry_run: no database modifications performed if true.

    :param force: allow updates even when the job is in an active state.

    :return: the job ID, or None if there isn't one.
    """

    job_is_empty = not (parent_jobs or input_file_names)

    if description is None:
        description = '{} job tagged {}'.format(task, tag)

    # Check if task already exists. Print a warning if it has not
    # yet been added to task table.
    try:
        task_info = db.get_task_info(task=task)
    except NoRowsError:
        logger.warning('Task %s is not in the database', task)

    # Check if job already exists in database.
    try:
        oldjob = db.get_job(tag=tag)

        logger.debug(
            '%s is in already-existing job %i', description, oldjob.id)

    except NoRowsError:
        logger.debug(
            '%s is not already in database', description)

        if job_is_empty:
            # If no input files / parent jobs specified, do nothing (to retain
            # old behavior).
            return None

        # If the job is new, add the job to the database with the list
        # of parent jobs.
        if not allow_add:
            raise JSAProcError(
                'Cannot add %s. It doesn\'t already exist '
                'and adding is turned off!' %
                (description,))

        if dry_run:
            logger.info('DRYRUN: %s would have been created', description)
            return None

        job_id = db.add_job(tag, location, mode, parameters, task,
                            input_file_names=input_file_names,
                            parent_jobs=parent_jobs, filters=filters,
                            priority=priority,
                            obsidss=obsidss, tilelist=tilelist)
        logger.info('%s has been created', description)
        return job_id

    if job_is_empty:
        # If no files / parent jobs could be found, then the job should marked
        # as deleted.
        if not allow_del:
            raise JSAProcError(
                'Cannot delete %s. It already exists '
                'in job %i and deleting is turned off!' %
                (description, oldjob.id))

        if JSAProcState.get_info(oldjob.state).active and not force:
            raise JSAProcError(
                'Cannot delete %s. It already exists '
                'in job %i which is currently active!' %
                (description, oldjob.id))

        if oldjob.state == JSAProcState.DELETED:
            logger.info(
                'Job %i for %s is already marked as deleted',
                oldjob.id, description)

        elif not dry_run:
            db.change_state(
                oldjob.id, JSAProcState.DELETED,
                'No valid parent jobs found for %s;'
                ' marking job as DELETED' % (description,))
            logger.info(
                'Job %i for %s marked as deleted'
                ' (no valid input jobs)',
                oldjob.id, description)
        else:
            logger.info(
                'DRYRUN: job %i for %s would be marked as DELETED',
                oldjob.id, description)

        return oldjob.id

    # Retrieve old input files / parents in separate try ... except blocks
    # in case any database methods raises NoRowsError (we still want to
    # get the other input sets).
    try:
        oldparents = set(db.get_parents(oldjob.id))
    except NoRowsError:
        oldparents = set()

    try:
        old_input_files = set(db.get_input_files(oldjob.id))
    except NoRowsError:
        old_input_files = set()

    # If the job was previously there, check if the job is
    # different, and rewrite if required.
    update = UpdateAction(*(False for x in UpdateAction._fields))

    # Check for change to input file list.
    if input_file_names is None:
        input_file_names_set = set()
    else:
        input_file_names_set = set(input_file_names)

    if input_file_names_set != old_input_files:
        update = update._replace(input_files=True)

        logger.debug(
            'Input files list for job %i has changed', oldjob.id)
        for file_ in old_input_files.difference(input_file_names_set):
            logger.debug('Input file removed: %s', file_)
        for file_ in input_file_names_set.difference(old_input_files):
            logger.debug('Input file added: %s', file_)

    # Check for update to parents list.
    if parent_jobs is None:
        parents = set()
    else:
        parents = set(zip(parent_jobs, filters))

    if parents != oldparents:
        update = update._replace(parents=True)

        logger.debug(
            'Parent/filter list for job %i has changed from '
            'previous state', oldjob.id)

        # Get lists of added and removed jobs for logging information.
        added_jobs = parents.difference(oldparents)
        removed_jobs = oldparents.difference(parents)
        logger.debug(
            'Parent jobs %s have been removed from coadd.',
            str(removed_jobs))
        logger.debug(
            'Parent jobs %s have been added to coadd.',
            str(added_jobs))

    # Check for change to mode.
    if mode != oldjob.mode:
        update = update._replace(mode=True)

        logger.debug(
            'Mode for job %i has changed from %s to %s',
            oldjob.id, oldjob.mode, mode)

    # Check for change to parameters.
    if parameters != oldjob.parameters:
        update = update._replace(parameters=True)

        logger.debug(
            'Parameters for job %i have changed from "%s" to "%s"',
            oldjob.id, oldjob.parameters, parameters)

    # Check for a change to tilelist, but only if it was specified.
    if tilelist is not None:
        oldtiles = db.get_tilelist(oldjob.id)
        if set(tilelist) != oldtiles:
            update = update._replace(tilelist=True)

            logger.debug(
                'Tile list for job %i has changed from %s to %s',
                oldjob.id, str(sorted(oldtiles)), str(sorted(tilelist)))

    # Check for a change to obsinfo, but only if it was specified.
    if obsidss is not None:
        # Get existing obsid_subsysnrs:
        oldobsidss = [x.obsidss for x in db.get_obs_info(oldjob.id)]

        obsidss_changed = False
        # Check if different
        if set(obsidss) != set(oldobsidss):
            obsidss_changed = True


        if obsidss_changed:
            update = update._replace(obsidss=True)
            logger.debug('Obsid subsysnrs for job %i has changed', oldjob.id)

    if not any(update):
        logger.debug(
            'Settings for job %i (%s) are unchanged',
            oldjob.id, description)

        # TODO: Check if last changed time of each parent job is < last
        # processed time of old job If nothing has changed, check
        # if the job needs redoing (if any of its parent jobs have
        # been redone since last time)

    elif not allow_upd:
        raise JSAProcError(
            'Cannot update %s. It already exists '
            'in job %i and updating is turned off!' %
            (description, oldjob.id))

    elif JSAProcState.get_info(oldjob.state).active and not force:
        raise JSAProcError(
            'Cannot update %s. It already exists '
            'in job %i which is currently active!' %
            (description, oldjob.id))

    elif dry_run:
        logger.info(
            'DRYRUN: job %i (%s) would have been'
            ' updated and status changed',
            oldjob.id, description)

    else:
        # Perform whichever updates were necessary.
        if update.input_files:
            db.set_input_files(
                oldjob.id,
                ([] if input_file_names is None else input_file_names))

        if update.parents:
            # Replace the parent jobs with updated list
            db.replace_parents(
                oldjob.id,
                ([] if parent_jobs is None else parent_jobs),
                filters=([] if filters is None else filters))

        if update.mode:
            db.set_mode(oldjob.id, mode)

        if update.parameters:
            db.set_parameters(oldjob.id, parameters)

        if update.tilelist:
            db.set_tilelist(oldjob.id, tilelist)

        if update.obsidss:
            db.set_obsidss(oldjob.id, obsidss)

        # Reset the job status and issue logging info if a significant
        # change happened.
        if any(update._replace(**{x: False for x in UPDATE_NO_RESET})):
            db.change_state(oldjob.id, JSAProcState.UNKNOWN,
                            'Job has been updated; reset to UNKNOWN')
            logger.info(
                'Job %i (%s) updated and reset to UNKNOWN',
                oldjob.id, description)
        else:
            logger.info(
                'Job %i (%s) updated but does not need to be reset',
                oldjob.id, description)

    return oldjob.id


