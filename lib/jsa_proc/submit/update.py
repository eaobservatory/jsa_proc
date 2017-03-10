# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2015-2016 East Asian Observatory.
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
import logging

from ..error import JSAProcError, NoRowsError
from ..state import JSAProcState

UpdateAction = namedtuple(
    'UpdateAction', ('parents',))

logger = logging.getLogger(__name__)


def add_upd_del_job(
        db,
        tag, location, mode, parameters, task, priority,
        parent_jobs=None, filters=None, tilelist=None,
        allow_add=True, allow_upd=True, allow_del=True,
        description=None, dry_run=False):
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

    :return: the job ID, or None if there isn't one.
    """

    # TODO: support file-based jobs as well as than parent-based jobs.
    # (And jobs which use both?)

    # TODO: update job parameters if they have changed.

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
        oldparents = set(db.get_parents(oldjob.id))

        logger.debug(
            '%s is in already-existing job %i', description, oldjob.id)

    except NoRowsError:
        logger.debug(
            '%s is not already in database', description)

        if not parent_jobs:
            # If no parent jobs specified, do nothing (to retain old behavior).
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
                            parent_jobs=parent_jobs, filters=filters,
                            priority=priority,
                            tilelist=tilelist)
        logger.info('%s has been created', description)
        return job_id

    if not parent_jobs:
        # If no parent jobs could be found, then the job should marked
        # as deleted.
        if not allow_del:
            raise JSAProcError(
                'Cannot delete %s. It already exists '
                'in job %i and deleting is turned off!' %
                (description, oldjob.id))

        if not dry_run:
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

    # If the job was previously there, check if the job is
    # different, and rewrite if required.
    update = UpdateAction(*(False for x in UpdateAction._fields))

    # Check for update to parents list.
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

    elif dry_run:
        logger.info(
            'DRYRUN: job %i (%s) would have been'
            ' updated and status changed',
            oldjob.id, description)

    else:
        # Perform whichever updates were necessary.
        if update.parents:
            # Replace the parent jobs with updated list
            db.replace_parents(oldjob.id, parent_jobs, filters=filters)

        # Reset the job status and issue logging info.
        db.change_state(oldjob.id, JSAProcState.UNKNOWN,
                        'Parent job list has been updated;'
                        ' job reset to UNKNOWN')
        logger.info(
            'Job %i (%s) updated and reset to UNKNOWN',
            oldjob.id, description)

    return oldjob.id
