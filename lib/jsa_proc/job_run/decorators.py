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
Decorators for functions related to running jobs.
"""

import logging

from jsa_proc.config import get_database
from jsa_proc.state import JSAProcState

logger = logging.getLogger(__name__)


class ErrorDecorator(object):
    """
    This decorator wraps a function so that if an error is raised
    during th fucntion, the state of the job that function is called
    on will be changed to ERROR and the error message will be written
    in the log.


    Can only be used around functions which have an integer job_id as
    the first (non-keyword) argument.

    Will connect to the database in config, unless the function
    called has a keyword argument db, in which case it will assume
    that is a db instance it can uses instead.
    """

    def __init__(self, function):
        self.function = function

    def __call__(self, *args, **kwargs):
        try:
            return self.function(*args, **kwargs)
        except Exception as theexception:
            logger.exception('Error caught running function %s',
                             function.__name__)

            if 'db' in kwargs and kwargs['db'] is not None:
                db = kwargs['db']
            else:
                db = get_database()
            db.change_state(args[0], JSAProcState.ERROR,
                            'Error message and args: ' +
                            ' '.join([str(i) for i in theexception.args]))
            raise
