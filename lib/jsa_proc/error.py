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
Errors defined for the JSA Processing classes.

"""

class JSAProcError(Exception):
    """
    Base Class to handle errors in this module.
    """
    pass


class JSAProcDBError(JSAProcError):
    """
    Class for errors from the JSAProc Database.

    """

    def __init__(self, table, query, *args):
        """
        Takes in strings for the table name and the query.
        """
        message = 'Database error from  table %s and query %s'%(table,query)

        Exception.__init__(self, message, *args)


class NoRowsError(JSAProcDBError):
    """
    Error indicating that no rows were found.

    Must be provided with the name of the table it was trying to
    access and a human readable indication of what it was looking for
    (e.g. either the SQL query, or a simple string that makes it clear).

    """

    def __init__(self, table, query, *args):
        """
        Takes in strings for the table name and the query.
        """

        message = 'No rows found in table %s, matching "%s"'%(table,query)

        Exception.__init__(self, message, *args)

class ExcessRowsError(JSAProcDBError):
    """
    Error indicating that more than the expected number of rows was found.
    """

    def __init__(self, table, query, *args):
        """
        Takes in strings for the table name and the query.
        """

        message = 'More than the expected number of rows found in table %s, matching "%s"'%(table, query)
        Exception.__init__(self, message, *args)

class NotAtJACError(JSAProcError):
    """
    Error indicating something is not at the JAC.
    """
    def __init__(self, something, *args):
        """
        Something must be a string
        """
        message = '%s was not found at the JAC' % (something)
        Exception.__init__(self, message, *args)
