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

from ConfigParser import SafeConfigParser

from jsa_proc.db.mysql import JSAProcMySQL

config_file = 'etc/jsa_proc.ini'
config = None

database = None

def get_config():
    """Read the configuration file.

    Returns a SafeConfigParser object.
    """

    global config

    if config is None:
        config = SafeConfigParser()

        config.read(config_file)

    return config


def get_database():
    """Construct a database access object.

    In principal this could be configured, but for now it
    is always an object of the MySQL access class.  A single
    object is constructed and returned to all callers.
    """

    global database

    if database is None:
        database = JSAProcMySQL(get_config())

    return database
