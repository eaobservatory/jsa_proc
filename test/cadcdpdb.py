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

import sqlite3
from unittest import TestCase

from jsa_proc.cadc.dpdb import CADCDP
from jsa_proc.db.sqlite import JSAProcSQLiteLock

schema = """
CREATE TABLE dp_recipe (
    script_name VARCHAR(30),
    project VARCHAR(30),
    recipe_id INTEGER
);

CREATE TABLE dp_recipe_instance (
    state CHAR(1),
    parameters VARCHAR(255),
    tag VARCHAR(80),
    identity_instance_id INTEGER,
    recipe_id INTEGER
);
"""

test_data = """
INSERT INTO dp_recipe VALUES ("jcmtProcessScubaRWS.pl", "JCMT_JAC", 1);
INSERT INTO dp_recipe VALUES ("jsawrapdr", "JCMT_JAC", 2);
INSERT INTO dp_recipe VALUES ("jsawrapdr", "JCMT_JAC", 3);
INSERT INTO dp_recipe VALUES ("jsawrapdr", "JCMT_JAC", 4);
INSERT INTO dp_recipe VALUES ("chftprevwrap.py", "CFHT", 5);
INSERT INTO dp_recipe VALUES ("ukirtwrapdr", "UKIRT_JAC", 6);

INSERT INTO dp_recipe_instance VALUES ("Q",
    "-drparameters='REDUCE_SCIENCE_LEGACY'", "hpx-1001-850um", 1001, 4);
INSERT INTO dp_recipe_instance VALUES ("Q",
    "-drparameters='REDUCE_SCAN_JSA_PUBLIC'", "hpx-1002-850um", 1002, 3);
INSERT INTO dp_recipe_instance VALUES ("N",
    "-drparameters='REDUCE_SCAN_JSA_PUBLIC'", "hpx-1003-850um", 1003, 6);
INSERT INTO dp_recipe_instance VALUES ("N",
    "-drparameters='REDUCE_SCAN_JSA_PUBLIC'", "hpx-1004-850um", 1004, 2);
INSERT INTO dp_recipe_instance VALUES ("Y",
    "-drparameters='REDUCE_SCAN_JSA_PUBLIC'", "hpx-1005-850um", 1005, 1);
INSERT INTO dp_recipe_instance VALUES ("Y",
    "-drparameters='REDUCE_SCAN_JSA_PUBLIC'", "hpx-1006-850um", 1006, 2);
INSERT INTO dp_recipe_instance VALUES ("E",
    "-drparameters='REDUCE_SCIENCE_LEGACY'", "hpx-1007-850um", 1007, 4);
"""


class DummyCADCDP(CADCDP):
    def __init__(self):
        conn = sqlite3.connect(':memory:', check_same_thread=False)

        c = conn.cursor()
        c.execute('PRAGMA foreign_keys = ON')
        c.close()

        self.db = JSAProcSQLiteLock(conn)
        self.recipe = None


class CADCDPDBTestCase(TestCase):
    """Base test case class for tests using a dummy CADC DP database.
    """

    def setUp(self):
        """Prepare for testing by creating an in-memory
        SQLite database from the test schema.
        """

        self.db = DummyCADCDP()

        with self.db.db as c:
            c.executescript(schema)
            c.executescript(test_data)

    def tearDown(self):
        """Disconnect from the database by deleting the
        CADC dummy DP database object.
        """

        del self.db
