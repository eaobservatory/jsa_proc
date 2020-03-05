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

from unittest import TestCase

from jsa_proc.db.sqlite import JSAProcSQLite
import os
from datetime import datetime
schema = None


def create_dummy_database():
    """Create an in-memory SQLite database from the schema."""

    global schema

    if schema is None:
        with open('doc/schema.sql') as f:
            schema = f.read()

    db = JSAProcSQLite(':memory:')

    with db.db as c:
        c.executescript(schema)


    # Can't have 2 databases in memory
    with open('doc/test-jcmt-schema.sql') as f:
        jcmtschema = f.read()

    db2 = JSAProcSQLite('tmpjcmtfile.db', file_already_exists=False)
    with db2.db as c:
        c.executescript(jcmtschema)

        # Insert test data into database.
        info_1 = {'obsid': '1', 'obsidss': '1-1', 'utdate': 20140101,
                  'obsnum': 1, 'instrume': 'F', 'backend': 'B',
                  'subsys': '1', 'survey': 'GBS', 'project': 'G01',
                  'date_obs': datetime(2014, 1, 1, 10, 0, 0), 'filename': 'test1'}

        info_2 = info_1.copy()
        info_2.update(obsidss='1-2', subsys=2, filename='test2')

        info_3 = info_1.copy()
        info_3.update(obsid='2', obsidss='2-3', survey='DDS', project='D01', filename='test3')

        info_4 = info_1.copy()
        info_4.update(obsid='3',obsidss='3-4',survey=None, project='XX01', filename='test4')

        info_5 = info_4.copy()
        info_5.update(obsid='4',obsidss='4-5',project='JCMTCAL', filename='test5',)

        info_6 = info_4.copy()
        info_6.update(obsid='5', obsidss='5-6', project='CAL', filename='test6',)

        info_7 = info_1.copy()
        info_7.update(obsid='6', obsidss='6-7', project='JCMTCAL', filename='test7')
        for obs in (info_1, info_2, info_3, info_4, info_5, info_6, info_7):
            c.execute('INSERT INTO FILES (file_id, obsid, subsysnr, nsubscan, obsid_subsysnr) ' +
                      'VALUES (%s, %s, %s, 100, %s)',
                      (obs['filename'], obs['obsid'], obs['subsys'], obs['obsidss']))
            c.execute('INSERT INTO COMMON (obsid, utdate, obsnum, instrume, backend, survey, project, date_obs) ' +
                      'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                      (obs['obsid'], obs['utdate'], obs['obsnum'], obs['instrume'],
                       obs['backend'], obs['survey'], obs['project'], obs['date_obs']))


    with db.db as c:
        c.execute('ATTACH DATABASE "tmpjcmtfile.db" AS jcmt')

    with open('doc/test-omp-schema.sql') as f:
        ompschema = f.read()
    db3 = JSAProcSQLite('tmpompfile.db', file_already_exists=False)
    with db3.db as c:
        c.executescript(ompschema)
    print('attaching omp database')
    with db.db as c:
        c.execute('ATTACH DATABASE "tmpompfile.db" AS omp')


    del(db3)
    del(db2)
    return db


class DBTestCase(TestCase):
    """Base test case class for tests using the database.
    """

    def setUp(self):
        """Prepare for testing by creating an in-memory
        SQLite database from the schema file.
        """

        self.db = create_dummy_database()



    def tearDown(self):
        """Disconnect from the database by deleting the
        JSAProcDB object.
        """


        try:
            os.remove('tmpjcmtfile.db')
        except:
            warning('Could not remove tmpjcmtfile.db')
        try:
            os.remove('tmpompfile.db')
        except:
            warning('Could not remove tmpompfile.db')

        del(self.db)
        #del self.db2
        #del self.db3
