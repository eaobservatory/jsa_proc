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

from jsa_proc.cadc.dpdb import CADCDPInfo
from jsa_proc.cadc.initial_import import import_from_cadcdp
from jsa_proc.state import JSAProcState

from .db import DBTestCase
from .dummycadcdp import DummyCADCDP


class CADCImportTestCase(DBTestCase):
    def test_dummy_cadc(self):
        """Check that the dummy CADC DP object itself works!"""

        ri5 = CADCDPInfo(5, 'S', 'tag-5', 'PARAM')
        ri6 = CADCDPInfo(6, 'N', 'tag-6', 'PARAM')

        cadc = DummyCADCDP([
            (ri5, ['file1', 'file2']),
            (ri6, ['file3', 'file4']),
        ])

        self.assertEqual(cadc.get_recipe_info(), [ri5, ri6])
        self.assertEqual(cadc.get_recipe_input_files(6), ['file3', 'file4'])
        self.assertEqual(cadc.get_recipe_input_files(7), [])

    def test_import(self):
        """Test whether the initial import function works."""

        param1 = '-mode="obs" -drparameters="RECIPE_NAME"'
        param2 = '-mode="night" -drparameters="ANOTHER_RECIPE_NAME"'

        cadc = DummyCADCDP([
            (CADCDPInfo(11, 'S', 'tag-11', param1), ['f_11_01']),
            (CADCDPInfo(12, 'N', 'tag-12', param1), ['f_12_01', 'f_12_02']),
            (CADCDPInfo(13, 'Q', 'tag-13', param2), ['f_13_01']),
            (CADCDPInfo(14, 'Y', 'tag-14', param2), ['f_14_01', 'f_14_02']),
        ])

        # Check that dry-run mode really does nothing.
        self.assertTrue(import_from_cadcdp(True, self.db, cadc))
        self.assertEqual(self.db.find_jobs(), [])

        # Now perform real import.
        self.assertTrue(import_from_cadcdp(False, self.db, cadc))

        # See if we got all 4 recipe instances.
        jobs = self.db.find_jobs()
        self.assertEqual(len(jobs), 4)

        # Organise data fetched from the database.
        tag = {}
        for job in jobs:
            tag[job.tag] = (self.db.get_job(id_=job.id),
                            sorted(self.db.get_input_files(job.id)))

        # Check the retrieved information.
        self.assertIn('tag-11', tag)
        self.assertEqual(tag['tag-11'][0].location, 'CADC')
        self.assertEqual(tag['tag-11'][0].foreign_id, '11')
        self.assertEqual(tag['tag-11'][0].state, JSAProcState.RUNNING)
        self.assertEqual(tag['tag-11'][0].mode, 'obs')
        self.assertEqual(tag['tag-11'][0].parameters, 'RECIPE_NAME')
        self.assertEqual(tag['tag-11'][1], ['f_11_01'])

        self.assertIn('tag-12', tag)
        self.assertEqual(tag['tag-12'][0].location, 'CADC')
        self.assertEqual(tag['tag-12'][0].foreign_id, '12')
        self.assertEqual(tag['tag-12'][0].state, JSAProcState.RUNNING)
        self.assertEqual(tag['tag-12'][0].mode, 'obs')
        self.assertEqual(tag['tag-12'][0].parameters, 'RECIPE_NAME')
        self.assertEqual(tag['tag-12'][1], ['f_12_01', 'f_12_02'])

        self.assertIn('tag-13', tag)
        self.assertEqual(tag['tag-13'][0].location, 'CADC')
        self.assertEqual(tag['tag-13'][0].foreign_id, '13')
        self.assertEqual(tag['tag-13'][0].state, JSAProcState.QUEUED)
        self.assertEqual(tag['tag-13'][0].mode, 'night')
        self.assertEqual(tag['tag-13'][0].parameters, 'ANOTHER_RECIPE_NAME')
        self.assertEqual(tag['tag-13'][1], ['f_13_01'])

        self.assertIn('tag-14', tag)
        self.assertEqual(tag['tag-14'][0].location, 'CADC')
        self.assertEqual(tag['tag-14'][0].foreign_id, '14')
        self.assertEqual(tag['tag-14'][0].state, JSAProcState.COMPLETE)
        self.assertEqual(tag['tag-14'][0].mode, 'night')
        self.assertEqual(tag['tag-14'][0].parameters, 'ANOTHER_RECIPE_NAME')
        self.assertEqual(tag['tag-14'][1], ['f_14_01', 'f_14_02'])
