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

import os
from unittest import TestCase

import jsa_proc.config
from jsa_proc.config import get_config
from jsa_proc.error import JSAProcError


class ConfigTestCase(TestCase):
    def setUp(self):
        # Unload existing configuation.
        jsa_proc.config.config = None

        # Unset home directory variable.
        if 'JSA_PROC_DIR' in os.environ:
            del os.environ['JSA_PROC_DIR']

    def tearDown(self):
        # Same clean-up as setUp().
        self.setUp()

    def test_database_config(self):
        config = get_config()

        self.assertTrue(config.has_section('database'))
        self.assertTrue(config.has_option('database', 'host'))
        self.assertTrue(config.has_option('database', 'database'))
        self.assertTrue(config.has_option('database', 'user'))
        self.assertTrue(config.has_option('database', 'password'))

    def test_cadc_config(self):
        config = get_config()

        self.assertTrue(config.has_section('cadc'))
        self.assertTrue(config.has_option('cadc', 'username'))
        self.assertTrue(config.has_option('cadc', 'password'))

    def test_home_var(self):
        """Test that we get an error if the file doesn't exists.

        Also checks that the environment variable is being read.
        """

        os.environ['JSA_PROC_DIR'] = '/HORSEFEATHERS'
        with self.assertRaises(JSAProcError):
            config = get_config()
