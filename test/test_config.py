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

from jsa_proc.config import get_config


class ConfigTestCase(TestCase):
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
                        
