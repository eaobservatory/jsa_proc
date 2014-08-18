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


class DummyCADCDP:
    def __init__(self, ris):
        self.info = []
        self.file = {}

        for (info, input) in ris:
            self.info.append(info)
            self.file[info.id] = input

    def get_recipe_info(self):
        return self.info

    def get_recipe_input_files(self, id_):
        return self.file.get(id_, [])
