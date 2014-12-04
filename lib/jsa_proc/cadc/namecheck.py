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

import logging
import os.path
import re

from lxml import etree

from jsa_proc.config import get_home

logger = logging.getLogger(__name__)

namecheck_file = 'etc/namecheck.xml'
namecheck_section = set(('RAW', 'PROCESSED'))
namecheck_pattern = None


def _get_namecheck_pattern():
    """Get a list of namecheck patterns.

    Returns a cached version if available, otherwise reads the namecheck
    configuration file.  The patterns are listed as regular expression
    objects.
    """

    global namecheck_pattern

    # If we already read the configuration, return it.
    if namecheck_pattern is not None:
        return namecheck_pattern

    # Otherwise read the namecheck XML file.
    namecheck_pattern = []

    file = os.path.join(get_home(), namecheck_file)
    tree = etree.parse(file)
    root = tree.getroot()

    for outerlist in root.iter('list'):
        for struct in outerlist.iter('struct'):
            for list in struct.iter('list'):
                key = list.get('key')

                if key in namecheck_section:
                    logger.debug('Reading namecheck section %s', key)

                    for value in list.iter('value'):
                        namecheck_pattern.append(
                            re.compile('^{0}$'.format(value.text)))

                else:
                    logger.debug('Skipping namecheck section %s', key)

    return namecheck_pattern
