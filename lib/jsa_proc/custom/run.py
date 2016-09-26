# Copyright (C) 2016 East Asian Observatory.
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

from __future__ import absolute_import, division, print_function

import logging
import os
import sys

from docopt import docopt

logger = logging.getLogger(__name__)

program_usage = """
{0} - Custom data reduction wrapper script

Usage:
    {0} [-v | -q] --id <id> --inputs <inputs> --transdir <transdir> [--] [<parameter> ...]

Options:
    --help, -h              Show usage information.
    --verbose, -v           Show debugging information.
    --quiet, -q             Do not show general logging information.
    --id <id>               Job identifier.
    --inputs <inputs>       Specify file giving input file listing.
    --transdir <transdir>   Specify target directory for final products.
"""


class CustomJobRun(object):
    def __init__(self, program_name='custom_run'):
        """
        Constructor for custom processing script class.
        """

        self.program_name = program_name

    def run(self):
        """
        Main routine for custom processing scripts.

        Handles command line arguments, reads the input file list
        and checks that the transfer directory exists.
        """

        args = docopt(program_usage.format(self.program_name))

        # Configure logging.
        logging.basicConfig(
            level=logging.DEBUG if args['--verbose'] else (
                logging.WARNING if args['--quiet'] else logging.INFO))

        # Read the input file listing.
        logger.debug('Reading input file list')
        try:
            inputs = []

            with open(args['--inputs']) as f:
                for file_ in f:
                    file_ = file_.strip()

                    if not file_:
                        continue

                    if not os.path.exists(file_):
                        raise Exception(
                            'Input file {0} does not exist'.format(file_))

                    inputs.append(file_)

        except:
            logger.exception('Exception reading input file listing')
            sys.exit(1)

        # Check that the transfer directory exists.
        logger.debug('Checking transfer directory exists')
        transdir = args['--transdir']

        if not os.path.isdir(transdir):
            logger.error('Specified transdir does not exist (or is a file)')
            sys.exit(1)

        # Run the data processing function.
        logger.debug('About to begin processing')
        try:
            self.run_processing(
                inputs, transdir, id_=args['--id'],
                parameters=[x for x in args['<parameter>'] if x != '--'])

        except:
            logger.exception('Exception during processing')
            sys.exit(1)

        logger.debug('Processing complete')

    def run_processing(self, inputs, transdir, id_='unknown', parameters=[]):
        # This method must be overridden by subclasses.
        raise Exception('Custom processing routine not defined')
