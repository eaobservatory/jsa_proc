# Copyright (C) 2018 East Asian Observatory.
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
{0} - Custom data ingestion script

Usage:
    {0} [-v | -q] [-n] --transdir <transdir>

Options:
    --help, -h              Show usage information.
    --verbose, -v           Show debugging information.
    --quiet, -q             Do not show general logging information.
    --dry-run, -n           Skip actual ingestion step.
    --transdir <transdir>   Specify directory containing products.
"""


class CustomJobIngest(object):
    def __init__(self, program_name='custom_ingest'):
        """
        Constructor for custom ingest script class.
        """

        self.program_name = program_name

    def run(self):
        """
        Main routine for custom ingestion script.

        Handles command line arguments, checks that the transfer directory
        exists and reads a listing of its contents.
        """

        args = docopt(program_usage.format(self.program_name))

        # Configure logging.
        logging.basicConfig(
            level=logging.DEBUG if args['--verbose'] else (
                logging.WARNING if args['--quiet'] else logging.INFO))

        # Check that the transfer directory exists.
        logger.debug('Checking transfer directory exists')
        transdir = args['--transdir']

        if not os.path.isdir(transdir):
            logger.error('Specified transdir does not exist (or is a file)')
            sys.exit(1)

        # Get transfer directory listing.
        filenames = []
        logger.debug('Reading transfer directory listing')
        for file_ in os.listdir(transdir):
            if os.path.isfile(os.path.join(transdir, file_)):
                filenames.append(file_)
            else:
                logger.warning(
                    'Transfer directory contains non-file {0}'.format(file_))

        # Run the data ingestion function.
        logger.debug('About to begin ingestion')
        try:
            self.run_ingestion(transdir, filenames, dry_run=args['--dry-run'])

        except:
            logger.exception('Exception during ingestion')
            sys.exit(1)

        logger.debug('Ingestion complete')

    def run_ingestion(self, transdir, filenames, dry_run=False):
        """
        Function to launch data ingestion.

        :param transdir: directory containing job output files.
        :param filenames: list of output files.
        :param dry_run: specifies dry run mode.
        """

        # This method must be overridden by subclasses.
        raise Exception('Custom ingestion routine not defined')
