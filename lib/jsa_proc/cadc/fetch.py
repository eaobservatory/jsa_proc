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

"""
Routines for downloading data from CADC.

"""

import requests
import os.path

from jsa_proc.config import get_config

jcmt_data_url = 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/JCMT/'

def fetch_cadc_file(filename):
    """
    Routine which will fetch a file from CADC into the current
    directory. It assumes the url is of the form:
    http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/JCMT/s4d20130401_00001_0002

    parameters;
    filename, string
    This can remove a .sdf or .sdf.gz extension, but not any others.

    Will raise an requests.except.HTTPError if it can't connect.
    """

    # Filename -- remove .sdf or .sdf.gz if present, otherwise leave
    # as given.
    split = os.path.splitext(filename)
    if split[1] == '.gz':
        split = os.path.splitext(split[0])
    if split[1] == '.sdf':
        filename = split[0]

    # Data path.
    data_path = jcmt_data_url+filename

    # Get CADC login.
    config = get_config()
    cadc_username = config.get('cadc', 'username')
    cadc_password = config.get('cadc', 'password')

    # Local name to save to (requests automatically decompresses, so
    # don't need the .gz).
    local_file = filename+'.sdf'

    # Connect with stream=True for large files.
    r = requests.get(data_path, auth=(cadc_username, cadc_password), stream=True)
    
    # Check if its worked. (raises error if not okay)
    r.raise_for_status()

    # write out to a file.
    with open(local_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)
