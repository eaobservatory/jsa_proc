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

from base64 import b64decode
from codecs import ascii_decode
import requests
from requests.exceptions import RequestException
import os.path

from jsa_proc.error import JSAProcError

jcmt_data_url = 'https://ws-cadc.canfar.net/minoc'

proxy_certificate = os.path.expanduser('~/.ssl/cadcproxy.pem')


def fetch_cadc_file(filename, output_directory, suffix='.sdf'):
    """
    Routine which will fetch a file from CADC and save it into the output
    directory. It assumes the url is of the form:
    http://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/JCMT/s4d20130401_00001_0002

    parameters;
    filename, string
    This assumes a filename without extension or path.

    output_directory, string
    Path to save file to.

    suffix: additional suffix to be added to the filename
    before saving to the output directory.
    (string, default: ".sdf")

    Will raise an JSAProcError if it can't connect.

    Returns name of file with path
    """

    # Local name to save to (requests automatically decompresses, so
    # don't need the .gz).
    local_file = filename + suffix
    output_file_path = os.path.join(output_directory, local_file)

    try:
        (args, kwargs) = _prepare_cadc_request(filename)

        # Connect with stream=True for large files.
        kwargs['stream'] = True

        r = requests.get(*args, **kwargs)

        # Check if its worked. (raises error if not okay)
        r.raise_for_status()

        # write out to a file in the requested output directory
        with open(output_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)

    except RequestException as e:
        raise JSAProcError('Error fetching CADC file: ' + str(e))

    return output_file_path


def fetch_cadc_file_info(filename):
    """Retrieve information about a file in the JCMT archive at CADC.

    This routine works in the same way as fetch_cadc_file but makes
    an HTTP HEAD request instead of an HTTP GET request.
    """

    try:
        (args, kwargs) = _prepare_cadc_request(filename)

        kwargs['allow_redirects'] = True

        r = requests.head(*args, **kwargs)

        if r.status_code == 404:
            return None

        # Check if its worked. (raises error if not okay)
        r.raise_for_status()

        # The minoc service seems to be returning the base64 encoding
        # of the hex representation of the MD5.  Decode it here to
        # restore the previous style header and in case this changes.
        if 'content-md5' not in r.headers:
            digest = r.headers['digest']
            if not digest.startswith('md5='):
                raise JSAProcError('Digest not in expected md5= format')

            r.headers['content-md5'] = ascii_decode(b64decode(digest[4:]))[0]

        return r.headers

    except RequestException as e:
        raise JSAProcError('Error fetching CADC file info: ' + str(e))


def check_cadc_files(files):
    """Check whether the given files are at CADC.

    Returns a boolean list of the same length as the input list,
    with true values corresponding only to the files which
    are present.
    """

    result = []

    for filename in files:
        result.append(fetch_cadc_file_info(filename) is not None)

    return result


def put_cadc_file(filename, input_directory):
    """Put the given file into the CADC archive.

    Raises a JSAProcError on failure.
    """

    (args, kwargs) = _prepare_cadc_request(filename)
    r = None

    try:
        with open(os.path.join(input_directory, filename), 'rb') as f:
            kwargs['data'] = f

            r = requests.put(*args, **kwargs)

            r.raise_for_status()

            if r.status_code in (200, 201):
                return

    except RequestException as e:
        text = 'no text received' if r is None else r.text
        raise JSAProcError('Error putting CADC file: {0}: {1}'
                           .format(str(e), text))

    raise JSAProcError('Putting CADC file gave bad status: {0}: {1}'
                       .format(r.status_code, r.text))


def _prepare_cadc_request(filename):
    """Prepare request parameters for a CADC data web service
    request.

    Returns arguments and keyword arguments for the requests
    library methods (get and head).
    """

    # Data path.
    url = '{}/files/{}'.format(jcmt_data_url, make_artifact_uri(filename))

    return ([url], {'cert': proxy_certificate})


def make_artifact_uri(filename, archive='JCMT'):
    return 'cadc:{}/{}'.format(archive, filename)
