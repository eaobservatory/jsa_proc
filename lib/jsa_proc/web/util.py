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

from __future__ import absolute_import, division

import math
from collections import namedtuple

import flask
import functools
import werkzeug.exceptions
import werkzeug.routing
import werkzeug.urls

Pagination = namedtuple('Pagination', 'first prev next last up')

url_for = flask.url_for

url_for_omp = werkzeug.urls.Href('http://omp.jach.hawaii.edu/cgi-bin')


def url_for_omp_comment(obsid, instrument, obsnum, date_obs):
    return url_for_omp('staffobscomment.pl', {
        'oid': obsid, 'inst': instrument, 'runnr': obsnum,
        'ut': date_obs.strftime('%Y-%m-%d-%H-%M-%S')})


class HTTPError(werkzeug.exceptions.InternalServerError):
    """Exception class for raising HTTP errors."""

    pass


class HTTPUnauthorized(werkzeug.exceptions.Unauthorized):
    """Exception class raising an HTTP unauthorized 401 error"""

    pass


class HTTPNotFound(werkzeug.exceptions.NotFound):
    """Exception class for HTTP not found errors."""

    pass


class HTTPRedirect(werkzeug.routing.RequestRedirect):
    """Exception class requesting a temporary ("See Other") HTTP redirect."""

    code = 303
    pass


def templated(template):
    """Template application decorator.

    Based on the example in the Flask documentation at:
    http://flask.pocoo.org/docs/patterns/viewdecorators/
    """

    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            result = f(*args, **kwargs)
            resp = flask.make_response(
                flask.render_template(template, **result))
            resp.headers['Content-Language'] = 'en'
            return resp

        return decorated_function

    return decorator


def calculate_pagination(count, number_per_page, default_number,
                         page_number, page_name, url_args):
    """Process pagination options and create pagination links.

    Arguments:
        number_per_page: number of items to show per page
                         (sanitized -- can be an HTTP parameter)
        default_number: default number of items per page
        page_number: requested page number
                     (sanitized -- can be an HTTP parameter)
        page_name: name of page to link to (used with url_for)
        url_args: additional arguments to pass to url_for

    Returns a tuple:
        number_per_page: sanitized number of items per page
        page_number: sanitized page number
        pagination: named tuple with first, prev, next and last elements
    """

    # Sanitize input of number_per_page and page_number.
    if (number_per_page == '') or (number_per_page is None):
        number_per_page = default_number
    else:
        number_per_page = int(number_per_page)

    if (page_number == '') or (page_number is None):
        page_number = 0
    else:
        page_number = int(page_number)

    # Determine the maximum page number (zero-indexed).
    if count == 0:
        page_max = 0
    else:
        page_max = int(math.ceil(count / number_per_page)) - 1

    # Ensure the current page number is within range.
    if page_number < 0:
        page_number = 0
    elif page_number > page_max:
        page_number = page_max

    # Create links for pagination.  Prefer to issue "prev" and "next"
    # rather than "first" and "last".
    pagination = Pagination(
        url_for('job_list', page=0,
                number=number_per_page, **url_args)
        if page_number > 1 else None,

        url_for('job_list', page=(page_number - 1),
                number=number_per_page, **url_args)
        if page_number > 0 else None,

        url_for('job_list', page=(page_number + 1),
                number=number_per_page, **url_args)
        if page_number < page_max else None,

        url_for('job_list', page=page_max,
                number=number_per_page, **url_args)
        if page_number < (page_max - 1) else None,

        None
    )

    return (number_per_page, page_number, pagination)
