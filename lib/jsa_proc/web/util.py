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

Pagination = namedtuple('Pagination', 'first prev next last up count')

url_for = flask.url_for

url_for_omp = werkzeug.urls.Href('http://omp.eao.hawaii.edu/cgi-bin')


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


class ErrorPage(Exception):
    """Exception class where an error page should be shown."""

    pass


def templated(template):
    """Template application decorator.

    Based on the example in the Flask documentation at:
    http://flask.pocoo.org/docs/patterns/viewdecorators/

    The ErrorPage exception is caught, and rendered using
    the error_page_repsonse method.
    """

    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                return _make_response(template, f(*args, **kwargs))

            except ErrorPage as err:
                return error_page_response(err)

        return decorated_function

    return decorator


def error_page_response(err):
    """Prepare flask response for an error page."""

    return _make_response('error.html',
                          {'title': 'Error', 'message': err.message})


def _make_response(template, result):
    """Prepare flask repsonse via a template."""

    resp = flask.make_response(flask.render_template(template, **result))
    resp.headers['Content-Language'] = 'en'

    return resp


def calculate_pagination(count, default_number,
                         page_number, page_name, url_args):
    """Process pagination options and create pagination links.

    Arguments:
        count: total number of observations
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


    # Check if number is given within the url_args, if not then
    # set it to the default number.
    if 'number' not in url_args or url_args['number'] is None or url_args['number'] == 0:
        number_per_page = default_number
        url_args['number'] = default_number
    else:
        number_per_page = int(url_args['number'])

    # Sanitize input of page_number.
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
                **url_args)
        if page_number > 1 else None,

        url_for('job_list', page=(page_number - 1),
                **url_args)
        if page_number > 0 else None,

        url_for('job_list', page=(page_number + 1),
                **url_args)
        if page_number < page_max else None,

        url_for('job_list', page=page_max,
                **url_args)
        if page_number < (page_max - 1) else None,

        None,

        count
    )

    return (number_per_page, page_number, pagination)
