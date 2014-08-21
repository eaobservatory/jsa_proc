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


import flask
import functools
import werkzeug.exceptions
import werkzeug.routing


url_for = flask.url_for


class HTTPError(werkzeug.exceptions.InternalServerError):
    """Exception class for raising HTTP errors."""

    pass


class HTTPNotFound(werkzeug.exceptions.NotFound):
    """Exception class for HTTP not found errors."""

    pass


class HTTPRedirect(werkzeug.routing.RequestRedirect):
    """Exception class requesting an HTTP redirect."""

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
            return flask.render_template(template, **result)
        return decorated_function
    return decorator
