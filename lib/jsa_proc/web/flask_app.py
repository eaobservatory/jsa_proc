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
import os.path
import jsa_proc.config

from jsa_proc.web.util import \
    url_for, templated, HTTPError, HTTPNotFound, HTTPRedirect

from jsa_proc.web.job_list import prepare_job_list
from jsa_proc.web.job_info import prepare_job_info


def create_web_app():
    """Function to prepare the Flask web application."""

    home = jsa_proc.config.get_home()
    db = jsa_proc.config.get_database()

    app = flask.Flask(
        'jsa_proc',
        static_folder=os.path.join(home, 'web', 'static'),
        template_folder=os.path.join(home, 'web', 'templates'),
    )

    @app.route('/')
    def home_page():
        raise HTTPRedirect(url_for('job_list'))

    @app.route('/job/')
    @templated('job_list.html')
    def job_list():
        return prepare_job_list(db)

    @app.route('/job/<int:job_id>')
    @templated('job_info.html')
    def job_info(job_id):
        return prepare_job_info(db, job_id)

    return app
