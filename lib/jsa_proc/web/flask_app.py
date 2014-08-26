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

from flask import Flask, flash, request, send_file
import os.path

import jsa_proc.config
from jsa_proc.state import JSAProcState

from jsa_proc.web.util import \
    url_for, templated, HTTPError, HTTPNotFound, HTTPRedirect

from jsa_proc.web.job_list import prepare_job_list
from jsa_proc.web.job_change_state import prepare_change_state
from jsa_proc.web.job_summary import prepare_job_summary
from jsa_proc.web.job_info import prepare_job_info
from jsa_proc.web.job_preview import prepare_job_preview
from jsa_proc.web.job_log import prepare_job_log
from jsa_proc.web.error_summary import prepare_error_summary


def create_web_app():
    """Function to prepare the Flask web application."""

    home = jsa_proc.config.get_home()
    db = jsa_proc.config.get_database()

    app = Flask(
        'jsa_proc',
        static_folder=os.path.join(home, 'web', 'static'),
        template_folder=os.path.join(home, 'web', 'templates'),
    )

    # Route Handlers.

    @app.route('/')
    def home_page():
        raise HTTPRedirect(url_for('job_summary'))

    @app.route('/job/')
    @templated('job_list.html')
    def job_list():
        return prepare_job_list(
            db,
            request.args.get('location', None),
            request.args.get('state', None),
            request.args.get('number', None),
            request.args.get('page', None)
        )

    @app.route('/summary/')
    @templated('job_summary.html')
    def job_summary():
        return prepare_job_summary(db)

    @app.route('/error_summary/')
    @templated('error_summary.html')
    def error_summary():
        return prepare_error_summary(
            db,
            filtering = request.args.get('filtering', None),
        )

    @app.route('/job/<int:job_id>', methods=['GET'])
    @templated('job_info.html')
    def job_info(job_id):
        return prepare_job_info(db, job_id)

    @app.route('/job_change_state' ,methods=['POST'])
    def job_change_state():

        # Get the variables from POST
        newstate = request.form['newstate']
        message = request.form['message']
        job_ids = request.form.getlist('job_id')
        url = request.form['url']

        # Change the state.
        prepare_change_state(db, job_ids,
                             newstate,
                             message)

        # Redirect the page to correct info.
        #flash('You have successfully mangled the job status!')
        raise HTTPRedirect(url)

    # Image handling.
    @app.route('/job/<int:job_id>/preview/<preview>')
    def job_preview(job_id, preview):
        path = prepare_job_preview(job_id, preview)
        return send_file(path, mimetype='image/png')

    @app.route('/job/<int:job_id>/log/<log>')
    def job_log_html(job_id, log):
        path = prepare_job_log(job_id, log)
        return send_file(path, mimetype='text/html')

    @app.route('/job/<int:job_id>/log_text/<log>')
    def job_log_text(job_id, log):
        path = prepare_job_log(job_id, log)
        return send_file(path, mimetype='text/plain')

    # Filters and Tests.

    @app.template_filter('state_name')
    def state_name_filter(state):
        return JSAProcState.get_name(state)

    @app.template_test('state_active')
    def state_active_test(state):
        return JSAProcState.get_info(state).active

    @app.template_filter('state_phase')
    def state_phase_filter(state):
        phase = JSAProcState.get_info(state).phase
        if phase == JSAProcState.PHASE_QUEUE:
            return 'queue'
        elif phase == JSAProcState.PHASE_FETCH:
            return 'fetch'
        elif phase == JSAProcState.PHASE_RUN:
            return 'run'
        elif phase == JSAProcState.PHASE_COMPLETE:
            return 'complete'
        elif phase == JSAProcState.PHASE_ERROR:
            return 'error'
        raise HTTPError('Unknown phase {0}'.format(phase))

    # Return the Application.

    return app
