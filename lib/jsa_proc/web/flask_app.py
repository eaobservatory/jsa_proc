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

import datetime
from flask import Flask, flash, request, Response, render_template, \
    send_file, session
from functools import wraps
import os.path

from omp.obs.state import OMPState

from jsa_proc.config import get_config, get_database, get_home
from jsa_proc.state import JSAProcState
from jsa_proc.qa_state import JSAQAState

from jsa_proc.jcmtobsinfo import ObsQueryDict
from jsa_proc.omp.auth import check_staff_password
from jsa_proc.web.util import \
    url_for, url_for_omp, url_for_omp_comment, templated, HTTPError, \
    HTTPNotFound, HTTPRedirect, HTTPUnauthorized, \
    ErrorPage, error_page_response

from jsa_proc.web.job_list import prepare_job_list
from jsa_proc.web.job_change_state import prepare_change_state, \
    prepare_change_qa
from jsa_proc.web.job_summary import prepare_job_summary, \
    prepare_task_summary, prepare_summary_piechart, \
    prepare_task_qa_summary
from jsa_proc.web.job_info import prepare_job_info
from jsa_proc.web.job_preview import prepare_job_preview
from jsa_proc.web.job_qa_info import prepare_job_qa_info
from jsa_proc.web.job_log import prepare_job_log
from jsa_proc.web.error_summary import prepare_error_summary

from jsa_proc.omp.config import get_omp_database

loginstring = 'Basic realm="Login Required: use your username and ' + \
              'the staff password, or hit cancel to log out"'


def create_web_app():
    """Function to prepare the Flask web application."""

    home = get_home()
    db = get_database()
    database_name = get_config().get('database', 'database')

    app = Flask(
        'jsa_proc',
        static_folder=os.path.join(home, 'web', 'static'),
        template_folder=os.path.join(home, 'web', 'templates'),
    )

    app.secret_key = get_config().get('web', 'key')

    # Web authorization -- mostly take from flask docs snippets 8
    # http://flask.pocoo.org/snippets/8
    def check_auth(password):
        """
        Check that the staff pasword has been used.

        (Note that we don't care what the username is).
        """
        return check_staff_password(password)

    def authenticate():
        """
        Send a 401 response so that we can log in.
        """

        return Response(render_template('logout.html',
                                        redirect=request.referrer),
                        401, {'WWW-Authenticate': loginstring})

    def requires_auth(f):
        """
        A decorator to wrap functions that require authorization.
        """

        @wraps(f)
        def decorated(*args, **kwargs):
            auth = request.authorization
            if not auth or not check_auth(auth.password):
                return authenticate()
            return f(*args, **kwargs)

        return decorated

    # Route Handlers.

    @app.route('/')
    def home_page():
        raise HTTPRedirect(url_for('task_summary'))

    @app.route('/job/')
    @templated('job_list.html')
    def job_list():
        # Prepare query arguments list: special parameters first.
        kwargs = {
            'state': request.args.getlist('state'),
            'mode': request.args.get('mode', 'JSAProc'),
            'tiles': request.args.get('tiles')
        }

        # Now add regular string parameters, including those from
        # jcmtobsinfo.
        params = [
            'location',
            'task',
            'date_min',
            'date_max',
            'qa_state',
            'sourcename',
            'obsnum',
            'project',
            'number',
            'tau_min',
            'tau_max',
        ]

        params.extend(ObsQueryDict.keys())

        for key in params:
            kwargs[key] = request.args.get(key, None)

        # Process empty strings used as null form parameters.
        for argname in kwargs:
            if kwargs[argname] == '':
                kwargs[argname] = None

        # Store the query in the session.
        session['job_query'] = kwargs

        # Finally prepare the template context.
        return prepare_job_list(
            db,
            page=request.args.get('page', None),
            **kwargs)

    @app.route('/image/<task>/piechart')
    def summary_piechart(task='None'):
        if task == 'None':
            task = None
        obsquerydict = {}
        for key in ObsQueryDict.keys():
            obsquerydict[key] = request.args.get(key, None)
        date_min = request.args.get('date_min', None)
        date_max = request.args.get('date_max', None)
        return prepare_summary_piechart(db, task=task,
                                        obsquerydict=obsquerydict,
                                        date_min=date_min, date_max=date_max)

    @app.route('/summary/')
    @templated('task_summary.html')
    def task_summary():
        return prepare_task_summary(db)

    @app.route('/qa')
    @templated('task_qa_summary.html')
    def task_qa_summary():
        return prepare_task_qa_summary(db)

    @app.route('/job_summary/')
    @templated('job_summary.html')
    def job_summary():
        task = request.args.get('task', None)
        date_min = request.args.get('date_min', None)
        date_max = request.args.get('date_max', None)
        return prepare_job_summary(db, task=task, date_min=date_min,
                                   date_max=date_max)

    @app.route('/error_summary/')
    @templated('error_summary.html')
    def error_summary():
        return prepare_error_summary(
            db,
            redirect_url=request.full_path,
            filtering=request.args.get('filtering', None),
            chosentask=request.args.get('chosentask', None),
            extrafilter=request.args.get('extrafilter', None),
            state_prev=request.args.get('state_prev', None),
        )

    @app.route('/job/<int:job_id>', methods=['GET'])
    @templated('job_info.html')
    def job_info(job_id):
        return prepare_job_info(db, job_id, session.get('job_query'))

    @app.route('/job/<int:job_id>/qa', methods=['GET'])
    @templated('job_qa.html')
    def job_qa(job_id):
        return prepare_job_qa_info(db, job_id, session.get('job_query'))

    @app.route('/job_change_state', methods=['POST'])
    @requires_auth
    def job_change_state():

        # Get the variables from POST
        newstate = request.form['newstate']
        state_prev = request.form['state_prev']
        message = request.form['message']
        job_ids = request.form.getlist('job_id')
        url = request.form['url']
        username = request.authorization['username']

        try:
            # Change the state.
            prepare_change_state(db, job_ids,
                                 newstate,
                                 state_prev,
                                 message,
                                 username)

            # Redirect the page to correct info.
            flash('The status has been changed to %s.' % JSAProcState.get_name(
                newstate))
            raise HTTPRedirect(url)

        except ErrorPage as err:
            return error_page_response(err)

    @app.route('/job_change_qa', methods=['POST'])
    @requires_auth
    def job_change_qa():

        # Get the variables from POST
        if "action_next" in request.form:
            url = request.form['url-next']
        else:
            url = request.form['url']
        qa_state = request.form['qa_state']
        message = request.form['message']
        job_ids = request.form.getlist('job_id')
        username = request.authorization['username']

        try:
            # Change the state.
            prepare_change_qa(db, job_ids,
                              qa_state,
                              message,
                              username,
                              )
            # Redirect the page to correct info.
            flash(
                'The QA status of job %s has been changed to %s.' %
                (str(' '.join(job_ids)),JSAQAState.get_name(qa_state))
            )
            raise HTTPRedirect(url)

        except ErrorPage as err:
            return error_page_response(err)

    # QA Nightly Summary pages
    @app.route('/qa-nightly')
    @templated('task_qa_summary_nightly.html')
    def qa_night_page():
        """
        By default, show the previous week.

        Note that prepare_task_qa_summary interprets dates as
        inclusive, so use 6 days for the time delta to get a week

        """
        date_min = request.args.get('date_min', None)
        if date_min is None or date_min == '':
            date_min = (
                datetime.date.today() - datetime.timedelta(days=6)
            ).strftime('%Y-%m-%d')

        date_max = request.args.get('date_max', None)
        if date_max is None or date_max == '':
            date_max = datetime.date.today().strftime('%Y-%m-%d')
        return prepare_task_qa_summary(db, date_min=date_min,
                                       date_max=date_max, task='jcmt-nightly',
                                       byDate=True)

    @app.route('/login')
    @requires_auth
    def login():
        raise HTTPRedirect(request.referrer)

    @app.route('/logout')
    def logout():
        return authenticate()

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


    @app.route('/fop_summary', methods=['GET'])
    @templated('fop_summary.html')
    def fop_summary():
        userid = request.args.get('userid', None)
        semester = request.args.get('semester', None)
        projdict = {}
        if userid and semester:
            ompdb = get_omp_database(write_access=None)
            projects = ompdb.get_support_projects(str(userid), str(semester))
            for p in projects:
                jobs = db.find_jobs(obsquery={'project':p}, task='jcmt-nightly')
                projdict[p] = [len(jobs),
                               sum(1 for j in jobs if j.state == "E"),
                               sum(1 for j in jobs if j.qa_state== 'B'),
                               sum(1 for j in jobs if j.qa_state == 'Q'),
                               sum(1 for j in jobs if j.qa_state == '?'),
                               sum(1 for j in jobs if j.qa_state =='G')]
        else:
            projects = None

        return {'userid':userid, 'semester':semester, 'projects':projdict}
    # Filters and Tests.

    @app.route('/fop_summary_getres', methods=['POST'])
    def fop_summary_getres():
        userid = request.form['userid']
        semester = request.form['semester']
        raise HTTPRedirect(url_for('fop_summary', userid=userid, semester=semester))

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

    @app.template_filter('qa_state_name')
    def qa_state_name(qa_state):
        if qa_state.lower() != 'total':
            name = JSAQAState.get_name(qa_state)
        else:
            name = 'Total'
        return name

    @app.template_filter('omp_state_name')
    def omp_state_name_filter(ompstate):
        return OMPState.get_name(ompstate)

    @app.template_filter('uniq')
    def uniq_filter(xs):
        return set(xs)

    @app.template_filter('datetimeformat')
    def datetimeformat(value, format='%Y-%m-%d<br>%H:%M'):
        return value.strftime(format)

    @app.template_filter('replace0')
    def replace_zero(value):
        if value==0:
            return '-'
        else:
            return value
    @app.context_processor
    def add_to_context():
        return {
            'url_for_omp': url_for_omp,
            'url_for_omp_comment': url_for_omp_comment,
            'database_name': database_name,
        }

    # Return the Application.

    return app
