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

from collections import OrderedDict

import datetime
import numpy as np
import matplotlib
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import StringIO
import time


from flask import send_file

from jsa_proc.db.db import Range
from jsa_proc.jcmtobsinfo import ObsQueryDict
from jsa_proc.state import JSAProcState
from jsa_proc.qastate import JSAQAState
from jsa_proc.web.util import url_for


def prepare_summary_piechart(db, task=None, obsquerydict = None, date_min=None, date_max=None):
    """
    Create a piechart of number of jobs in each state for a given task
    and obsquery.

    *task*: name of task in database
    *obsquerydict*: dictionary of values that match the jcmtobsinfo.ObsQueryDict.

    Returns a sendfile object of mime-type image/png.

    """

    # Dictionaries for the result
    job_summary_dict = OrderedDict()

    # Fix up the obsquery to the right format for find jobs
    obsquery = {}
    for key, value in obsquerydict.items():
        if value:
            obsquery.update(ObsQueryDict[key][value].where)
    # Sort out dates
    if date_min is not None or date_max is not None:
        obsquery['utdate'] = Range(date_min, date_max)

    # Perform the find_jobs task for the given constraints in each JSAProcState.
    for s in JSAProcState.STATE_ALL:

        # Don't include deleted jobs in pie chart
        if JSAProcState.get_name(s) != 'Deleted':
            job_summary_dict[s] = db.find_jobs(state=s, task=task, obsquery=obsquery,
                                               count=True)

    # Get numbers, names and colors for the pie chart.
    values = job_summary_dict.values()
    names = [JSAProcState.get_name(i) for i in JSAProcState.STATE_ALL[:-1]]
    # This should probably be done better...
    phases = ['red'] *3 + [ 'yellow'] *2 + ['green'] * 4 + ['blue'] + ['black'] * 1

    # Remove any states that don't have any jobs in them
    i=0
    while i < len(values):
        if values[i] == 0:
            values.pop(i)
            names.pop(i)
            phases.pop(i)
        else:
            i += 1

    # Create pie chart
    fig = Figure(figsize=(6,5))
    ax = fig.add_subplot(111)
    ax.set_aspect(1)
    p,t,a = ax.pie(values, labels=names, colors= phases, autopct = '%.1F')
    for i in range(len(a)):
        if p[i].get_facecolor() == (1.0, 1.0, 0.0, 1.0):
            a[i].set_color('black')
        else:
            a[i].set_color('white')
        p[i].set_edgecolor('none')

    ax.patch.set_visible(False)
    fig.patch.set_visible(False)

    # Put figure into a send_file object
    canvas = FigureCanvas(fig)
    img = StringIO.StringIO()
    canvas.print_png(img)
    img.seek(0)

    return send_file(img, mimetype='image/png')


def prepare_task_summary(db):
    """
    Prepare a summary of tasks in the database.

    """

    tasks = db.get_tasks()
    results = {}
    for t in tasks:
        results[t] = {'total':db.find_jobs(task=t, count=True)}
        for s in JSAProcState.STATE_ALL:
            results[t][s] = db.find_jobs(task=t, state=s, count=True)

    return {'results': results, 'states': JSAProcState.STATE_ALL,
            'title': 'Summary'}

def prepare_task_qa_summary(db, task=None, date_min=None, date_max=None, byDate=None):
    """
    Prepare a summary of tasks in the database based on QA state.

    """
    # Sort out dates.
    obsquery = {}
    if date_min is not None or date_max is not None:
        obsquery['utdate'] = Range(date_min, date_max)
    daylist = []

    # Create list of days if requested.
    if byDate is True:
        d1 = datetime.date(*[int(i) for i in date_min.split('-')])
        d2 = datetime.date(*[int(i) for i in date_max.split('-')])
        larger = max(d2, d1)
        smaller = min(d2, d1)
        delta = larger + datetime.timedelta(1) - smaller
        if larger == d2:
            direction =  1
        else:
            direction = -1

        daylist = [(d1 + datetime.timedelta(days=i*direction)).strftime('%Y-%m-%d') for i in range(abs(delta.days))]


    if task:
        tasks = [task]
    else:
        tasks = db.get_tasks()

    qa_reduced_state = list(JSAProcState.STATE_POST_RUN)
    qa_raw_state = list(JSAProcState.STATE_PRE_RUN | set((JSAProcState.RUNNING,)))
    qa_error_state = [JSAProcState.ERROR]
    qa_deleted_state = [JSAProcState.DELETED]
    results = {}
    statedict = OrderedDict(zip(['Reduced', 'Error', 'Deleted', 'Raw'],
                                      [qa_reduced_state, qa_error_state, qa_deleted_state, qa_raw_state]))
    if byDate is True:

        # Go through each task
        for t in tasks:
            results[t] = OrderedDict()

            # Within each task go through each day
            for d in daylist:
                # Create the Range object
                obsquery['utdate'] = Range(d, d)

                # Find the total number of jobsf or that dat, put it in the dayresults dictionary
                dayresults = OrderedDict(total=db.find_jobs(task=t, count=True, obsquery=obsquery))

                # Go through each Reduced and Error states.
                for name,state_options in zip(['Reduced','Error'],[qa_reduced_state, qa_error_state]):
                    dayresults[name] = OrderedDict()
                    # Go through each  qa state
                    for q in JSAQAState.STATE_ALL:
                        dayresults[name][q] = db.find_jobs(task=t, qa_state=q, state=state_options,count=True,
                                                       obsquery=obsquery)

                    dayresults[name]['total'] = sum(dayresults[name].values())

                # Add on the totals for Raw and Deleted jobs to the dayresults
                dayresults['Deleted'] = {'total': db.find_jobs(task=t, state='X', count=True, obsquery=obsquery)}
                dayresults['Raw'] = {'total': db.find_jobs(task=t, state=qa_raw_state,
                                                           count=True, obsquery=obsquery)}

                # Update the results object
                results[t][d] = dayresults

    # If not separating by date
    else:
        for t in tasks:
            # Results dict for each task
            results[t] = {'total':db.find_jobs(task=t, count=True, obsquery=obsquery)}
            for q in JSAQAState.STATE_ALL:
                results[t][q] = db.find_jobs(task=t, qa_state=q, count=True, obsquery=obsquery)

    return {'results':results, 'qastates':JSAQAState.STATE_ALL,
            'daylist': daylist, 'statedict' : statedict,
            'title': 'QA Summary'}

def prepare_job_summary(db, task=None, date_min=None, date_max=None):

    """
    Prepare a summary of jobs, for a specific task and date.

    Needs to get:

           * Total jobs in db.
           * Number of jobs in JAC and CADC
           * Number of jobs in each state

    """

    states = JSAProcState.STATE_ALL
    locations = ['JAC', 'CADC']

    # Sort out dates.
    obsquery = {}
    if date_min is not None or date_max is not None:
        obsquery['utdate'] = Range(date_min, date_max)


    job_summary_dict = OrderedDict()
    for s in states:
        job_summary_dict[s] = OrderedDict()
        for l in locations:
            job_summary_dict[s][l] = db.find_jobs(location=l, state=s,
                                                  count=True, task=task, obsquery=obsquery)

    total_count = sum([int(c) for j in job_summary_dict.values()
                       for c in j.values()])

    # Get dates of first and last observations in task.
    firstobs, lastobs = db.get_date_range(task=task)[0]

    # Get processing time taken for All jobs, pointings only, cals only, science only. for this task.
    jobs,durations, obsinfos =  db.get_processing_time_obs_type(jobdict={'task':task})
    durations = np.array(durations)
    obsinfos = np.array(obsinfos)
    obstypes = obsinfos[:,0]
    obsprojects = obsinfos[:,2]

    pointings_mask = obstypes=='pointing'
    cals_mask = (obstypes=='science') & ( (obsprojects=='JCMTCAL') | (obsprojects=='CAL'))
    science_mask = (obstypes=='science') & ( (obsprojects!='JCMTCAL') & (obsprojects!='CAL'))

    total_processing_time_hrs = durations.sum()/(60.0*60.0)
    pointings_processing_time_hrs = durations[pointings_mask].sum()/(60.0*60.0)
    cals_processing_time_hrs = durations[cals_mask].sum()/(60.0*60.0)
    science_processing_time_hrs  = durations[science_mask].sum()/(60.0*60.0)
    # Title
    title = 'Summary of All jobs'
    if task:
        title = task + ' Summary'
    # Observations.

    return {
        'title': title,
        'total_count': total_count,
        'job_summary_dict': job_summary_dict,
        'locations': locations,
        'task': task,
        'stamp': time.time(),
        'date_min': date_min,
        'date_max': date_max,
        'firstobs': firstobs,
        'lastobs': lastobs,
        'total_proc_time': '%.1F'%total_processing_time_hrs,
        'pointings_proc_time': '%.1F'%pointings_processing_time_hrs,
        'cals_proc_time': '%.1F'%cals_processing_time_hrs,
        'science_proc_time': '%.1F'%science_processing_time_hrs,
    }
