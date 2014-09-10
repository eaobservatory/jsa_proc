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

import matplotlib
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import StringIO
import time


from flask import send_file

from jsa_proc.state import JSAProcState
from jsa_proc.web.util import url_for


def prepare_summary_piechart(db, task=None):
    """
    Create a piechart of jobs for a given task.
    """
    job_summary_dict = OrderedDict()
    for s in JSAProcState.STATE_ALL:
        if JSAProcState.get_name(s) != 'Deleted':
            job_summary_dict[s] = db.find_jobs(state=s,
                                               count=True, task=task)

    # Plot of total results
    values = job_summary_dict.values()
    names = [JSAProcState.get_name(i) for i in JSAProcState.STATE_ALL[:-1]]
    phases = ['red'] *3 + [ 'yellow'] *2 + ['green'] * 4 + ['blue'] + ['black'] * 1
    i=0
    while i < len(values):
        if values[i] == 0:
            values.pop(i)
            names.pop(i)
            phases.pop(i)
        else:
            i +=1

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
    fig.tight_layout()
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

    return {'results':results, 'states':JSAProcState.STATE_ALL}

def prepare_job_summary(db, task=None):

    """
    Prepare a summary of jobs.

    Needs to get:

           * Total jobs in db.
           * Number of jobs in JAC and CADC
           * Number of jobs in each state

    """

    states = JSAProcState.STATE_ALL
    locations = ['JAC', 'CADC']

    job_summary_dict = OrderedDict()
    for s in states:
        job_summary_dict[s] = OrderedDict()
        for l in locations:
            job_summary_dict[s][l] = db.find_jobs(location=l, state=s,
                                                  count=True, task=task)

    total_count = sum([int(c) for j in job_summary_dict.values()
                       for c in j.values()])

    # Plot of total results
    values = [sum(job_summary_dict[s].values()) for s in states]
    names = [JSAProcState.get_name(i) for i in states]
    phases = ['red'] *3 + [ 'yellow'] *2 + ['green'] * 4 + ['blue'] + ['black'] * 2



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
        'stamp': time.time()
    }
