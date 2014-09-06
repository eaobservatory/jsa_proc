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

from collections import namedtuple

from jsa_proc.db.db import Not


# Information about the query:
#      name: Short name
#  fullname: Longer description
#     where: Query string to be included in where part of query.
def _where_maker(name):
    return namedtuple(name, 'name fullname where')


# Surveys.
SurveyInfo = _where_maker('SurveyInfo')

Surveys = dict(
    GBS=SurveyInfo('GBS', 'Gould Belt Survey', {'survey': 'GBS'}),
    JPS=SurveyInfo('JPS', 'Galactic Plane Survey', {'survey': 'JPS'}),
    NGS=SurveyInfo('NGS', 'Nearby Galaxies Survey', {'survey': 'NGS'}),
    DDS=SurveyInfo('DDS', 'Survey of Nearby Stars', {'survey': 'DDS'}),
    NoSurvey=SurveyInfo('NoSurvey', 'No Survey', {'survey': None}),
    SASSY=SurveyInfo('SASSY', 'SASSY', {'survey': 'SASSY'}),
    CLS=SurveyInfo('CLS', 'Cosmology Legacy Survey', {'survey': 'CLS'}),
    SLS=SurveyInfo('SLS', 'Spectral Legacy Survey', {'survey': 'SLS'}),
)


# ObsType.
ObsTypeInfo = _where_maker('ObsType')
ObsTypes = dict(
    Pointing=ObsTypeInfo('Pointing', 'Pointing observations',
                         {'obstype': 'pointing'}),
    Science=ObsTypeInfo('Science', 'Science observations',
                        {'obstype': 'science'}),
)


# Calibration.
CalQuery = _where_maker('CalQuery')
CalTypes = dict(
    Calibrations=CalQuery(
        'Calibrations', 'Observations marked as JCMTCAL and CAL',
        {'project': ['JCMTCAL', 'CAL']}),
    NoCalibrations=CalQuery(
        'NoCalibrations', 'Observations not marked as calib',
        {'project': Not(['JCMTCAL', 'CAL'])}),
)


# ScanMode.
ScanMode = _where_maker('ScanMode')
ScanModes = dict(
    Daisy=ScanMode('Daisy', 'Daisy scans',
                   {'scanmode': ['DAISY', 'CV_DAISY']}),
    Pong=ScanMode('Pong', 'Pong scans', {'scanmode': 'CURVY_PONG'}),
)

# Subsystem
SubsystemInfo = _where_maker('SubsystemInfo')

ObsQueryDict = {
    'Surveys': Surveys,
    'ObsTypes': ObsTypes,
    'CalTypes': CalTypes,
    'ScanModes': ScanModes,
    'subsystem': {
        '450': SubsystemInfo('450', '450um', {'subsys': '450'}),
        '850': SubsystemInfo('850', '850um', {'subsys': '850'}),
    },
}
