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


# Information about the query:
#      name: Short name
#  fullname: Longer description
#     where: Query string to be included in where part of query.
def _where_maker(name):
    return namedtuple(name, 'name fullname where')


# Surveys.
SurveyInfo = _where_maker('SurveyInfo')

Surveys = dict(
    GBS=SurveyInfo('GBS', 'Gould Belt Survey', 'obs.survey="GBS"'),
    JPS=SurveyInfo('JPS', 'Galactic Plane Survey', 'obs.survey = "JPS"'),
    NGS=SurveyInfo('NGS', 'Nearby Galaxies Survey', 'obs.survey = "NGS"'),
    DDS=SurveyInfo('DDS', 'Survey of Nearby Stars', 'obs.survey = "DDS"'),
    NoSurvey=SurveyInfo('NoSurvey', 'No Survey', 'obs.survey IS NULL'),
    SASSY=SurveyInfo('SASSY', 'SASSY', 'obs.survey = "SASSY"'),
    CLS=SurveyInfo('CLS', 'Cosmology Legacy Survey', 'obs.survey = "CLS"'),
    SLS=SurveyInfo('SLS', 'Spectral Legacy Survey', 'obs.survey = "SLS"'),
)


# ObsType.
ObsTypeInfo = _where_maker('ObsType')
ObsTypes = dict(
    Pointing=ObsTypeInfo('Pointing', 'Pointing observations',
                         'obs.obstype = "pointing"'),
    Science=ObsTypeInfo('Science', 'Science observations',
                        'obs.obstype = "science"'),
)


# Calibration.
CalQuery = _where_maker('CalQuery')
CalTypes = dict(
    Calibrations=CalQuery(
        'Calibrations', 'Observations marked as JCMTCAL and CAL',
        '(obs.project = "JCMTCAL"  OR obs.project = "CAL")'),
    NoCalibrations=CalQuery(
        'NoCalibrations', 'Observations not marked as calib',
        '(obs.project != "JCMTCAL" AND obs.project !="CAL")'),
)


# ScanMode.
ScanMode = _where_maker('ScanMode')
ScanModes = dict(
    Daisy=ScanMode('Daisy', 'Daisy scans',
                   '(obs.scanmode = "DAISY" OR obs.scanmode = "CV_DAISY")'),
    Pong=ScanMode('Pong', 'Pong scans', '(obs.scanmode = "CURVY_PONG")'),
)


ObsQueryDict = {
    'Surveys': Surveys,
    'ObsTypes': ObsTypes,
    'CalTypes': CalTypes,
    'ScanModes': ScanModes,
}


ObsQuery = namedtuple('ObsQuery', 'querylist')
