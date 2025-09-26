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

from collections import namedtuple, OrderedDict

from jsa_proc.db.db import Not


# Information about the query:
# name: Short name
# fullname: Longer description
#     where: Query string to be included in where part of query.
def _where_maker(name):
    return namedtuple(name, 'fullname where')

InstrumentInfo = _where_maker('Instrument')
SurveyInfo = _where_maker('SurveyInfo')
ObsTypeInfo = _where_maker('ObsType')
CalQuery = _where_maker('CalQuery')
ScanMode = _where_maker('ScanMode')
SubsystemInfo = _where_maker('SubsystemInfo')

ObsQueryDict = OrderedDict((
    ('Surveys', OrderedDict((
        ('NoSurvey', SurveyInfo(
            'No Survey',
            {'survey': None})),
        ('__a', None),
        ('CLS', SurveyInfo(
            'Cosmology Legacy Survey', {'survey': 'CLS'})),
        ('NotCLS', SurveyInfo(
            'Not: Cosmology Legacy Survey',
            {'survey': Not('CLS')})),
        ('__b', None),
        ('DDS', SurveyInfo(
            'Survey of Nearby Stars',
            {'survey': 'DDS'})),
        ('GBS', SurveyInfo(
            'Gould Belt Survey',
            {'survey': 'GBS'})),
        ('JPS', SurveyInfo(
            'Galactic Plane Survey',
            {'survey': 'JPS'})),
        ('NGS', SurveyInfo(
            'Nearby Galaxies Survey',
            {'survey': 'NGS'})),
        ('SASSY', SurveyInfo(
            'SASSY',
            {'survey': 'SASSY'})),
        ('SLS', SurveyInfo(
            'Spectral Legacy Survey',
            {'survey': 'SLS'})),
    ))),
    ('ObsTypes', OrderedDict((
        ('Science', ObsTypeInfo(
            'Science observations',
            {'obstype': 'science'})),
        ('Pointing', ObsTypeInfo(
            'Pointing observations',
            {'obstype': 'pointing'})),
    ))),
    ('CalTypes', OrderedDict((
        ('Calibrations', CalQuery(
            'Observations marked as JCMTCAL and CAL',
            {'project': ['JCMTCAL', 'CAL']})),
        ('NoCalibrations', CalQuery(
            'Observations not marked as calib',
            {'project': Not(['JCMTCAL', 'CAL'])})),
    ))),
    ('ScanModes', OrderedDict((
        ('Daisy', ScanMode(
            'Daisy scans',
            {'sam_mode': 'scan', 'scan_pat': ['DAISY', 'CV_DAISY']})),
        ('Pong', ScanMode(
            'Pong scans',
            {'sam_mode': 'scan', 'scan_pat': 'CURVY_PONG'})),
        ('__a', None),
        ('Jiggle', ScanMode(
            'Jiggle',
            {'sam_mode': 'jiggle'})),
        ('Grid', ScanMode(
            'Grid',
            {'sam_mode': 'grid'})),
        ('Raster', ScanMode(
            'Raster',
            {'sam_mode': 'scan', 'scan_pat': 'DISCRETE_BOUSTROPHEDON'})),
    ))),
    ('subsystem', OrderedDict((
        ('850', SubsystemInfo('850um', {'subsys': '850'})),
        ('450', SubsystemInfo('450um', {'subsys': '450'})),
        ('__a', None),
        ('1',   SubsystemInfo('1', {'subsys': '1'})),
        ('2',   SubsystemInfo('2', {'subsys': '2'})),
        ('3',   SubsystemInfo('3', {'subsys': '3'})),
        ('4',   SubsystemInfo('4', {'subsys': '4'})),
        ('5',   SubsystemInfo('5', {'subsys': '5'})),
        ('6',   SubsystemInfo('6', {'subsys': '6'})),
        ('7',   SubsystemInfo('7', {'subsys': '7'})),
        ('8',   SubsystemInfo('8', {'subsys': '8'})),
    ))),
    ('instrument', OrderedDict((
        ('SCUBA-2', InstrumentInfo(
            'SCUBA-2',
            {'instrument': 'SCUBA-2'})),
        ('Heterodyne', InstrumentInfo(
            'Heterodyne',
            {'instrument': Not('SCUBA-2')})),
        ('__a', None),
        ('HARP', InstrumentInfo(
            'HARP',
            {'instrument': 'HARP'})),
        ('RxA3', InstrumentInfo(
            'RxA3',
            {'instrument': ['RXA3', 'RXA3M']})),
        ('ALAIHI', InstrumentInfo(
            '\u02bbAla\u02bbihi',
            {'instrument': 'ALAIHI'})),
        ('UU', InstrumentInfo(
            '\u02bb\u016a\u02bb\u016b',
            {'instrument': 'UU'})),
        ('AWEOWEO', InstrumentInfo(
            '\u02bb\u0100weoweo',
            {'instrument': 'AWEOWEO'})),
        ('Kuntur', InstrumentInfo(
            'Kuntur',
            {'instrument': 'KUNTUR'})),
    ))),
))
