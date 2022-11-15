# Copyright (C) 2015 East Asian Observatory.
#
# TODO: insert license statement.

try:
    from configparser import SafeConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser

from omp.siteconfig import get_omp_siteconfig
from omp.db.db import OMPDB
from jsa_proc.error import JSAProcError

omp_database_access = {}


def get_omp_database(write_access=None):
    """Construct an OMP database access object.

    Write access can either be None (the default), "omp"
    or "jcmt".  Read-only and OMP credentials come from
    the OMP siteconfig file.  JCMT database write permissions
    come from the JSA Proc configuration system.
    """

    global omp_database_access

    if write_access not in omp_database_access:
        # Connect using the "hdr_database" set of credentials, which is
        # the "staff" user (supposed to be read only) at the time of
        # writing, unless the write_access option is specified.
        if write_access is None:
            config = get_omp_siteconfig()
            credentials = 'hdr_database'
        elif write_access == 'omp':
            config = get_omp_siteconfig()
            credentials = 'database'
        elif write_access == 'jcmt':
            config = get_enterdata_config()
            credentials = 'database'
        else:
            raise JSAProcError('Unknown write_access request {0}'
                               .format(write_access))

        omp_database_access[write_access] = OMPDB(
            server=config.get(credentials, 'server'),
            user=config.get(credentials, 'user'),
            password=config.get(credentials, 'password'),
            read_only=(write_access is None))

    return omp_database_access[write_access]


def get_enterdata_config():
    """Read the enterdata config file."""

    enterdata_config_file = '/jac_sw/etc/enterdata/enterdata.cfg'

    config = SafeConfigParser()

    config.read(enterdata_config_file)

    return config
