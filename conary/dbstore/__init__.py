#
# Copyright (c) rPath, Inc.
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
#


import os
import re

from conary.lib import cfg, cfgtypes

from base_drv import BaseDatabase as Database
from base_drv import BaseCursor as Cursor
from migration import SchemaMigration
from sqlerrors import InvalidBackend

# default driver we want to use
__DRIVER = "sqlite"

_driverCache = {}


def __get_driver(driver = __DRIVER):
    global __DRIVER
    if not driver:
        driver = __DRIVER
    # requesting a postgresql driver that is pooling aware switches to
    # the pgpool driver
    if driver == "postgresql" and os.environ.has_key("POSTGRESQL_POOL"):
        driver = "pgpool"

    if driver not in _driverCache:
        _loadDriver(driver)
    return _driverCache[driver]


def _loadDriver(name):
    if not re.match('^[a-zA-Z0-9_]+', name):
        raise ValueError("Invalid SQL driver name %r" % (name,))

    modName = name + '_drv'
    try:
        driverModule = __import__(modName, globals(), locals())
    except ImportError, err:
        if modName in str(err):
            # Re-throw only in cases where the dbstore driver missing.
            raise InvalidBackend("The SQL backend %r is not supported" %
                    (name,))
        else:
            # Otherwise a dependency failed to load, let those bubble up
            # normally for easy debugging.
            raise
    _driverCache[name] = getattr(driverModule, 'Database')


# create a database connection and return an instance
# all drivers parse a db string in the form:
#   [[user[:password]@]host/]database
def connect(db, driver=None, **kw):
    driver = __get_driver(driver)
    dbh = driver(db)
    assert(dbh.connect(**kw))
    return dbh

# A class for configuration of a database driver
class CfgDriver(cfg.CfgType):
    def parseString(self, str):
        s = str.split()
        if len(s) != 2:
            raise cfgtypes.ParseError("database driver and path expected")
        return tuple(s)
    def format(self, val, displayOptions = None):
        return "%s %s" % val

__all__ = [ "connect", "InvalidBackend", "CfgDriver"]
