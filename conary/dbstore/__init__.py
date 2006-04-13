#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from conary.lib import cfg, cfgtypes

from base_drv import BaseDatabase as Database
from base_drv import BaseCursor as Cursor
from migration import SchemaMigration
from sqlerrors import InvalidBackend

# default driver we want to use
__DRIVER = "sqlite"

def __get_driver(driver = __DRIVER):
    global __DRIVER
    if not driver:
        driver = __DRIVER
    if driver == "sqlite":
        try:
            from sqlite_drv import Database
        except ImportError, e:
            raise InvalidBackend(
                "Could not locate driver for backend '%s'" % (driver,),
                e.args + (driver,))
        else:
            return Database
    # postgresl support
    if driver == "postgresql":
        try:
            from postgresql_drv import Database
        except ImportError, e:
            raise InvalidBackend(
                "Could not locate driver for backend '%s'" % (driver,),
                e.args + (driver,))
        else:
            return Database
    # mysql support
    if driver == "mysql":
        try:
            from mysql_drv import Database
        except ImportError, e:
            raise InvalidBackend(
                "Could not locate driver for backend '%s'" % (driver,),
                e.args + (driver,))
        else:
            return Database
    # ingres support
    if driver == "ingres":
        try:
            from ingres_drv import Database
        except ImportError, e:
            raise InvalidBackend(
                "Could not locate driver for backend '%s'" % (driver,),
                e.args + (driver,))
        else:
            return Database
    # ELSE, INVALID
    raise InvalidBackend(
        "Database backend '%s' is not supported" % (driver,),
        driver)

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
