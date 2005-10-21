#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from base_drv import DBStoreError, DBStoreCursorError
from base_drv import BaseDatabase as Database
from base_drv import BaseCursor as Cursor

# default driver we want to use
__DRIVER = "sqlite"

class DBInvalidBackend(DBStoreError):
    def __init__(self, msg, data):
        self.msg = msg
        self.data = data
        DBStoreError.__init__(self, msg, data)

def __get_driver(driver = __DRIVER):
    global __DRIVER
    if not driver:
        driver = __DRIVER
    if driver == "sqlite":
        try:
            from sqlite_drv import Database
        except ImportError:
            raise DBInvalidBackend(
                "Could not locate driver for backend '%s'" % (driver,),
                driver)
        else:
            return Database
    # postgresl support
    if driver == "postgresql":
        try:
            from postgresql_drv import Database
        except ImportError:
            raise DBInvalidBackend(
                "Could not locate driver for backend '%s'" % (driver,),
                driver)
        else:
            return Database
    # mysql support
    if driver == "mysql":
        try:
            from mysql_drv import Database
        except ImportError:
            raise DBInvalidBackend(
                "Could not locate driver for backend '%s'" % (driver,),
                driver)
        else:
            return Database
        
    raise DBInvalidBackend(
        "Database backend '%s' is not supported" % (driver,),
        driver)
    
# create a database connection and return an instance
# all drivers parse a db string in the form:
#   [[user[:password]@]host/]database
def connect(db, driver=None, *args, **kw):
    driver = __get_driver(driver)
    dbh = driver(db)
    assert(dbh.connect(*args, **kw))
    return dbh


__all__ = [ "connect",
            "DBStoreError", "DBStoreCursorError", "DBInvalidBackend" ]
