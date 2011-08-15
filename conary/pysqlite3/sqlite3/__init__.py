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


import conary._sqlite3 as _sqlite

"""Python interface to the SQLite embedded database engine."""

#------------------------------------------------------------------------------
# Module Information
#------------------------------------------------------------------------------

__revision__ = """$Revision: 1.4 $"""[11:-2]

threadsafety = 1
apilevel = "2.0"
paramstyle = "qmark"

# This is the version string for the current PySQLite version.
version = "0.0.0"

# This is a tuple with the same digits as the vesrion string, but it's
# suitable for comparisons of various versions.
version_info = (0, 0, 0)

#------------------------------------------------------------------------------
# Data type support
#------------------------------------------------------------------------------

from main import DBAPITypeObject, Cursor, Connection

STRING    = DBAPITypeObject(_sqlite.STRING)

BINARY    = DBAPITypeObject(_sqlite.BINARY)

INT       = DBAPITypeObject(_sqlite.INTEGER)

NUMBER    = DBAPITypeObject(_sqlite.INTEGER,
                            _sqlite.FLOAT)

DATE      = DBAPITypeObject(_sqlite.DATE)

TIME      = DBAPITypeObject(_sqlite.TIME)

TIMESTAMP = DBAPITypeObject(_sqlite.TIMESTAMP)

ROWID     = DBAPITypeObject()

# Nonstandard extension:
UNICODESTRING = DBAPITypeObject(_sqlite.UNICODESTRING)

#------------------------------------------------------------------------------
# Exceptions
#------------------------------------------------------------------------------

from conary._sqlite3 import Warning, Error, InterfaceError, \
    DatabaseError, DataError, OperationalError, IntegrityError, InternalError, \
    ProgrammingError, NotSupportedError

#------------------------------------------------------------------------------
# Global Functions
#------------------------------------------------------------------------------

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

from conary._sqlite3 import encode, decode

__all__ = ['connect','IntegrityError', 'InterfaceError', 'InternalError',
           'NotSupportedError', 'OperationalError',
           'ProgrammingError', 'Warning',
           'Connection', 'Cursor',
           'apilevel', 'paramstyle', 'threadsafety', 'version', 'version_info',
           'decode']
