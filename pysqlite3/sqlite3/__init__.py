import _sqlite3 as _sqlite

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

from _sqlite3 import Warning, Error, InterfaceError, \
    DatabaseError, DataError, OperationalError, IntegrityError, InternalError, \
    ProgrammingError, NotSupportedError

#------------------------------------------------------------------------------
# Global Functions
#------------------------------------------------------------------------------

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

from _sqlite3 import encode, decode

__all__ = ['connect','IntegrityError', 'InterfaceError', 'InternalError',
           'NotSupportedError', 'OperationalError',
           'ProgrammingError', 'Warning',
           'Connection', 'Cursor',
           'apilevel', 'paramstyle', 'threadsafety', 'version', 'version_info',
           'decode']
