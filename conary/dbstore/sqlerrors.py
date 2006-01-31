#
# Copyright (c) 2005-2006 rPath, Inc.
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

class DatabaseError(Exception):
    def __init__(self, msg, *args, **kw):
        self.msg = str(msg)
        self.args = args
        self.kw = kw

    def __str__(self):
        ret = self.msg
        if len(self.args):
            ret += " args: " + str(self.args)
        if len(self.kw):
            ret += " kw: " + str(self.kw)
        return ret

class InvalidBackend(DatabaseError):
    pass

class DatabaseLocked(DatabaseError):
    pass

class ReadOnlyDatabase(DatabaseError):
    pass

class SchemaVersionError(DatabaseError):
    pass

class CursorError(DatabaseError):
    pass

class UnknownDatabase(DatabaseError):
    pass

class ColumnNotUnique(CursorError):
    pass

class ConstraintViolation(CursorError):
    pass

class DuplicateColumnName(CursorError):
    pass
