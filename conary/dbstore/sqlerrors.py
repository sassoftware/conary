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


class DatabaseError(Exception):
    def __init__(self, msg, *args, **kw):
        """
        A database error occurred.  A possible causes is incorrect SQL
        statement input.
        """
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
    """
    Database is locked and cannot be accessed by the current process.

    PUBLIC API
    """
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

class InvalidTable(CursorError):
    pass
