#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
