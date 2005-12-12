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

class DatabaseError(Exception):
    def __init__(self, args):
        self.args = args

    def __str__(self):
        return str(self.args)

class DatabaseLocked(DatabaseError):
    pass

class SchemaVersionError(DatabaseError):
    pass

class CursorError(DatabaseError):
    pass

class ColumnNotUnique(CursorError):
    pass

