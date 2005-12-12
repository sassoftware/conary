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

import sys
import re

import sqlerrors, sqllib

# base Cursor class. All backend drivers are expected to provide this
# interface
class BaseCursor:
    PASSTHROUGH = ["lastrowid"]

    def __init__(self, dbh=None):
        self.dbh = dbh
        self._cursor = self._getCursor()
        self.description = None

    # map some attributes back to self._cursor
    def __getattr__(self, name):
        if name in self.__dict__.keys():
            return self.__dict__[key]
        if name in self.PASSTHROUGH:
            return getattr(self._cursor, name)
        raise AttributeError("'%s' attribute is invalid" % (name,))

    # this will need to be provided by each separate driver
    def _getCursor(self):
        assert(self.dbh)
        return self.dbh.cursor()

    def execute(self, sql, *args, **kw):
        assert(len(sql) > 0)
        assert(self.dbh and self._cursor)
        self.description = None
        # force dbi compliance here
        if len(kw) == 0:
            if len(args) == 0:
                return self._cursor.execute(sql)
            if len(args) == 1:
                return self._cursor.execute(sql, args[0])
            return self._cursor.execute(sql, *args)
        if len(args) == 1 and isinstance(args[0], dict):
            kw.update(args[0])
        return self._cursor.execute(sql, **kw)

    # return the column names of the current select
    def __rowDict(self, row):
        assert(self._cursor and self._cursor.description)
        if row is None:
            return None
        if len(row) != len(self._cursor.description):
            raise sqlerrors.CursorError("Cursor description doew not match row data",
                                     row = row, desc = self._cursor.description)
        if not self.description:
            self.description = [ x[0] for x in self._cursor.description ]
        return dict(zip(self.description, row))

    # (a,b)
    def fetchone(self):
        return self._cursor.fetchone()
    # [(a1,b1),(a2, b2)]
    def fetchall(self):
        return self._cursor.fetchall()
    def fetchmany(self, count=1):
        return self._cursor.fetchmany(count)

    # { name_a : a, name_b : b }
    def fetchone_dict(self):
        try:
            return self.__rowDict(self.fetchone())
        except:
            pass
        return None
    # [ {name_a:a1, name_b:b1}, {name_a:a2, name_b:b2} ]
    def fetchall_dict(self):
        try:
            return [ self.__rowDict(row) for row in self.fetchall() ]
        except:
            pass
        return None
    def fetchmany_dict(self, count=1):
        try:
            return [ self.__rowDict(row) for row in self.fetchmany(count) ]
        except:
            pass
        return None

    def __iter__(self):
        return self

    def next(self):
        item = self.fetchone()
        if item is None:
            raise StopIteration
        else:
            return item

# A cursor class for the drivers that do not support bind
# parameters. Instead of :name they use a Python-esque %(name)s
# syntax. This is quite fragile...
class BindlessCursor(BaseCursor):
    def __mungeSQL(self, sql):
        regex = re.compile(':(\w+)')
        # edit the input query
        keys = set()
        for key in regex.findall(sql):
            keys.add(key)
            sql = re.sub(":" + key, "%("+key+")s", sql)
        sql = re.sub("(?P<c>[(,<>=])(\s+)?[?]", "\g<c> %s", sql)
        sql = re.sub("(?i)(?P<kw>LIKE|AND|BETWEEN|LIMIT|OFFSET)(\s+)?[?]", "\g<kw> %s", sql) 
        return (sql, keys)

    # we need to "fix" the sql code before calling out
    def execute(self, sql, *params, **kw):
        self.description = None
        sql, keys = self.__mungeSQL(sql)
        # figure out possible invocations
        if len(params) == 0:
            assert (sorted(kw.keys()) == sorted(keys))
        elif len(params) == 1:
            assert(isinstance(params, tuple))
            if isinstance(params[0], tuple):
                params = params[0]
            p = params[0]
            # if it is a dictionary, it must contain bind arguments
            if hasattr(p, 'keys'):
                kw.update(p)
            else: # special case - single positional argument
                assert(len(keys)==0 and len(kw)==0)
                return self._cursor.execute(sql, params)
        else: # many params, we don't mix in bind arguments
            assert(len(keys)==0 and len(kw)==0)
            return self._cursor.execute(sql, params)
        # we have a dict of bind arguments
        return self._cursor.execute(sql, kw)

# A class to handle database operations
class BaseDatabase:
    # need to figure out a statement generic enough for all kinds of backends
    alive_check = "select 1 where 1 = 1"
    basic_transaction = "begin transaction"
    cursorClass = BaseCursor
    type = "base"

    def __init__(self, db):
        assert(db)
        self.database = db
        self.dbh = None
        # stderr needs to be around to print errors. hold a reference
        self.stderr = sys.stderr

    # the string syntax for database connection is [[user[:password]@]host/]database
    def _connectData(self, names = ["user", "password", "host", "database"]):
        assert(self.database)
        assert(len(tuple(names)) == 4)
        # regexes are k001 and I am 1337 h@x0r
        regex = re.compile(
            "^(((?P<%s>[^:]+)(:(?P<%s>[^@]+))?@)?(?P<%s>[^/]+)/)?(?P<%s>.+)$" % tuple(names)
            )
        m = regex.match(self.database.strip())
        assert(m)
        return m.groupdict()

    def connect(self):
        assert(self.database)
        raise RuntimeError("This connect function has to be provided by the database driver")

    # reopens the database backend, if required. Kind of specific t sqlite-type backends
    def reopen(self):
        """Returns True if the database backend was reopenedor a
        reconnection was required"""
        return False

    def close(self):
        if self.dbh:
            self.dbh.close()
        self.dbh = self.database = None

    def alive(self):
        assert(self.dbh)
        try:
            c = self.cursor()
            c.execute(self.alive_check)
        except:
            # database connection lost
            return False
        else:
            del c
        return True

    def closed(self):
        return self.dbh is None

    def cursor(self):
        assert (self.dbh)
        return self.cursorClass(self.dbh)

    # perform the equivalent of a analyze on $self
    def analyze(self):
        assert(self.database)
        pass

    def commit(self):
        assert(self.dbh)
        return self.dbh.commit()

    # transaction support
    def transaction(self, name = None):
        "start transaction [ named point ]"
        # basic class does not support savepoints
        assert(not name)
        assert(self.dbh)
        c = self.cursor()
        c.execute(self.basic_transaction)
        return c

    def rollback(self, name=None):
        "rollback [ to transaction point ]"
        # basic class does not support savepoints
        assert(not name)
        assert(self.dbh)
        self.dbh.rollback()

    # easy access to the schema state
    def loadSchema(self):
        assert(self.dbh)
        # keyed by table, values are indexes on the table
        self.tables = sqllib.CaselessDict()
        self.views = []
        self.functions = []
        self.sequences = []
        self.version = 0

    def getVersion(self):
        assert(self.dbh)
        c = self.cursor()
        if 'DatabaseVersion' not in self.tables:
            self.version = 0
            return 0
        c.execute("select max(version) as version from DatabaseVersion")
        self.version = c.fetchone()[0]
        return self.version

    def setVersion(self, version):
        assert(self.dbh)
        c = self.cursor()
        assert (version >= self.getVersion())
        if 'DatabaseVersion' not in self.tables:
            c.execute("CREATE TABLE DatabaseVersion (version INTEGER)")
            c.execute("INSERT INTO DatabaseVersion (version) VALUES (0)")
        c.execute("UPDATE DatabaseVersion set version = ?", version)
        self.commit()
        # usually a setVersion occurs after some schema modification...
        self.loadSchema()
        return version

    # try to close it first nicely
    def __del__(self):
        if self.dbh is not None:
            try:
                self.dbh.close()
            except:
                pass
        self.dbh = self.database = None


# A class to handle calls to the SQL functions and procedures
class Callable:
    def __init__(self, name, call):
        self.name = name
        self.call = call
    def __call__(self, *args):
        assert(self.call is not None)
        apply(self.call, args)

