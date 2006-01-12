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
from conary.lib import cfg

# class for encapsulating binary strings for dumb drivers
class BaseBinary:
    def __init__(self, s):
        assert(isinstance(s, str))
        self.s = s
    def __str__(self):
        return self.s
    def __repr__(self):
        return self.s

# this will be derived by the backend drivers to handle schema creation
class BaseKeywordDict(dict):
    keys = { 'PRIMARYKEY' : 'INTEGER PRIMARY KEY',
             'BLOB'       : 'BLOB',
             'MEDIUMBLOB' : 'BLOB'                  }
    def __init__(self):
        dict.__init__(self, self.keys)

    def binaryVal(self, binLen):
        return "BINARY(%d)" % binLen

    def __getitem__(self, val):
        if val.startswith('BINARY'):
            binLen = val[6:]
            return self.binaryVal(int(binLen))

        return dict.__getitem__(self, val)

# base Cursor class. All backend drivers are expected to provide this
# interface
class BaseCursor:
    PASSTHROUGH = ["description", "lastrowid"]
    binaryClass = BaseBinary
    def __init__(self, dbh=None):
        self.dbh = dbh
        self._cursor = self._getCursor()

    # map some attributes back to self._cursor
    def __getattr__(self, name):
        if name in self.__dict__.keys():
            return self.__dict__[key]
        if name in self.PASSTHROUGH:
            return getattr(self._cursor, name)
        raise AttributeError("'%s' attribute is invalid" % (name,))

    def lastid(self):
        # wrapper for the lastrowid attribute.
        if hasattr(self._cursor, "lastrowid"):
            return self._cursor.lastrowid
        raise AttributeError("This driver does not know about `lastrowid`")

    # this will need to be provided by each separate driver
    def _getCursor(self):
        assert(self.dbh)
        return self.dbh.cursor()

    def binary(self, s):
        if s is None:
            return None
        return self.binaryClass(s)

    def frombinary(self, s):
        return s

    def execute(self, sql, *args, **kw):
        assert(len(sql) > 0)
        assert(self.dbh and self._cursor)
        # force dbi compliance here. we prefer args over the kw
        if len(args) == 0:
            return self._cursor.execute(sql, **kw)
        if len(args) == 1 and isinstance(args[0], dict):
            kw.update(args[0])
            return self._cursor.execute(sql, **kw)
        if len(kw):
            raise sqlerrors.CursorError(
                "Do not pass both positional and named bind arguments",
                *args, **kw)
        if len(args) == 1:
            return self._cursor.execute(sql, args[0])
        return self._cursor.execute(sql, *args)

    # return a list of the field names for the last select (if any)
    def fields(self):
        if not self._cursor.description:
            return None
        return [ x[0] for x in self._cursor.description ]

    # return the column names of the current select
    def __rowDict(self, row):
        assert(self._cursor)
        if row is None:
            return None
        if len(row) != len(self._cursor.description):
            raise sqlerrors.CursorError("Cursor description doew not match row data",
                                     row = row, desc = self._cursor.description)
        return dict(zip(self.fields(), row))

    # (a,b)
    def fetchone(self):
        return self._cursor.fetchone()
    # [(a1,b1),(a2, b2)]
    def fetchall(self):
        return list(self._cursor.fetchall())
    def fetchmany(self, count=1):
        return list(self._cursor.fetchmany(count))

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
##         for key in regex.findall(sql):
##             keys.add(key)
##             sql = re.sub(":" + key, "%("+key+")s", sql)
        sql = re.sub("(?P<c>[(,<>=])(\s+)?[?]", "\g<c> %s", sql)
        sql = re.sub("(?i)(?P<kw>LIKE|AND|BETWEEN|LIMIT|OFFSET)(\s+)?[?]", "\g<kw> %s", sql)
        return (sql, keys)

    # we need to "fix" the sql code before calling out
    def execute(self, sql, *args, **kw):
        assert(len(sql) > 0)
        assert(self.dbh and self._cursor)
        sql, keys = self.__mungeSQL(sql)
        # force dbi compliance here. we prefer args over the kw
        if len(args) == 1:
            if isinstance(args[0], (tuple, list)):
                args = args[0]
        if len(args) == 0:
            assert (sorted(kw.keys()) == sorted(keys))
        elif len(args) == 1:
            p = args[0]
            # if it is a dictionary, it must contain bind arguments
            if hasattr(p, 'keys'):
                kw.update(p)
            else: # special case - single positional argument
                assert(len(keys)==0 and len(kw)==0)
                return self._cursor.execute(sql, args)
        else: # many args, we don't mix in bind arguments
            assert(len(keys)==0 and len(kw)==0)
            return self._cursor.execute(sql, args)
        # we have a dict of bind arguments
        return self._cursor.execute(sql, **kw)


# A class for working with sequences
class BaseSequence:
    def __init__(self, db, name):
        assert(name)
        self.db = db
        self.name = name
        self.seqName = "%s_sequence" % (name,)
        self.__currval = None

    # if we have not called nextval() yet, then currval should be
    # undefined. To easily catch places where we might be doing that,
    # we raise an exception
    def currval(self):
        if self.__currval is None:
            raise sqlerrors.CursorError(
                "Sequence has currval undefined until nextval() is called",
                self.name)
        return self.__currval
    # this is meant to be provided by the drivers
    def nextval(self):
        pass
    # Enforce garbage collection to avoid circular deps
    def __del__(self):
        self.db = self.cu = None

# A class to handle database operations
class BaseDatabase:
    # need to figure out a statement generic enough for all kinds of backends
    alive_check = "select 1 where 1 = 1"
    basic_transaction = "begin transaction"
    cursorClass = BaseCursor
    sequenceClass = BaseSequence
    driver = "base"
    keywords = BaseKeywordDict()

    def __init__(self, db):
        assert(db)
        self.database = db
        self.dbh = None
        # stderr needs to be around to print errors. hold a reference
        self.stderr = sys.stderr
        self.closed = True
        self.tempTables = sqllib.CaselessDict()

    # the string syntax for database connection is [[user[:password]@]host[:port]/]database
    def _connectData(self, names = ["user", "password", "host", "port", "database"]):
        assert(self.database)
        assert(len(tuple(names)) == 5)
        # regexes are k001 and I am 1337 h@x0r
        regex = re.compile(
            "^(((?P<%s>[^:]+)(:(?P<%s>[^@]+)?)?@)?(?P<%s>[^/:]+)(:(?P<%s>[^/]+))?/)?(?P<%s>.+)$" % tuple(names)
            )
        m = regex.match(self.database.strip())
        assert(m)
        ret = m.groupdict()
        if ret["port"] is not None:
            ret["port"] = int(ret["port"])
        return ret

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
        self.dbh = None
        self.closed = True

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

    def sequence(self, name):
        assert(self.dbh)
        return self.sequenceClass(self, name)

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
        return self.dbh.rollback()

    # trigger schema handling
    def createTrigger(self, table, when, onAction, sql):
        assert(table in self.tables)
        name = "%s_%s" % (table, onAction)
        if name in self.triggers:
            return False
        sql = """
        CREATE TRIGGER %s %s %s ON %s
        FOR EACH ROW BEGIN
        %s
        END
        """ % (name, when.upper(), onAction.upper(), table, sql)
        cu = self.dbh.cursor()
        cu.execute(sql)
        self.triggers[name] = table
        return True
    def dropTrigger(self, table, onAction):
        onAction = onAction.lower()
        name = "%s_%s" % (table, onAction)
        if name in self.triggers:
            return False
        cu = self.dbh.cursor()
        cu.execute("DROP TRIGGER %s" % name)
        del self.triggers[name]
        return True

    # index schema handling
    def createIndex(self, table, name, columns, unique = False):
        assert(table in self.tables)
        if unique:
            unique = "UNIQUE"
        else:
            unique = ""
        if name in self.tables[table]:
            return False
        sql = "CREATE %s INDEX %s on %s (%s)" % (
            unique, name, table, columns)
        cu = self.dbh.cursor()
        cu.execute(sql)
        return True
    def dropIndex(self, table, name):
        if name not in self.tables[table]:
            return False
        sql = "DROP INDEX %s" % (name,)
        cu = self.dbh.cursor()
        cu.execute(sql)
        self.tables[table].remove(name)
        return True

    # easy access to the schema state
    def loadSchema(self):
        assert(self.dbh)
        # keyed by table, values are indexes on the table
        self.tables = sqllib.CaselessDict()
        self.views = sqllib.CaselessDict()
        self.functions = sqllib.CaselessDict()
        self.sequences = sqllib.CaselessDict()
        self.triggers = sqllib.CaselessDict()
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
        self.closed = True

# A class to handle calls to the SQL functions and procedures
class Callable:
    def __init__(self, name, call):
        self.name = name
        self.call = call
    def __call__(self, *args):
        assert(self.call is not None)
        apply(self.call, args)

# A class for configuration of a database driver
class CfgDriver(cfg.CfgType):

    def parseString(self, str):
        s = str.split()
        if len(s) != 2:
            raise ParseError, "database driver and path expected"

        return tuple(s)

    def format(self, val, displayOptions = None):
        return "%s %s" % val
