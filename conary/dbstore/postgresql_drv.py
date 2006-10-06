#
# Copyright (c) 2005-2006 rPath, Inc.
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

import re
import pgsql

from base_drv import BaseDatabase, BaseCursor, BaseBinary
from base_drv import BaseKeywordDict
import sqlerrors
import sqllib

class KeywordDict(BaseKeywordDict):
    keys = BaseKeywordDict.keys.copy()
    keys.update( { 'BLOB' : 'BYTEA',
                   'MEDIUMBLOB' : 'BYTEA',
                   'PRIMARYKEY' : 'SERIAL PRIMARY KEY' } )

    def binaryVal(self, len):
        return "BYTEA"

# class for encapsulating binary strings for dumb drivers
class Binary(BaseBinary):
    __binary__ = True
    def __quote__(self):
        return self.s
    def __pg_repr__(self):
        return "decode('%s','hex')" % "".join("%02x" % ord(c) for c in self.s)

# edit the input query to make it postgres compatible
def _mungeSQL(sql):
    keys = [] # needs to be a list because we're dealing with positional args
    def __match(m):
        d = m.groupdict()
        kw = d["kw"][1:]
        if len(kw): # a real keyword
            if kw not in keys:
                keys.append(kw)
            d["kwIdx"] = keys.index(kw)+1
        else: # if we have just the ? then kw is "" here
            keys.append(None)
            d["kwIdx"] = len(keys)
        return "%(pre)s%(s)s$%(kwIdx)d" % d

    sql = re.sub("(?i)(?P<pre>[(,<>=]|(LIKE|AND|BETWEEN|LIMIT|OFFSET)\s)(?P<s>\s*)(?P<kw>:\w+|[?])",
                 __match, sql)
    # force dbi compliance here. args or kw or none, no mixes
    if len(keys) and keys[0] is not None:
        return (sql, keys)
    return (sql, [])

class Cursor(BaseCursor):
    binaryClass = Binary
    driver = "postgresql"

##     def binary(self, s):
##         return s

##     def frombinary(self, s):
##         #return s.decode("string_escape")
##         return s

    # execute with exception translation
    def _tryExecute(self, func, *params, **kw):
        try:
            ret = func(*params, **kw)
        except pgsql.DatabaseError, e:
            msg = e.args[0]
            if msg.find("violates foreign key constraint") > 0:
                raise sqlerrors.ConstraintViolation(msg)
            if re.search('relation \S+ does not exist', msg, re.I):
                raise sqlerrors.InvalidTable(msg)
            if msg.find("duplicate key violates unique constraint") > 0:
                raise sqlerrors.ColumnNotUnique(msg)
            raise sqlerrors.CursorError(msg, e)
        return ret

    # we need to "fix" the sql code before calling out
    def execute(self, sql, *args, **kw):
        self._executeCheck(sql)
        keys = []

        kw.pop("start_transaction", True)
        args, kw  = self._executeArgs(args, kw)

        # don't do unnecessary work
        if len(args) or len(kw):
            sql, keys = _mungeSQL(sql)

        # if we have args, we can not have keywords
        if len(args):
            if len(kw) or len(keys):
                raise sqlerrors.CursorError(
                    "Do not pass both positional and named bind arguments",
                    *args, **kw)
            ret = self._tryExecute(self._cursor.execute, sql, args)
        elif len(keys): # check that all keys used in the query appear in the kw
            if False in [kw.has_key(x) for x in keys]:
                raise CursorError(
                    "Query keys not defined in named argument dict",
                    sorted(keys), sorted(kw.keys()))
            # need to transform kw into pozitional args
            ret = self._tryExecute(self._cursor.execute, sql,
                                   [kw[x] for x in keys])
        else:
            ret = self._tryExecute(self._cursor.execute, sql)
        if ret == self._cursor:
            return self
        return ret

    # executemany - we have to process the query code
    def executemany(self, sql, argList, **kw):
        self._executeCheck(sql)
        kw.pop("start_transaction", True)
        sql, keys = _mungeSQL(sql)
        if len(keys):
            # need to transform the dicts in tuples for the query
            return self._tryExecute(self._cursor.executemany, sql,
                                    (tuple([row[x] for x in keys]) for row in argList))
        return self._tryExecute(self._cursor.executemany, sql, argList)

    # support for prepared statements
    def compile(self, sql):
        self._executeCheck(sql)
        sql, keys = _mungeSQL(sql.strip())
        stmt = self.dbh.prepare(sql)
        stmt.keys = keys
        return stmt
    def execstmt(self, stmt, *args):
        assert(isinstance(stmt, pgsql.PreparedCursor))
        if not len(args):
            ret = self._tryExecute(stmt._source.execute)
        elif isinstance(args[0], (tuple, list)):
            ret = self._tryExecute(stmt._source.execute, *args)
        else:
            ret = self._tryExecute(stmt._source.execute, args)
        if isinstance(ret, int):
            return ret
        return stmt

    # override this with the native version
    def fields(self):
        return self._cursor.fields

    # pgsql has its own fetch*_dict methods
    def fetchone_dict(self):
        ret = self._cursor.fetchone_dict()
        return sqllib.CaselessDict(ret)
    def fetchmany_dict(self, size):
        return [ sqllib.CaselessDict(x) for x in self._cursor.fetchmany_dict(size) ]
    def fetchall_dict(self):
        return [ sqllib.CaselessDict(x) for x in self._cursor.fetchall_dict() ]

    # we have "our own" lastrowid
    def __getattr__(self, name):
        if name == "lastrowid":
            return self.lastid()
        return BaseCursor.__getattr__(self, name)

    # postgresql can not report back the last value from a SERIAL
    # PRIMARY KEY column insert, so we have to look it up ourselves
    def lastid(self):
        ret = self.execute("select lastval()").fetchone()
        if ret is None:
            return 0
        return ret[0]

# PostgreSQL lowercase everything automatically, so we need a special
# "lowercase match" list type for matches like
# idxname in db.tables[x]
class Llist(list):
    def __contains__(self, item):
        return item.lower() in [x.lower() for x in list.__iter__(self)]

class Database(BaseDatabase):
    driver = "postgresql"
    avail_check = "select count(*) from pg_tables"
    cursorClass = Cursor
    keywords = KeywordDict()

    def connect(self, **kwargs):
        assert(self.database)
        cdb = self._connectData()
        if not cdb.get("port", None):
            cdb["port"] = -1
        try:
            self.dbh = pgsql.connect(**cdb)
        except pgsql.InternalError:
            raise sqlerrors.DatabaseError("Could not connect to database", cdb)
        # reset the tempTables since we just lost them because of the (re)connect
        self.tempTables = sqllib.CaselessDict()
        self.closed = False
        return True

    def loadSchema(self):
        BaseDatabase.loadSchema(self)
        c = self.cursor()
        # get tables
        c.execute("""
        select tablename as name
        from pg_tables
        where schemaname not in ('pg_catalog', 'pg_toast',
                                 'information_schema')
        """)
        for table, in c.fetchall():
            self.tables[table] = Llist()
        if not len(self.tables):
            return self.version
        # views
        c.execute("""
        select viewname as name
        from pg_views
        where schemaname not in ('pg_catalog', 'pg_toast',
                                 'information_schema')
        """)
        for name, in c.fetchall():
            self.views[name] = True
        # indexes
        c.execute("""
        select indexname as name, tablename as table
        from pg_indexes
        where schemaname not in ('pg_catalog', 'pg_toast',
                                 'information_schema')
        """)
        for (name, table) in c.fetchall():
            self.tables.setdefault(table, Llist()).append(name)
        # sequences. I wish there was a better way...
        c.execute("""
        SELECT c.relname as name
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
        AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
        AND pg_catalog.pg_table_is_visible(c.oid)
        """)
        for name, in c.fetchall():
            self.sequences[name] = True
        # triggers
        c.execute("""
        SELECT t.tgname, c.relname
        FROM pg_catalog.pg_trigger t, pg_class c, pg_namespace n
        WHERE t.tgrelid = c.oid AND c.relnamespace = n.oid
        AND NOT tgisconstraint
        AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
        """)
        for (name, table) in c.fetchall():
            self.triggers[name] = table
        version = self.getVersion()
        return version

    # Postgresql's trigegr syntax kind of sucks because we have to
    # create a function first and then call that function from the
    # trigger
    def createTrigger(self, table, column, onAction, pinned = False):
        onAction = onAction.lower()
        assert(onAction in ["insert", "update"])
        # first create the trigger function
        triggerName = "%s_%s" % (table, onAction)
        if triggerName in self.triggers:
            return False
        funcName = "%s_func" % triggerName
        cu = self.dbh.cursor()
        if pinned:
            cu.execute("""
            CREATE OR REPLACE FUNCTION %s()
            RETURNS trigger
            AS $$
            BEGIN
                NEW.%s := OLD.%s ;
                RETURN NEW;
            END ; $$ LANGUAGE 'plpgsql';
            """ % (funcName, column, column))
        else:
            cu.execute("""
            CREATE OR REPLACE FUNCTION %s()
            RETURNS trigger
            AS $$
            BEGIN
                NEW.%s := TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'), '99999999999999') ;
                RETURN NEW;
            END ; $$ LANGUAGE 'plpgsql';
            """ % (funcName, column))
        # now create the trigger based on the above function
        cu.execute("""
        CREATE TRIGGER %s
        BEFORE %s ON %s
        FOR EACH ROW
        EXECUTE PROCEDURE %s()
        """ % (triggerName, onAction, table, funcName))
        self.triggers[triggerName] = table
        return True

    # avoid leaving around invalid transations when schema is not initialized
    def getVersion(self):
        ret = BaseDatabase.getVersion(self)
        if ret == 0:
            # need to rollback the last transaction
            self.dbh.rollback()
        return ret
