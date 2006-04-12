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

import os
import re
import sys
import time

import ingresdbi

from base_drv import BaseDatabase, BaseCursor, BaseSequence, BaseKeywordDict, BaseBinary
import sqlerrors, sqllib

class KeywordDict(BaseKeywordDict):
    keys = BaseKeywordDict.keys.copy()
    keys['PRIMARYKEY'] = 'INTEGER PRIMARY KEY NOT NULL'
    keys['MEDIUMBLOB'] = 'LONG BYTE'
    keys['BLOB'] = 'BYTE VARYING(32000)'

    def binaryVal(self, len):
        return "BYTE VARYING(%d)" % (len,)

# class for encapsulating binary strings for dumb drivers
class Binary(BaseBinary):
    def __str__(self):
        return "x'" + "".join("%02x" % ord(c) for c in self.s) + "'"

class Cursor(BaseCursor):
    driver = "ingres"
    binaryClass = Binary

    def execute(self, sql, *params, **kw):
        if kw.has_key("start_transaction"):
            del kw["start_transaction"]
        if len(kw):
            raise sqlerrors.CursorError("Ingres driver does not support keyword arguments")
        try:
            # normalize cu.execute(sql, a, b, c) -> cu.execute(sql, (a,b,c))
            # XXX: this should be done by the base driver
            if len(params) and not isinstance(params[0], tuple):
                ret = BaseCursor.execute(self, sql, params)
            else:
                ret = BaseCursor.execute(self, sql, *params)
        except ingresdbi.DataError, e:
            (err, msg) = (e.args[1], e.args[3])
            if err in [2117, 2753]:
                raise sqlerrors.InvalidTable(err, msg)
            if err == 4500:
                raise sqlerrors.ColumnNotUnique(err, msg)
            if err == 6406:
                raise sqlerrors.ConstraintViolation(err, msg)
            raise sqlerrors.CursorError(err, msg, e)
        return self

class Database(BaseDatabase):
    driver = "ingres"
    alive_check = "select date('now')"
    basic_transaction = "begin"
    cursorClass = Cursor
    keywords = KeywordDict()

    def connect(self, **kwargs):
        assert(self.database)
        cdb = self._connectData(["user", "passwd", "host", "port", "db"])
        for x in cdb.keys()[:]:
            if cdb[x] is None:
                del cdb[x]
        if kwargs.has_key("timeout"):
            del kwargs["timeout"]

        self.dbh = ingresdbi.connect(vnode = "@%s,tcp_ip,II;connection_type=direct" % (cdb["host"],),
                                     database = cdb["db"],
                                     uid = cdb["user"], pwd = cdb["passwd"])
        self.dbName = cdb['db']
        # reset the tempTables since we just lost them because of the (re)connect
        self.tempTables = sqllib.CaselessDict()
        self.closed = False
        return True

    def reopen(self):
        # make sure the connection is still valid by attempting a
        # ping.  If an exception happens, reconnect.
        if not self.alive():
            return self.connect()
        return False

    def loadSchema(self):
        BaseDatabase.loadSchema(self)
        c = self.cursor()
        c.execute("""
        select table_name, table_type
        from iitables
        where system_use = 'U'
        and table_type in ('V', 'T')
        """)
        slist = c.fetchall()
        if not len(slist):
            return self.version
        for (name, type) in slist:
            name = name.strip()
            if name.startswith("ii"):
                continue
            if type == "T":
                self.tables.setdefault(name, [])
            elif type == "V":
                self.views.setdefault(name, None)
        c.execute("""
        select index_name, base_name from iiindexes
        where system_use = 'U'
        """)
        ilist = c.fetchall()
        if not len(ilist):
            return self.getVersion()
        for (name, table) in ilist:
            if name.startswith("ii") or table.startswith("ii"):
                continue
            self.tables[table.strip()].append(name.strip())
        return self.getVersion()

    # A trigger that syncs up the changed column
    def createTrigger(self, table, column, onAction, pinned = False):
        return True
        onAction = onAction.lower()
        assert(onAction in ["insert into", "update of"])
        # first create the trigger function
        triggerName = "%s_%s" % (table, onAction)
        if triggerName in self.triggers:
            return False
        funcName = "%s_func" % triggerName
        cu = self.dbh.cursor()
        # XXX: fix pinned values
        if pinned:
            cu.execute("""
            CREATE PROCEDURE %s() AS
            BEGIN
                NEW.%s := OLD.%s ;
                RETURN NEW;
            END ;
            """ % (funcName, column, column))
        else:
            cu.execute("""
            CREATE PROCEDURE %s() AS
            BEGIN
                NEW.%s := _bintim() ;
                RETURN NEW;
            END ;
            """ % (funcName, column))
        # now create the trigger based on the above function
        cu.execute("""
        CREATE RULE %s
        BEFORE %s %s
        FOR EACH ROW
        EXECUTE PROCEDURE %s()
        """ % (triggerName, onAction, table, funcName))
        self.triggers[triggerName] = table
        return True

