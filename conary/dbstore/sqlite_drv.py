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

import os

from conary import sqlite3
from conary.lib.tracelog import logMe

from base_drv import BaseDatabase, BaseCursor
import sqlerrors

class Cursor(BaseCursor):
    type = "sqlite"

    # this is basically the BaseCursor's execute with special handling
    # for start_transaction
    def _execute(self, sql, *args, **kw):
        assert(len(sql) > 0)
        assert(self.dbh and self._cursor)
        self.description = None
        # force dbi compliance here. we prefer args over the kw
        if len(args) == 0:
            return self._cursor.execute(sql, **kw)
        if len(args) == 1 and isinstance(args[0], dict):
            kw.update(args[0])
            return self._cursor.execute(sql, **kw)
        # special case the start_transaction parameter
        st = kw.get("start_transaction", True)
        if kw.has_key("start_transaction"):
            del kw["start_transaction"]
        if len(kw):
            raise sqlerrors.CursorError(
                "Do not pass both positional and named bind arguments",
                *args, **kw)
        if len(args) == 1:
            return self._cursor.execute(sql, args[0], start_transaction = st)
        kw["start_transaction"] = st
        return self._cursor.execute(sql, *args, **kw)

    def execute(self, sql, *params, **kw):
        #logMe(3, "SQL:", sql, params, kw)
        try:
            inAutoTrans = False
            if not self.dbh.inTransaction:
                inAutoTrans = True
            ret = self._execute(sql, *params, **kw)
            # commit any transactions which were opened automatically
            # by the sqlite3 bindings and left hanging:
            if inAutoTrans and self.dbh.inTransaction:
                self.dbh.commit()
        except sqlite3.ProgrammingError, e:
            if inAutoTrans and self.dbh.inTransaction:
                self.dbh.rollback()
            if e.args[0].startswith("column") and e.args[0].endswith("not unique"):
                raise sqlerrors.ColumnNotUnique(e)
            raise sqlerrors.CursorError(e)
        except:
            if inAutoTrans and self.dbh.inTransaction:
                self.dbh.rollback()
            raise
        return ret

class Database(BaseDatabase):
    type = "sqlite"
    alive_check = "select count(*) from sqlite_master"
    cursorClass = Cursor
    basic_transaction = "begin immediate"
    VIRTUALS = [ ":memory:" ]

    def connect(self, timeout=10000):
        assert(self.database)
        cdb = self._connectData()
        assert(cdb["database"])
        # FIXME: we should channel exceptions into generic exception
        # classes common to all backends
        try:
            self.dbh = sqlite3.connect(cdb["database"], timeout=timeout)
        except sqlite3.InternalError, e:
            if str(e) == 'database is locked':
                raise sqlerrors.DatabaseLocked(e)
            raise
        self.loadSchema()
        if self.database in self.VIRTUALS:
            self.inode = (None, None)
            self.closed = False
            return True
	sb = os.stat(self.database)
        self.inode= (sb.st_dev, sb.st_ino)
        self.closed = False
        return True

    def reopen(self):
        if self.database in self.VIRTUALS:
            return False
        sb = os.stat(self.database)
        inode= (sb.st_dev, sb.st_ino)
	if self.inode != inode:
            self.dbh.close()
            del self.dbh
            return self.connect()
        return False

    def loadSchema(self):
        BaseDatabase.loadSchema(self)
        c = self.cursor()
        c.execute("select type, name, tbl_name from sqlite_master")
        slist = c.fetchall()
        if not len(slist):
            return self.version
        for (type, name, tbl_name) in slist:
            if type == "table":
                if name.endswith("_sequence"):
                    self.sequences.append(name[:-len("_sequence")])
                else:
                    self.tables.setdefault(name, [])
            elif type == "view":
                self.views.append(name)
            elif type == "index":
                self.tables.setdefault(tbl_name, []).append(name)
        return self.getVersion()

    def analyze(self):
        if sqlite3._sqlite.sqlite_version_info() <= (3, 2, 2):
            # ANALYZE didn't appear until 3.2.3
            return

        # perform table analysis to help the optimizer
        doAnalyze = False
        cu = self.cursor()
        # if there are pending changes, just re-run ANALYZE
        if self.dbh.inTransaction:
            doAnalyze = True
        else:
            # check to see if the sqlite_stat1 table exists.
            # ANALYZE creates it.
            if "sqlite_stat1" not in self.tables:
                doAnalyze = True

        if doAnalyze:
            cu.execute('ANALYZE')
            self.loadSchema()

    def transaction(self, name = None):
        try:
            return BaseDatabase.transaction(self, name)
        except sqlite3.ProgrammingError, e:
            if str(e) == 'attempt to write a readonly database':
                raise sqlerrors.ReadOnlyDatabase
            raise

