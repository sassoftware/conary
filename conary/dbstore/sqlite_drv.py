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
import re

from conary import sqlite3
from conary.lib.tracelog import logMe

from base_drv import BaseDatabase, BaseCursor, BaseSequence
import sqlerrors

# implement the regexp function for sqlite
def _regexp(pattern, item):
    regexp = re.compile(pattern)
    return regexp.match(item) is not None

class Cursor(BaseCursor):
    driver = "sqlite"

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
            ret = self._execute(sql, *params, **kw)
        except sqlite3.ProgrammingError, e:
            #if self.dbh.inTransaction:
            #    self.dbh.rollback()
            if e.args[0].startswith("column") and e.args[0].endswith("not unique"):
                raise sqlerrors.ColumnNotUnique(e)
            elif e.args[0] == 'attempt to write a readonly database':
                raise sqlerrors.ReadOnlyDatabase(str(e))
            raise sqlerrors.CursorError(e.args[0], e)
        else:
            return ret

    # deprecated - this breaks programs by commiting stuff before its due time
    def executeWithCommit(self, sql, *params, **kw):
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

# Sequence implementation for sqlite
class Sequence(BaseSequence):
    def __init__(self, db, name, cu = None):
        BaseSequence.__init__(self, db, name)
        self.cu = cu
        if cu is None:
            self.cu = db.cursor()
        if name in db.sequences:
            return
        self.cu.execute("""
        CREATE TABLE %s_sequence (
            val         INTEGER PRIMARY KEY AUTOINCREMENT
        )""" % (name,))
        # refresh schema
        db.loadSchema()

    def nextval(self):
        # We have to make sure we do this in a transaction
        if not self.db.dbh.inTransaction:
            self.db.transaction()
        self.cu.execute("DELETE FROM %s" % self.seqName)
        self.cu.execute("INSERT INTO %s VALUES(NULL)" % self.seqName)
        self.cu.execute("SELECT val FROM %s" % self.seqName)
        self.__currval = self.cu.fetchone()[0]
        return self.__currval

    # Enforce garbage collection to avoid circular deps
    def __del__(self):
        if self.db.dbh.inTransaction and self.__currval is not None:
            self.db.commit()
        self.db = self.cu = None

class Database(BaseDatabase):
    driver = "sqlite"
    alive_check = "select count(*) from sqlite_master"
    cursorClass = Cursor
    sequenceClass = Sequence
    basic_transaction = "begin immediate"
    VIRTUALS = [ ":memory:" ]
    TIMEOUT = 10000

    def connect(self, **kwargs):
        assert(self.database)
        cdb = self._connectData()
        assert(cdb["database"])
        kwargs.setdefault("timeout", self.TIMEOUT)
        #kwargs.setdefault("command_logfile", open("/tmp/sqlite.log", "a"))
        try:
            self.dbh = sqlite3.connect(cdb["database"], **kwargs)
        except sqlite3.InternalError, e:
            if str(e) == 'database is locked':
                raise sqlerrors.DatabaseLocked(e)
            raise
        # add a regexp funtion to enable SELECT FROM bar WHERE bar REGEXP .*
        self.dbh.create_function('regexp', 2, _regexp)
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
                    self.sequences.setdefault(name[:-len("_sequence")], None)
                else:
                    self.tables.setdefault(name, [])
            elif type == "view":
                self.views.setdefault(name, None)
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
                raise sqlerrors.ReadOnlyDatabase(str(e))
            raise

