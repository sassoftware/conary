#!/usr/bin/python

import os
import sys
fullPath = os.path.dirname(sys.argv[0])
if fullPath in [ "", "."]:
    fullPath = os.getcwd()
else:
    if fullPath[0] != "/":
        fullPath = os.getcwd() + "/" + fullPath

sys.path.insert(0, os.path.dirname(fullPath))

import re
from conary.dbstore import sqlerrors
from conary.repository.netrepos import schema

class PrintDatabase:
    keywords = {
        'PRIMARYKEY' : 'INTEGER PRIMARY KEY',
        'BINARY'     : 'BINARY',
        'BLOB'       : 'BLOB',
        'MEDIUMBLOB' : 'BLOB',
        }
    def __init__(self, showTables = True, driver="sqlite"):
        self.tables = self.views = self.sequences = []
        self.tempTables = []
        self.version = 0
        self.showTables = showTables
        self.statements = []
        self.driver = driver
        if self.driver == "sqlite":
            self.keywords['PRIMARYKEY'] = 'INTEGER PRIMARY KEY AUTOINCREMENT'
        elif driver =="mysql":
            self.keywords['PRIMARYKEY'] = 'INTEGER PRIMARY KEY AUTO_INCREMENT'
        elif driver == "postgresql":
            self.keywords['BINARY'] = 'VARCHAR'
            self.keywords['BLOB'] = 'BYTEA'
            self.keywords['MEDIUMBLOB'] = 'BYTEA'
            self.keywords['PRIMARYKEY'] = 'SERIAL PRIMARY KEY'

    def connect(self, *args, **kwargs):
        pass
    def commit(self):
        pass
    def cursor(self):
        return self
    def loadSchema(self):
        pass
    # simulate non-existent tables for delete statements
    def __skip_delete(self, sql):
        delfrom = re.compile("(?i)DELETE\s+FROM.*")
        if delfrom.match(sql):
            raise sqlerrors.DatabaseError
        return False
    # ignore create temporary tables
    def __skip_tempTables(self, sql):
        tmptbl = re.compile("(?i)CREATE\s+TEMPORARY\s+TABLE\s+(?P<table>[^ (]+).*")
        m = tmptbl.match(sql)
        if m is not None:
            d = m.groupdict()
            # remember this temporary table
            self.tempTables.append(d["table"].strip())
            return True
        return False
    # ignore indexes for temporary tables
    def __skip_Indexes(self, sql, skipAll = False):
        tmpidx = re.compile("(?i)CREATE\s+(UNIQUE\s+)?INDEX\s+\S+\s+ON\s+(?P<table>[^ (]+).*")
        m = tmpidx.match(sql)
        if m is not None:
            d = m.groupdict()
            # remember this temporary table
            if skipAll or d["table"] in self.tempTables:
                return True
        return False
    def __skip_Triggers(self, sql, skipAll = False):
        tmptrg = re.compile("(?i)CREATE\s+TRIGGER")
        if tmptrg.match(sql):
            return skipAll
        return False
    def __skip_Tables(self, sql, skipAll = False):
        tbl = re.compile(
            "^(?i)(CREATE|ALTER)\s+(TABLE\s+(?P<table>[^(]+)|VIEW\s+(?P<view>[^( ]+))\s*([(]|ADD|AS).*"
            )
        m = tbl.match(sql)
        if m is not None:
            d = m.groupdict()
            if d["table"]: self.tables.append(d["table"].strip())
            if d["view"]: self.views.append(d["view"].strip())
            return skipAll
        return False

    def execute(self, sql, *args, **kwargs):
        sql = sql.strip()
        # skip the parametrized schema definitions
        if args and "?" in sql:
            return
        if self.__skip_delete(sql):
            return
        if self.__skip_tempTables(sql):
            return
        if self.__skip_Indexes(sql, self.showTables):
            return
        if self.__skip_Triggers(sql, self.showTables):
            return
        if self.__skip_Tables(sql, not self.showTables):
            return
        into = re.compile("^(?i)(INSERT INTO).*")
        # we don't do inserts because they're ot part of te schema definition
        if into.match(sql):
            return
        self.statements.append(sql)

    def trigger(self, table, column, onAction, sql = ""):
        onAction = onAction.lower()
        name = "%s_%s" % (table, onAction)
        assert(onAction in ["insert", "update"])
        if self.driver == "sqlite":
            # prepare the sql and the trigger name and pass it to the
            # BaseTrigger for creation
            when = "AFTER"
            if onAction == "insert":
                when = "BEFORE"
            sql = ("UPDATE %s SET %s = unix_timestamp() WHERE id = NEW.id ; "
                   "%s " % (table, column, sql))
        elif self.driver == "mysql":
            when = "BEFORE"
            # force the current_timestamp into a numeric context
            sql = "SET NEW.%s = current_timestamp() + 0 ; %s" % (column, sql)
        elif self.driver == "postgresql":
            pass
        else:
            raise NotImplementedError
        sql = """
        CREATE TRIGGER %s %s %s ON %s
        FOR EACH ROW BEGIN
        %s
        END
        """ % (name, when.upper(), onAction.upper(), table, sql)
        self.execute(sql)

    def setVersion(self, version):
        self.version = version
    def getVersion(self):
        return self.version

def getTables(driver = "mysql"):
    pd = PrintDatabase(True, driver)
    schema.checkVersion(pd)
    return pd.statements

def getIndexes(driver = "mysql"):
    pd = PrintDatabase(False, driver)
    schema.checkVersion(pd)
    return pd.statements

if __name__ == '__main__':
    for x in getTables(): print x
    for x in getIndexes(): print x
