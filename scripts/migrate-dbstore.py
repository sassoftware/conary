#!/usr/bin/python

import sys
import os
if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/conary/scripts")

import time
import types

from conary import dbstore
from conary.dbstore import sqlerrors
from conary.repository.netrepos import schema
from printSchema import getTables, getIndexes

if len(sys.argv) != 3:
    print "Usage: migrate <sqlite_path> <mysql_spec>"

sqlite = dbstore.connect(sys.argv[1], driver = "sqlite")
cs = sqlite.cursor()
mysql = dbstore.connect(sys.argv[2], driver = "mysql")
cm = mysql.cursor()

# create the tables, avoid the indexes
for stmt in getTables():
    cm.execute(stmt)
    print stmt
mysql.loadSchema()
cm.execute("SET SESSION AUTOCOMMIT = 0")

for t in sqlite.tables.keys():
    if t in mysql.tables:
        continue
    print "Only in sqlite:", t
for t in mysql.tables.keys():
    if t in sqlite.tables:
        continue
    print "Only in mysql:", t

tList = [
    'Branches',
    'Items',
    'Versions',
    'Labels',
    'LabelMap',
    'Flavors',
    'FlavorMap',
    'FlavorScores',
    'Users',
    'UserGroups',
    'UserGroupMembers',
    'EntitlementGroups',
    'Entitlements',
    'Permissions',
    'Instances',
    'Dependencies',
    'Latest',
    'Metadata',
    'MetadataItems',
    'Nodes',
    'ChangeLogs',
    'PGPKeys',
    'PGPFingerprints',
    'Provides',
    'Requires',
    'TroveTroves',
    'TroveInfo',
    'FileStreams',
    'TroveFiles',
    ]

BATCH=400
TICK=10

def hexstr(s):
    return "".join("%02x" % ord(c) for c in s)

def sqlstr(val):
    if isinstance(val, (types.IntType, types.FloatType)):
        return str(val)
    elif isinstance(val, types.StringType):
        return "x'%s'" % hexstr(val)
    elif val is None:
        return "NULL"
    elif isinstance(val, tuple):
        return "(" + ",".join([sqlstr(x) for x in val]) + ")"
    # ugly sqlite hack - why does sqlite makes it so hard to get a
    # real tuple out of a row without peeking inside the instance
    # structure?!
    elif hasattr(val, "data"):
        return sqlstr(val.data)
    else:
        raise AttributeError("We're not handling a value correctly", val, type(val))


def timings(current, total, tstart):
    tnow = time.time()
    tpassed = max(tnow-tstart,1)
    speed = max(current/tpassed,1)
    tremaining=(total-current)/speed
    return "%d/%d %02d%% (%d rec/sec, %d:%02d passed, %d:%02d remaining)" % (
        current, total, (current*100)/max(total,1),
        speed,
        tpassed/60, tpassed % 60,
        tremaining/60, tremaining % 60)

def slow_insert(t, fields, rows):
    global mysql, cm
    for row in rows:
        assert(len(fields) == len(row))
        sql = "INSERT INTO %s (%s) VALUES %s" % (
            t, ", ".join(fields), sqlstr(row))
        try:
            cm.execute(sql)
        except sqlerrors.ConstraintViolation, e:
            print
            print "%s: SKIPPED CONSTRAINT VIOLATION: %s" % (t, sql)
            print e.msg
            print
    mysql.commit()

for t in tList:
    count = cs.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
    i = 0
    cs.execute("SELECT * FROM %s" % t)
    t1 = time.time()
    while True:
        rows = cs.fetchmany(BATCH)
        if not len(rows):
            break
        fields = cs.fields()
        if t.lower() == "permissions":
            if "write" in fields:
                fields[fields.index("write")] = "canWrite"
        if t.lower() == "trovefiles":
            # versionId was declared as a binary string in sqlite instead of integer
            rows = [list(row) for row in rows]
            for row in rows:
                row[2] = int(row[2])
            rows = [tuple(row) for row in rows]
        sql = "INSERT INTO %s (%s) VALUES" % (t, ", ".join(fields))
        sql += ", ".join([sqlstr(row) for row in rows])
        i += len(rows)
        try:
            cm.execute(sql)
        except sqlerrors.ColumnNotUnique:
            print "\r%s: SKIPPING" % t, row
        except sqlerrors.ConstraintViolation:
            slow_insert(t, fields, rows)
        except:
            print "ERROR - SQL", sql
            raise
        else:
            if i % (BATCH*TICK) == 0:
                t2 = time.time()
                sys.stdout.write("\r%s: %s" % (t, timings(i, count, t1)))
                sys.stdout.flush()
            if i % (BATCH*TICK*5) == 0:
                mysql.commit()
    print "\r%s: %s %s" % (t, timings(count, count, t1), " "*10)
    mysql.commit()

# and now create the indexes
wtList = ["%s WRITE" % x for x in tList]
sql = "LOCK TABLES %s" % ", ".join(wtList)
for stmt in getIndexes():
    # in MySQL, tables need to be locked every time we create an index
    cm.execute(sql)
    print stmt
    cm.execute(stmt)
mysql.setVersion(schema.VERSION)
mysql.commit()
