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
from conary.repository.netrepos.schema import VERSION
from schema import getTables, getIndexes

if len(sys.argv) != 3:
    print "Usage: migrate <sqlite_path> <pgsql_spec>"

sqlite = dbstore.connect(sys.argv[1], driver = "sqlite")
cs = sqlite.cursor()
pgsql = dbstore.connect(sys.argv[2], driver = "postgresql")
cp = pgsql.cursor()

# create the tables, avoid the indexes
for stmt in getTables("postgresql"):
    print stmt
    cp.execute(stmt)
pgsql.loadSchema()

for t in sqlite.tables.keys():
    if t in pgsql.tables:
        continue
    print "Only in sqlite:", t
for t in pgsql.tables.keys():
    if t in sqlite.tables:
        continue
    print "Only in pgsql:", t

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
    'EntitlementOwners',
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


for t in tList:
    break
    count = cs.execute("SELECT COUNT(ROWID) FROM %s" % t).fetchone()[0]
    i = 0
    cs.execute("SELECT * FROM %s" % t)
    t1 = time.time()
    while True:
        row = cs.fetchone()
        if row is None:
            break
        fields = cs.fields()
        if t.lower() == "permissions":
            if "write" in fields:
                fields[fields.index("write")] = "canWrite"
        if t.lower() == "users":
            if "user" in fields:
                fields[fields.index("user")] = "userName"
        if t.lower() == "trovefiles":
            # versionId was declared as a binary string in sqlite instead of integer
            row[2] = int(row[2])
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            t, ", ".join(fields), ",".join(["?"]*len(fields)))
        i += 1
        try:
            cp.execute(sql, tuple(row))
        except sqlerrors.ColumnNotUnique:
            print "\r%s: SKIPPING" % t, row
        except:
            print "ERROR - SQL", sql
            raise
        else:
            if i % 1000 == 0:
                t2 = time.time()
                sys.stdout.write("\r%s: %s" % (t, timings(i, count, t1)))
                sys.stdout.flush()
            if i % 10000 == 0:
                pgsql.commit()
    print "\r%s: %s %s" % (t, timings(count, count, t1), " "*10)
    pgsql.commit()

# and now create the indexes
for stmt in getIndexes("postgresql"):
    print stmt
    cp.execute(stmt)
pgsql.setVersion(VERSION)
pgsql.commit()
