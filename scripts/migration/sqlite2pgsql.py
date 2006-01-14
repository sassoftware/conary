#!/usr/bin/python
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

import sys
import os
if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/conary/scripts")

import time
import types

from conary import dbstore
from conary.dbstore import sqlerrors
from conary.server.schema import VERSION
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

# escape the row data based on the sqlite column types
def escape(cu, data):
    row = list(data)
    for i in range(len(data.data)):
        assert(data.description[i][1] in (0,1,6,8,9))
        if data.description[i][1] == 8:
            row[i] = cu.binary(data.data[i])
        elif data.description[i][1] == 9:
            row[i] = int(data.data[i])
    return tuple(row)

for t in tList:
    count = cs.execute("SELECT COUNT(ROWID) FROM %s" % t).fetchone()[0]
    i = 0
    cs.execute("SELECT * FROM %s" % t)
    t1 = time.time()
    while True:
        row = cs.fetchone()
        if row is None:
            break
        fields = cs.fields()
        row.data = list(row.data)
        row.description = [ list(x) for x in row.description ]
        if t.lower() == "permissions":
            if "write" in fields:
                fields[fields.index("write")] = "canWrite"
        if t.lower() == "users":
            if "user" in fields:
                fields[fields.index("user")] = "userName"
        if t.lower() == "trovetroves":
            if "byDefault" in fields:
                fields[fields.index("byDefault")] = "flags"
        if t.lower() == "trovefiles":
            # versionId was declared as a binary string in sqlite instead of integer
            row.data[2] = int(row.data[2])
            row.description[2][1] = 0
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            t, ", ".join(fields), ",".join(["?"]*len(fields)))
        i += 1
        try:
            cp.execute(sql, escape(cp, row))
        except sqlerrors.ColumnNotUnique:
            print "\r%s: SKIPPING DUPLICATE" % t, row
            pgsql.commit()
        except sqlerrors.ConstraintViolation, e:
            print
            print "%s: SKIPPED CONSTRAINT VIOLATION: %s" % (t, sql)
            print row, e.msg
            print
            pgsql.commit()
        except Exception, e:
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
