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

# There are certain schema changing operations that can not be done
# through the automatic schema migration (constraints, schema
# definition formatting, etc). This script will regenerate the sqlite
# schema into a new file and transfer data over

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
    print "Usage: migrate <sqldb-orig> <sqldb-new>"

source = dbstore.connect(sys.argv[1], driver = "sqlite")
cs = source.cursor()
dest = dbstore.connect(sys.argv[2], driver = "sqlite")
cp = dest.cursor()

# create the tables, avoid the indexes
for stmt in getTables("sqlite"):
    print stmt
    cp.execute(stmt)
dest.loadSchema()

for t in source.tables.keys():
    if t in dest.tables:
        continue
    print "Only in source:", t
for t in dest.tables.keys():
    if t in source.tables:
        continue
    print "Only in dest:", t

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

BATCH=5000

for t in tList:
    count = cs.execute("SELECT COUNT(ROWID) FROM %s" % t).fetchone()[0]
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
        if t.lower() == "users":
            if "user" in fields:
                fields[fields.index("user")] = "userName"
        if t.lower() == "trovetroves":
            if "byDefault" in fields:
                fields[fields.index("byDefault")] = "flags"
        if t.lower() == "trovefiles":
            # versionId was declared as a binary string in sqlite instead of integer
            rows = [list(row) for row in rows]
            for row in rows:
                row[2] = int(row[2])
            rows = [tuple(row) for row in rows]
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            t, ", ".join(fields), ",".join(["?"]*len(fields)))
        for row in rows:
            i += 1
            try:
                cp.execute(sql, tuple(row))
            except:
                print "ERROR - SQL", sql, row
                raise
        t2 = time.time()
        sys.stdout.write("\r%s: %s" % (t, timings(i, count, t1)))
        sys.stdout.flush()
        if i % (BATCH*5) == 0:
            dest.commit()
    print "\r%s: %s %s" % (t, timings(count, count, t1), " "*10)
    dest.commit()

# and now create the indexes
for stmt in getIndexes("sqlite"):
    print stmt
    cp.execute(stmt)
dest.setVersion(VERSION)
dest.commit()
