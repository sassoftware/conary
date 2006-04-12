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

import os
import sys
if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/conary/scripts")

import time
import types

from conary import dbstore
from conary.dbstore import sqlerrors
from conary.server.schema import VERSION
from schema import getTables, getIndexes

# is set to True, insert statements are printed on stdout instead
DUMP = False

if len(sys.argv) != 3:
    sys.stderr.write("Usage: migrate <sqlite_path> <pgsql_spec>\n")

sqlite = dbstore.connect(sys.argv[1], driver = "sqlite")
sqlite.loadSchema()
cs = sqlite.cursor()

ingres = dbstore.connect(sys.argv[2], driver = "ingres")
ingres.loadSchema()
dest = ingres.cursor()

# create the tables, avoid the indexes
for stmt in getTables("ingres"):
    print stmt, ";\g"
    if not DUMP:
        dest.execute(stmt)
ingres.loadSchema()

for t in sqlite.tables.keys():
    if t in ingres.tables:
        continue
    sys.stderr.write("WARNING: Only in sqlite: %s\n" %(t,))
for t in ingres.tables.keys():
    if t in sqlite.tables:
        continue
    sys.stderr.write("WARNING: Only in ingres: %s\n" %(t,))

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

def str_esc(cu, row):
    ret = []
    for i in range(len(row)):
        assert(row.description[i][1] in (0,1,6,8,9))
        val = row.data[i]
        if row.description[i][1] == 8:
            ret.append( str(cu.binary(val)) )
        elif row.description[i][1] == 9:
            ret.append(str(int(val)))
        elif isinstance(val, int) or isinstance(val, float):
            ret.append(str(int(val)))
        elif val is None:
            ret.append("NULL")
        else:
            ret.append("'%s'" % (val,))
    return ret

BATCH = 1000

for t in tList:
    count = cs.execute("SELECT COUNT(ROWID) FROM %s" % t).fetchone()[0]
    i = 0
    cs.execute("SELECT * FROM %s" % t)
    t1 = time.time()
    dest.execute("MODIFY %s TO HEAP WITH EXTEND=1024" % (t,))
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
        if DUMP:
            print "INSERT INTO %s (%s) VALUES (%s) ;\g" % (
                t, ", ".join(fields), ",".join(str_esc(dest, row)))
        try:
            if not DUMP:
                dest.execute(sql, tuple(row))
        except sqlerrors.ColumnNotUnique:
            sys.stderr.write("\r%s: SKIPPING DUPLICATE %s" %(t, row))
            ingres.commit()
        except sqlerrors.ConstraintViolation, e:
            sys.stderr.write("\n%s: SKIPPED CONSTRAINT VIOLATION: %s\n%s %s\n\n" % (t, sql, row, e.msg))
            ingres.commit()
        except Exception, e:
            print "ERROR - SQL", sql
            raise
        else:
            if i % BATCH == 0:
                t2 = time.time()
                sys.stderr.write("\r%s: %s" % (t, timings(i, count, t1)))
                sys.stderr.flush()
            if i % (BATCH * 3) == 0:
                if DUMP:
                    print "COMMIT ;\g"
                else:
                    ingres.commit()
    sys.stderr.write("\r%s: %s %s\n" % (t, timings(count, count, t1), " "*10))
    sys.stderr.flush()
    if DUMP:
        print "COMMIT ;\g"
    else:
        ingres.commit()

# and now create the indexes
for stmt in getIndexes("ingres"):
    print stmt, ";\g"
    if not DUMP:
        dest.execute(stmt)
ingres.setVersion(VERSION)
ingres.commit()
