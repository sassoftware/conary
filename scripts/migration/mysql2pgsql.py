#!/usr/bin/env python2.4
#
# Copyright (c) 2006 rPath, Inc.
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

import sys
import os
if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/conary/scripts")

import time
import types

from conary import dbstore
from conary.dbstore import sqlerrors, sqllib
from conary.server.schema import VERSION
from schema import getTables, getIndexes

import epdb

if len(sys.argv) != 3:
    print "Usage: migrate <mysql_path> <pgsql_spec>"
    sys.exit(-1)

srcdb = dbstore.connect(sys.argv[1], driver = "mysql")
srcdb.loadSchema()
src = srcdb.itercursor()
pgsql = dbstore.connect(sys.argv[2], driver = "postgresql")
pgsql.loadSchema()
dst = pgsql.cursor()

# create the tables, avoid the indexes
for stmt in getTables("postgresql"):
    print stmt
    dst.execute(stmt)
pgsql.loadSchema()

for t in srcdb.tables.keys():
    if t in pgsql.tables:
        continue
    print "Only in mysql:", t
for t in pgsql.tables.keys():
    if t in srcdb.tables:
        continue
    print "Only in pgsql:", t

tList = [
    'LatestMirror',
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
    'EntitlementAccessMap',
    'Caps',
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
    'TroveRedirects',
    'TroveTroves',
    'TroveInfo',
    'FileStreams',
    'TroveFiles',
    ]
skip = ['databaseversion', 'instructionsets']
knowns = [x.lower() for x in tList]
missing = []
for t in srcdb.tables:
    tl = t.lower()
    if tl in skip:
        continue
    if tl not in knowns:
        missing.append(tl)
if len(missing):
    raise RuntimeError("tList needs to be updated to handle tables", missing)

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

# fields which we should tag as binary
def getfunc(field):
    binaryCols = [
        "salt", "password", "data", "entitlement", "pgpkey",
        "stream", "fileid", "pathid", "sha1" ]
    if field.lower() in binaryCols:
        return dst.binary
    return lambda a: a

# check that the destination table knows about all the tList items
# we're about to process
for t in tList:
    if t not in pgsql.tables:
        raise RuntimeError("Destination schema does not have table", t)
    # check the have similar schemas
    src.execute("SELECT * FROM %s LIMIT 0" % t)
    f1 = src.fields()
    dst.execute("SELECT * FROM %s LIMIT 0" % t)
    f2 = dst.fields()
    # they should be the same set
    if set([x.lower() for x in f1]) != set([x.lower() for x in f2]):
        raise RuntimeError("""\
        Schema definitions are different between src and dst:
        Table: %s
        src: %s
        dst: %s""" % (t, f1, f2))

# update the primary key sequence
def fix_pk(table):
    # get the name of the primary key
    dst.execute("""
    select
        t.relname as table_name,
        ind.relname as pk_name,
        col.attname as column_name
    from pg_class t
    join pg_index i on t.oid = i.indrelid and i.indisprimary = true
    join pg_class ind on i.indexrelid = ind.oid
    join pg_attribute col on col.attrelid = t.oid and col.attnum = i.indkey[0]
    where
        t.relname = ?
    and i.indnatts = 1
    and pg_catalog.pg_table_is_visible(t.oid)
    """, table.lower())
    ret = dst.fetchall()
    if not len(ret):
        return
    pkname = ret[0][2]
    # get the max seq value
    dst.execute("select max(%s) from %s" % (pkname, table))
    pkval = dst.fetchall()[0][0]
    if not pkval:
        pkval = 1
    # now reset the sequence for the primary key
    dst.execute("select pg_catalog.setval(pg_catalog.pg_get_serial_sequence(?, ?), ?, false)",
                table.lower(), pkname.lower(), pkval)
    ret = dst.fetchall()[0][0]
    assert (ret == pkval)
    print "    SETVAL %s(%s) = %d" % (table, pkname, ret)

for t in tList:
    count = src.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
    # prepare the execution cursor
    src.execute("SELECT * FROM %s LIMIT 1" % (t,))
    fields = src.fields()
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (
        t, ", ".join(fields), ",".join(["?"]*len(fields)))
    stmt = dst.compile(sql)
    funcs = [ getfunc(x) for x in fields ]
    i = 0
    src.execute("SELECT * FROM %s" % t)
    t1 = time.time()
    while True:
        row = src.fetchone()
        if row is None:
            break
        i += 1
        rowval = map(lambda (f, x): f(x), zip(funcs, row))
        try:
            dst.execstmt(stmt, *tuple(rowval))
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
            epdb.st()
            raise
        else:
            if i % 1000 == 0:
                t2 = time.time()
                sys.stdout.write("\r%s: %s" % (t, timings(i, count, t1)))
                sys.stdout.flush()
            if i % 10000 == 0:
                pgsql.commit()
    print "\r%s: %s %s" % (t, timings(count, count, t1), " "*10)
    fix_pk(t)
    pgsql.commit()

# and now create the indexes
dst = pgsql.cursor()
for stmt in getIndexes("postgresql"):
    print stmt
    dst.execute(stmt)
pgsql.setVersion(VERSION)
pgsql.commit()
