#!/usr/bin/env python2.4
#
# Copyright (c) 2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
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
import itertools

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
    'FileStreams',
    'Nodes',
    'ChangeLogs',
    'Instances',
    'TroveInfo',
    'Dependencies',
    'Latest',
    'Metadata',
    'MetadataItems',
    'PGPKeys',
    'PGPFingerprints',
    'Provides',
    'Requires',
    'TroveRedirects',
    'TroveTroves',
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

# update the primary key sequences for all tables
def fix_primary_keys(cu):
    # get the name of the primary key
    cu.execute("""
    select
        t.relname as table_name,
        col.attname as column_name
    from pg_class t
    join pg_index i on t.oid = i.indrelid and i.indisprimary = true
    join pg_class ind on i.indexrelid = ind.oid
    join pg_attribute col on col.attrelid = t.oid and col.attnum = i.indkey[0]
    where i.indnatts = 1
      and pg_catalog.pg_table_is_visible(t.oid)
    """)
    for (t, col) in cu.fetchall():
        table = t.lower()
        cu.execute("select pg_catalog.pg_get_serial_sequence(?, ?)", (table, col))
        seqname = cu.fetchone()[0]
        if seqname is None:
            # this primary key does not have a sequence associated with it
            continue
        # get the max seq value
        cu.execute("select max(%s) from %s" % (col, table))
        seqval = cu.fetchone()[0]
        if not seqval:
            seqval = 1
        else:
            seqval += 1 # we need the next one in line
        # now reset the sequence for the primary key
        cu.execute("select pg_catalog.setval(?, ?, false)", (seqname, seqval))
        ret = cu.fetchone()[0]
        assert (ret == seqval)
        print "SETVAL %s = %d (%s.%s)" % (seqname, ret, table, col)

def migrate_table(t):
    global pgsql, src, dst

    count = src.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
    # prepare the execution cursor
    src.execute("SELECT * FROM %s LIMIT 1" % (t,))
    fields = src.fields()
    funcs = [ getfunc(x) for x in fields ]

    # fix up the values in a row to match the transforms
    def rowvals(funcs, row):
        return tuple(map(lambda (f, x): f(x), zip(funcs, row)))

    def copy1by1(t, src):
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            t, ", ".join(fields), ",".join(["?"]*len(fields)))
        stmt = dst.compile(sql)
        i = 0
        while True:
            lastrow = row = src.fetchone()
            if row is None:
                break
            i += 1
            dst.execstmt(stmt, *rowvals(funcs, row))
            if i % 1000 == 0:
                t2 = time.time()
                sys.stdout.write("\r%s: %s" % (t, timings(i, count, t1)))
                sys.stdout.flush()
            if i % 10000 == 0:
                pgsql.commit()
        pgsql.commit()
        return i

    def copyBulk(t, src, batch=10000):
        i = 0
        while i <= count:
            pgsql.dbh.bulkload(t, itertools.islice(src, batch), fields)
            i = i + batch
            pgsql.commit()
            sys.stdout.write("\r%s: %s" % (t, timings(i, count, t1)))
            sys.stdout.flush()
        dst.execute("select count(*) from %s" %(t,))
        ret = dst.fetchall()[0][0]
        sys.stdout.write("\r%s: %s" % (t, timings(ret, count, t1)))
        sys.stdout.flush()
        return ret

    src.execute("SELECT * FROM %s" % t)
    t1 = time.time()
    try:
        if hasattr(pgsql.dbh, "bulkload"):
            sys.stdout.write("Bulk load table: %s (%d records...)\r" % (t, count))
            sys.stdout.flush()
            ret = copyBulk(t, src)
        else:
            print "WARNING: not using bulk load, update your python-pgsql bindings!"
            ret = copy1by1(t, src)
        assert (ret == count), "Inserted %d rows != source count %d rows" % (ret, count)
    except Exception, e:
        print "ERROR:", e, e.args
        epdb.st()
        raise
    else:
        print "\r%s: %s %s" % (t, timings(count, count, t1), " "*10)
    pgsql.commit()

def verify_table(t, quick=False):
    nrsrc = src.execute("select count(*) from %s" % (t,)).fetchall()[0][0]
    nrdst = src.execute("select count(*) from %s" % (t,)).fetchall()[0][0]
    assert(nrsrc == nrdst), "not all records were copied: src=%d, dst=%d" % (nrsrc, nrdst)
    src.execute("select * from %s limit 1" % (t,))
    dst.execute("select * from %s limit 1" % (t,))
    setsrc = set([x.lower() for x in src.fields()])
    setdst = set([x.lower() for x in dst.fields()])
    assert ( setsrc == setdst ), "columns are different: src=%d, dst=%d" % (setsrc, setdst)
    if quick:
        return True
    fields = ",".join(setsrc)
    src.execute("select %s from %s" %(fields, t))
    dst.execute("select %s from %s" %(fields, t))
    t1 = time.time()
    i = 0
    for row1, row2 in itertools.izip(src, dst):
        i = i+1
        for a,b in zip(row1, row2):
            assert (a==b), "\nrow differences in table %s:\nsrc: %s\ndst: %s\n" %(
                t, row1, row2)
        if i%100 and i<nrsrc: continue
        sys.stdout.write("\rVerify %s: %s" % (t, timings(i, nrsrc, t1)))
        sys.stdout.flush()
    print
    return True

# PROGRAM MAIN LOOP
for t in tList:
    migrate_table(t)
    #verify_table(t)

# and now create the indexes
dst = pgsql.cursor()
for stmt in getIndexes("postgresql"):
    print stmt
    dst.execute(stmt)
# fix the primary keys
fix_primary_keys(dst)
print "VACUUM ANALYZE"
pgsql.dbh.execute("VACUUM ANALYZE")
pgsql.setVersion(VERSION)
pgsql.commit()

del src
srcdb.close()

del dst
pgsql.close()

print "Done"

