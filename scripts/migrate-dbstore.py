#!/usr/bin/python

import sys
import os
if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])

from conary import dbstore
from conary.dbstore import sqlerrors
from conary.repository.netrepos import schema

if len(sys.argv) != 3:
    print "Usage: migrate <sqlite_path> <mysql_spec>"

sqlite = dbstore.connect(sys.argv[1], driver = "sqlite")
cs = sqlite.cursor()
mysql = dbstore.connect(sys.argv[2], driver = "mysql")
cm = mysql.cursor()

schema.createSchema(mysql)

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
    'Versions',
    'Items',
    'Labels',
    'LabelMap',
    'Flavors',
    'FlavorMap',
    'FlavorScores',
    'UserGroups',
    'Users',
    'UserGroupMembers',
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
    'FileStreams',
    'TroveFiles',
    'TroveInfo',
    'TroveTroves',
    'EntitlementGroups',
    'Entitlements',
    ]

for t in tList:
    print
    print "Converting", t
    count = cs.execute("select count(*) from %s" % t).fetchone()[0]
    i = 0
    cs.execute("select * from %s" % t)
    cm.execute('alter table %s disable keys' % t)
    while True:
        row = cs.fetchone_dict()
        if row is None:
            break
        if t == "Permissions":
            row["canWrite"] = row["write"]
            del row["write"]
            if 'entGroupEdmin' in row:
                del row["entGroupAdmin"]
        row = row.items()
        sql = "insert into %s (%s) values (%s)" % (
            t, ", ".join(x[0] for x in row),
            ", ".join(["?"] * len(row)))
        i += 1
        try:
            cm.execute(sql, [x[1] for x in row])
        except sqlerrors.ColumnNotUnique:
            print "\r%s: SKIPPING" % t, row
        except:
            print "ERROR - SQL", sql, "ARGS:", [x[1] for x in row]
            raise
        else:
            if i % 1000 == 0:
                sys.stdout.write("\r%s: %d/%d %d%%" % (t, i, count, i*100/count))
                sys.stdout.flush()
            if i % 50000 == 0:
                mysql.commit()
    cm.execute('alter table %s enable keys' % t)
    print "\r%s: %d/%d 100%%" % (t, i, count)
    mysql.commit()

