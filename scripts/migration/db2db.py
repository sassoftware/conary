#!/usr/bin/env python2.4
#
# Copyright (c) 2007 rPath, Inc.
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

# Cristian Gafton, <gafton@rpath.com>
# Migrates a conary repository DB stored in a abacked supported by
# dbstore to another db stored in a backend supported by dbstore

import sys
import os
if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/conary/scripts")
    
import time
import itertools
import optparse

from conary.server.schema import VERSION

from tablelist import TableList
from database import getdb

class Callback:
    def __init__(self, table, count, tag = ""):
        self.table = table
        self.count = count
        self.tag = tag
        self.start = time.time()
        self.counter = 0
    def display(self, counter, pre = "", post = ""):
        sys.stdout.write("\r%s %s: %s %s" % (
            pre, self.table, self.timings(counter), post))
        sys.stdout.flush()
    def increment(self, counter = 1):
        self.counter += counter
        if self.counter % 1000 == 0:
            self.display(self.counter, pre = self.tag)
    def last(self):
        self.display(self.count, post = " " * (len(self.tag)+1))
    def timings(self, current):
        tnow = time.time()
        tpassed = max(tnow-self.start,1)
        speed = max(current/tpassed,1)
        tremaining = (self.count-current)/speed
        return "%d/%d %02d%% (%d rec/sec, %d:%02d passed, %d:%02d remaining)" % (
            current, self.count, (current*100)/max(self.count,1),
            speed,
            tpassed/60, tpassed % 60,
            tremaining/60, tremaining % 60)

def migrate_table(src, dst, t, batch=5000):
    count = src.getCount(t)
    fields = src.getFields(t)
    dstCu = dst.prepareInsert(t, fields)
    callback = Callback(t, count, "Copying")
    rowCounter = 0
    commitCounter = 0
    srcCu = src.iterRows(t)
    while rowCounter <= count:
        rows = srcCu.fetchmany(batch)
        if len(rows) == 0:
            break
        ret = dstCu.insertRows(rows, callback)
        rowCounter += ret
        commitCounter += ret
        if commitCounter > 10000:
            dst.commit()
            commitCounter = 0
    callback.last()
    dst.commit()
    # test out that we did a good insert
    dstCount = dst.getCount(t)
    assert (count == dstCount), "Source Rows count %d != target rows count %d for table %s" % (
        count, dstCount, t)
    return count

def verify_table(src, dst, table, quick=False):
    srcCount = src.getCount(table)
    dstCount = dst.getCount(table)
    assert(srcCount == dstCount), "not all records were copied: src=%d, dst=%d" %(
        srcCount, dstCount)
    srcFields = src.getFields(table)
    dstFields = dst.getFields(table)
    assert ( set(srcFields) == set(dstFields) ), "columns are different: src=%d, dst=%d" % (
        srcFields, dstFields)
    if quick:
        return True
    fields = ",".join(srcFields)
    srcCu = src.iterRows(table, fields)
    dstCu = dst.iterRows(table, fields)
    callback = Callback(table, srcCount, "Verify")
    for row1, row2 in itertools.izip(srcCu, dstCu):
        for a,b in zip(row1, row2):
            assert (a==b), "\nrow differences in table %s:\nsrc: %s\ndst: %s\n" %(
                table, row1, row2)
        callback.increment()
    callback.last()
    return True

#
# MAIN PROGRAM
#
if __name__ == '__main__':
    def store_db(option, opt_str, value, parser):
        if parser.values.db is None:
            parser.values.db = []
        parser.values.db.append((opt_str[2:], value))
        if len(parser.values.db) > 2:
            raise OptionValueError("Can only specify one source and one target database")
    parser = optparse.OptionParser(usage = "usage: %prog [options] srcopt=DB dstopt=DB")
    for db in ["sqlite", "mysql", "postgresql"]:
        parser.add_option("--" + db, action = "callback", callback = store_db, type="string",
                      dest = "db", help = "specify a %s database" %db, metavar = db.upper())
    parser.add_option("--verify", "-V", action = "store_true", dest = "verify",
                      help = "Verify each table after copy")
    (options, args) = parser.parse_args()
    if len(options.db) != 2:
        parser.print_help()
        sys.exit(-1)
    src = getdb(*options.db[0])
    dst = getdb(*options.db[1])

    dst.createSchema()
    # check that the source and target match schemas
    diff = set(src.getTables()).difference(set(dst.getTables()))
    if diff:
        print "WARNING: Only in Source (%s): %s" % (src.driver, diff)
    diff = set(dst.getTables()).difference(set(src.getTables()))
    if diff:
        print "WARNING: Only in Target (%s): %s" % (dst.driver, diff)
    src.checkTablesList()
    dst.checkTablesList(isSrc=False)
    # compare each table's schema between the source and target
    for table in TableList:
        srcFields = src.getFields(table)
        dstFields = dst.getFields(table)
        if set(srcFields) != set(dstFields):
            raise RuntimeError("""\
            Schema definitions are different between databases:
            Table: %s
            %s: %s
            %s: %s""" % (table, src.driver, srcFields, dst.driver, dstFields))

    # now migrate all tables
    for table in TableList:
        migrate_table(src, dst, table)
        if options.verify:
            verify_table(src, dst, table)
        sys.stdout.write("\n")
        sys.stdout.flush()

    # create the indexes to close the loop
    dst.createIndexes()
    dst.finalize(VERSION)

    src.close()
    dst.close()

    print "Done"
