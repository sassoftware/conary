#!/usr/bin/env python
#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
import sys

if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/conary/scripts")

import StringIO
import optparse

from conary import versions
from conary import conarycfg
from conary.lib import options

from conary import dbstore
from conary.dbstore import idtable
from conary.deps import deps

DB_VERSION = 1

class OptionError(Exception):
    def __init__(self, errcode, errmsg, *args):
        self.errcode = errcode
        self.errmsg = errmsg
        Exception.__init__(self, *args)
    def __str__(self):
        return "OptionError[%d]: %s %s" % (self.errcode, self.errmsg, self.args)
    
def fail(code, srcMap, pkgMap, grpMap, argv):
    print >>sys.stderr, "An error occurred while processing logaction.  Code: %d" % code
    print >>sys.stderr, "    srcMap=%s" % srcMap.items()
    print >>sys.stderr, "    pkgMap=%s" % pkgMap.items()
    print >>sys.stderr, "    grpMap=%s" % grpMap.items()
    print >>sys.stderr, "    argv=%s" % argv
    sys.stderr.flush()

def process(repos, cfg, commitList, srcMap, pkgMap, grpMap, argv, otherArgs):
    if not len(argv) and not len(otherArgs):
        return usage()

    argDef = {
        'user': options.ONE_PARAM,
        'file': options.ONE_PARAM,
    }

    # create an argv[0] for processArgs to ignore
    argv[0:0] = ['']
    argSet, someArgs = options.processArgs(argDef, {}, cfg, usage, argv=argv)
    # and now remove argv[0] again
    argv.pop(0)
    if len(someArgs):
        someArgs.pop(0)
    otherArgs.extend(someArgs)

    user = None
    dbfile = None
    if 'user' in argSet:
        user = argSet['user']
    if 'file' in argSet:
        dbfile = argSet['file']

    pid = os.fork()
    if not pid:
        #child 1
        logFd = os.open("/dev/null", os.O_RDWR | os.O_CREAT)
        os.dup2(logFd, 1)
        os.dup2(logFd, 2)
        os.close(0)
        os.close(logFd)
        pid2 = os.fork()
        if not pid2:
            #child 2
            doCommit(repos, cfg, commitList, user, dbfile)
            sys.exit(0)
        else:
            #parent 2
            pid2, status = os.waitpid(pid2, 0)
            if status:
                fail(status, srcMap, pkgMap, grpMap, argv)
            sys.exit(0)
    return 0

# create the schema for logging the commit information
def createSchema(db):
    db.loadSchema()
    cu = db.cursor()
    commit = False
    if "Commits" not in db.tables:
        cu.execute("""
        create table Commits(
            commitId    %(PRIMARYKEY)s,
            username    %(STRING)s,
            cfgtext     %(STRING)s,
            changed     %(CHANGED)s
        ) %(TABLEOPTS)s """ % db.keywords)
        db.tables["Commits"] = []
        commit = True
    db.createTrigger("Commits", "changed", "INSERT")
    db.createTrigger("Commits", "changed", "UPDATE")

    if idtable.createIdTable(db, "Items", "itemId", "item"):
        commit = True
    if idtable.createIdTable(db, "Versions", "versionId", "version"):
        commit = True
    if idtable.createIdTable(db, "Flavors", "flavorId", "flavor"):
        commit = True

    if "CommitList" not in db.tables:
        cu.execute("""
        create table CommitList(
            id          %(PRIMARYKEY)s,
            commitId    INTEGER NOT NULL,
            itemId      INTEGER NOT NULL,
            versionid   INTEGER NOT NULL,
            flavorId    INTEGER,
            changed     %(CHANGED)s,
            constraint CommitList_commitId_fk
                foreign key(commitId) references Commits(commitId)
                on delete cascade on update cascade,
            constraint CommitList_itemId_fk
                foreign key(itemId) references Items(itemId)
                on delete cascade on update cascade,
            constraint CommitList_versionId_fk
                foreign key(versionId) references Versions(versionId)
                on delete cascade on update cascade,
            constraint CommitList_flavorId_fk
                foreign key(flavorId) references Flavors(flavorId)
                on delete cascade on update cascade
        ) %(TABLEOPTS)s """ % db.keywords)
        db.tables["CommitList"] = []
        commit = True
    db.createTrigger("CommitList", "changed", "INSERT")
    db.createTrigger("CommitList", "changed", "UPDATE")
    db.createIndex("CommitList", "CommitList_commitId_idx", "commitId")
    db.createIndex("CommitList", "CommitList_itemId_idx", "itemId")
    db.createIndex("CommitList", "CommitList_version_idx", "versionId")
    if commit:
        db.commit()
    db.setVersion(DB_VERSION)
    return commit
    
def getDB(dbfile, create = False):
    db = dbstore.connect(dbfile, driver="sqlite")
    if create:
        createSchema(db)
    v = db.getVersion()
    assert (v == DB_VERSION)
    return db

def doCommit(repos, cfg, commitList, user, dbfile):
    db = getDB(dbfile, create = True)
    if commitList:
        s = StringIO.StringIO()
        cfg.store(s, False)
        cfgStr = s.getvalue()
        cu = db.transaction()
        cu.execute("insert into Commits(username, cfgtext) values (?, ?)", user, cfgStr)
        commitId = cu.lastrowid
        Items = idtable.IdTable(db, "Items", "itemId", "item")
        Versions = idtable.IdTable(db, "Versions", "versionId", "version")
        Flavors = idtable.IdTable(db, "Flavors", "flavorId", "flavor")
        def _commitIdList(commitList):
            for n,vStr,fStr in commitList:
                v = versions.VersionFromString(vStr)
                f = deps.parseFlavor(fStr)
                yield (commitId, Items.getOrAddId(n),
                       Versions.getOrAddId(v.asString()),
                       Flavors.getOrAddId(f.freeze()))
        cu.executemany("insert into CommitList(commitId, itemId, versionId, flavorId) values (?,?,?,?)",
                       _commitIdList(commitList))
        db.commit()
    db.close()
    return 0

def usage():
    usage = "\n".join([
        "commitaction [commitaction args] --module '/path/to/logaction --user <user> --file <dbfile>'",
        "or"
        "%prog --dbfile=DBFILE [--stdin --user=USER] [--list] [--show=ID]"
        ])
    print usage

def parseArgs(argv):
    usage = "\n".join([
        "commitaction [commitaction args] --module '/path/to/logaction --user <user> --file <dbfile>'",
        "or"
        "%prog --dbfile=DBFILE [--stdin --user=USER] [--list] [--show=ID]"
        ])
    parser = optparse.OptionParser(version = '%prog 0.1', usage = usage)
    parser.add_option("--user", dest = "user", metavar = "USER",
                      help = "username to record for commit")
    parser.add_option("--dbfile", dest = "dbfile", metavar = "DBFILE",
                      help = "path to database file")
    parser.add_option("--stdin", dest = "stdin",
                      action = "store_true", default = False,
                      help = "add new entries from stdin trove lines (as produced by commitaction)")
    parser.add_option("--list", dest = "list", action = "store_true",
                      help = "list all commits in the database")
    parser.add_option("--show", dest = "show", metavar = "ID",
                      help = "show the contents of commit id")
    (options, args) = parser.parse_args(argv)

    if options.dbfile is None:
        raise OptionError(1, 'a database file path is required')
    elif args:
        raise OptionError(1, 'unexpected arguments: %s' % " ".join(args))
    # check options validity
    if options.stdin and not options.user:
        raise OptionError(1, 'committing a new entry requires a user value')
    modes = []
    if options.stdin:
        modes.append("stdin")
    if options.list:
        modes.append("list")
    if options.show:
        modes.append("show")
    if len(modes) != 1:
        raise OptionError(1, "one major mode must be required", modes)
    return options

def doList(dbfile):
    db = getDB(dbfile)
    cu = db.cursor()
    cu.execute("select commitId, username, changed from Commits")
    for commitId, username, changed in cu:
        print "ID:%d\tUSERNAME:%-20s\tTIME:%14d" % (commitId, username, int(changed))
    db.close()

def doShow(dbfile, commitId):
    db = getDB(dbfile)
    cu = db.cursor()
    cu.execute("""
    select item, version, flavor from CommitList
    join Items on CommitList.itemId = Items.itemId
    join Versions on CommitList.versionId = Versions.versionId
    join Flavors on CommitList.flavorId = Flavors.flavorId
    where commitId = ? """, commitId)
    for n, vStr, fStr in cu:
        if fStr:
            f = deps.ThawFlavor(fStr)
            print "%s=%s[%s]" % (n, vStr, deps.formatFlavor(f))
        else:
            print "%s=%s" % (n, vStr)
    db.close()
    
def main(argv = None):
    if argv is None:
        argv = sys.argv[1:]
    options = parseArgs(argv)
    if options.stdin:
        data = [x[:-1] for x in sys.stdin.readlines()]
        # [1,2,3,4,5,6,...] -> [(1,2,3), (4,5,6), ...]
        commitList = zip(data, data[1:], data[2:])[::3]
        cfg = conarycfg.ConaryConfiguration()
        doCommit(None, cfg, commitList, user = options.user, dbfile = options.dbfile)
    elif options.list:
        doList(options.dbfile)
    elif options.show:
        doShow(options.dbfile, options.show)
    return

if __name__ == "__main__":
    main()
