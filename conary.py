#!/usr/bin/python2.3
# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import branch
import commit
import cook
import cscmd
from local import database
import display
import helper
import importrpm
import log
import os
import queryrep
import repository
import rollbacks
import srscfg
import srcctl
import sys
import updatecmd
import util
import xmlrpclib

if sys.version_info < (2, 3):
    print "error: python 2.3 or greater is requried"
    sys.exit(1)

sys.excepthook = util.excepthook

try:
    cfg = srscfg.SrsConfiguration()
except srscfg.ParseError, e:
    log.error(str(e))
    sys.exit(1)

def usage(rc = 1):
    print "usage: srs branch <newbranch> <branchfrom> [<trove>]"
    print "       srs changeset <pkg> [<oldver>] <newver> <outfile>"
    print "       srs cook    [--prep] [--macros file] <first.recipe> <second.recipe> ..."
    print "       srs commit       <changeset>"
    print "       srs erase        <pkgname> [<version>]"
    print "       srs localcs      <pkg> <outfile>"
    print "       srs localcommit  <changeset>"
    print "       srs pkglist      <pkgname> [<version>]"
    print "       srs remove       <path>"
    print "       srs replist      <pkgname> [<version>]"
    print "       srs rblist"
    print "       srs rollback     <rollback>"
    print "       srs source       [usage]"
    print "       srs update	   <pkgname> <version>"
    print "           update       <changeset>"
    print "       srs usage"
    print ""
    print "commit flags:  --target-branch <branch>"
    print ""
    print "common flags:  --config "<item> <value>"'
    print "               --reppath <repository-path>"
    print "               --root <root>"
    print ""
    print "pkglist flags: --sha1s"
    print "               --ids"
    print "               --ls"
    print ""
    print "replist flags: --all"
    print "               --sha1s"
    print "               --ids"
    print "               --ls"
    print ""
    print "update flags: --replace-files"
    return rc

def openRepository(path):
    try:
        return helper.openRepository(path)
    except repository.repository.OpenError, e:
	log.error('Unable to open repository %s: %s', path, str(e))
	sys.exit(1)

def openDatabase(root, path):
    try:
        db = database.Database(root, path)
    except repository.repository.OpenError, e:
        log.error('Unable to open database %s%s%s: %s', root, os.sep, path, str(e))
        sys.exit(1)
    return db

def realMain():
    otherArgs = [ sys.argv[0] ]
    argSet = {}
    argDef = {}
    cfgMap = {}
    # 0 - arg may occur, no parameter
    # 1 - arg may occur once, w/ parameter
    # 2 - arg may occur N times, w/ parameter

    cfgMap["reppath"] = "reppath"
    cfgMap["root"] = "root"

    argDef["all"] = 0
    argDef["config"] = 2
    argDef["debug"] = 0
    argDef["ids"] = 0
    argDef["ls"] = 0
    argDef["macros"] = 1
    argDef["prep"] = 0
    argDef["profile"] = 0
    argDef["replace-files"] = 0
    argDef["sha1s"] = 0
    argDef["target-branch"] = 1

    argDef.update(srcctl.argDef)

    for arg in cfgMap.keys():
	argDef[arg] = 1

    i = 1
    while i < len(sys.argv):
	if sys.argv[i][:2] != "--":
	    otherArgs.append(sys.argv[i])
	else:
	    arg = sys.argv[i][2:]
	    if not argDef.has_key(arg): return usage()

	    if not argDef[arg]:
		argSet[arg] = 1
	    else:
		# the argument takes a parameter
		i = i + 1
		if i >= len(sys.argv): return usage()

		if argDef[arg] == 1:
		    # exactly one parameter is allowd
		    if argSet.has_key(arg): return usage()
		    argSet[arg] = sys.argv[i]
		else:
		    # multiple parameters may occur
		    if argSet.has_key(arg):
			argSet[arg].append(sys.argv[i])
		    else:
			argSet[arg] = [sys.argv[i]]

	i = i + 1

    if '-v' in otherArgs:
	otherArgs.remove('-v')
	log.setVerbosity(1)
    else:
	log.setVerbosity(0)

    if argSet.has_key('debug'):
	del argSet['debug']
	import pdb
	pdb.set_trace()

    profile = False
    if argSet.has_key('profile'):
	import hotshot
	prof = hotshot.Profile('srs.prof')
	prof.start()
	profile = True
	del argSet['profile']

    if argSet.has_key('config'):
	for param in argSet['config']:
	    cfg.configLine(param)

	del argSet['config']

    for (arg, name) in cfgMap.items():
	if argSet.has_key(arg):
	    cfg.configLine("%s %s" % (name, argSet[arg]))
	    del argSet[arg]

    if (len(otherArgs) < 2):
	return usage()
    elif (otherArgs[1] == "branch"):
	if argSet: return usage
	if len(otherArgs) < 4 or len(otherArgs) > 5: return usage()
	repos = openRepository(cfg.reppath)

	args = [repos, ] + otherArgs[2:]
	branch.branch(*args)
    elif (otherArgs[1] == "changeset"):
	# current usage is "package file oldversion newversion"
	if len(otherArgs) != 5 and len(otherArgs) != 6:
	    return usage()

	name = otherArgs[2]
	if len(otherArgs) == 6:
	    (old, new) = (otherArgs[3], otherArgs[4])
	    outFile = otherArgs[5]
	else:
	    (old, new) = (None, otherArgs[3])
	    outFile = otherArgs[4]

	repos = openRepository(cfg.reppath)

	cscmd.ChangeSetCommand(repos, cfg, name, outFile, old, new)
    elif (otherArgs[1] == "commit"):
	targetBranch = None
	if argSet.has_key('target-branch'):
	    targetBranch  = argSet['target-branch']
	    del argSet['target-branch']
	if len(otherArgs) < 3: return usage()
	repos = openRepository(cfg.reppath)
	for changeSet in otherArgs[2:]:
	    commit.doCommit(repos, changeSet, targetBranch)
    elif (otherArgs[1] == "config"):
	if argSet: return usage
	if (len(otherArgs) > 2):
	    return usage()
	else:
	    cfg.display()
    elif (otherArgs[1] == "cook"):
	log.setVerbosity(1)
	macros = {}
	prep = 0
	if argSet.has_key('prep'):
	    del argSet['prep']
	    prep = 1
	if argSet.has_key('macros'):
	    argSet['macros']
	    f = open(argSet['macros'])
	    # XXX sick hack
	    macroSrc = "macros =" + f.read()
	    exec macroSrc
	    del f
	    del argSet['macros']
	if argSet: return usage()

	cook.cookCommand(cfg, otherArgs[2:], prep, macros)                
    elif (otherArgs[1] == "erase"):
	if argSet: return usage
	if len(otherArgs) >= 3 and len(otherArgs) <=4:
	    db = openDatabase(cfg.root, cfg.dbpath)

	    args = [db, cfg] + otherArgs[2:]
	    updatecmd.doErase(*args)
	else:
	    return usage()
    elif (otherArgs[1] == "import"):
	if len(otherArgs) != 3 and len(otherArgs) != 3:
	    return usage()

	repos = openRepository(cfg.reppath)
	importrpm.doImport(repos, cfg, otherArgs[2])
    elif (otherArgs[1] == "localcs"):
	if len(otherArgs) != 4 and len(otherArgs) != 4:
	    return usage()

	name = otherArgs[2]
	outFile = otherArgs[3]

	db = database.Database(cfg.root, cfg.dbpath, "r")
	cscmd.LocalChangeSetCommand(db, cfg, name, outFile)
    elif (otherArgs[1] == "localcommit"):
	if len(otherArgs) < 3: return usage()
	db = database.Database(cfg.root, cfg.dbpath, "c")
	for changeSet in otherArgs[2:]:
	    commit.doLocalCommit(db, changeSet)
    elif (otherArgs[1] == "pkglist"):
	ls = argSet.has_key('ls')
	if ls: del argSet['ls']

	ids = argSet.has_key('ids')
	if ids: del argSet['ids']

	sha1s = argSet.has_key('sha1s')
	if sha1s: del argSet['sha1s']

	db = openDatabase(cfg.root, cfg.dbpath)

	if argSet: return usage()

	if len(otherArgs) >= 2 and len(otherArgs) <= 4:
	    args = [db, cfg, ls, ids, sha1s] + otherArgs[2:]
	    try:
		display.displayTroves(*args)
	    except IOError, msg:
		sys.stderr.write(msg.strerror + '\n')
		sys.exit(1)
	else:
	    return usage()
    elif (otherArgs[1] == "replist"):
	all = argSet.has_key('all')
	if all: del argSet['all']

	ls = argSet.has_key('ls')
	if ls: del argSet['ls']

	ids = argSet.has_key('ids')
	if ids: del argSet['ids']

	sha1s = argSet.has_key('sha1s')
	if sha1s: del argSet['sha1s']

	repos = openRepository(cfg.reppath)

	if argSet: return usage()

	if len(otherArgs) >= 2 and len(otherArgs) <= 4:
	    args = [repos, cfg, all, ls, ids, sha1s] + otherArgs[2:]
	    try:
		queryrep.displayTroves(*args)
	    except IOError, msg:
		sys.stderr.write(msg.strerror + '\n')
		sys.exit(1)
	else:
	    return usage()
    elif (otherArgs[1] == "rblist"):
	if argSet: return usage
	db = openDatabase(cfg.root, cfg.dbpath)
	rollbacks.listRollbacks(db, cfg)
    elif (otherArgs[1] == "remove"):
	if len(otherArgs) != 3: return usage()
	if argSet: return usage
	db = openDatabase(cfg.root, cfg.dbpath)
	fullPath = util.joinPaths(cfg.root, otherArgs[2])
	if os.path.exists(fullPath):
	    os.unlink(fullPath)
	else:
	    log.warning("%s has already been removed", fullPath)
	db.removeFile(otherArgs[2])
    elif (otherArgs[1] == "rollback"):
	if argSet: return usage
	db = openDatabase(cfg.root, cfg.dbpath)
	args = [db, cfg] + otherArgs[2:]
	rollbacks.apply(*args)
    elif (otherArgs[1] == "source" or otherArgs[1] == "src"):
	return srcctl.sourceCommand(cfg, otherArgs[2:], argSet)
    elif (otherArgs[1] == "update"):
	kwargs = {}
	replaceFiles = argSet.has_key('replace-files')
	if replaceFiles:
	    kwargs['replaceFiles'] = True
	    del argSet['replace-files']
	if argSet: return usage
	if len(otherArgs) >=3 and len(otherArgs) <= 4:
	    repos = openRepository(cfg.reppath)
	    db = openDatabase(cfg.root, cfg.dbpath)

	    args = [repos, db, cfg] + otherArgs[2:]
	    updatecmd.doUpdate(*args, **kwargs)
	else:
	    return usage()
    elif (otherArgs[1] == "return usage"):
	return usage(rc = 0)
    else:
	return usage()

    if profile:
	prof.stop()

    if log.errorOccurred():
	sys.exit(1)

def main():
    try:
	realMain()
    except xmlrpclib.ProtocolError, e:
	if e.errcode == 403:
	    print >> sys.stderr, \
		"remote server denied permission for the requested operation"
	else:
	    raise

if __name__ == "__main__":
    sys.exit(main())
