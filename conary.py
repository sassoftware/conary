#l!/usr/bin/python2.3
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
import conarycfg
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
    cfg = conarycfg.ConaryConfiguration()
except conarycfg.ParseError, e:
    log.error(str(e))
    sys.exit(1)

def usage(rc = 1):
    print "usage: conary branch <newbranch> <branchfrom> [<trove>]"
    print "       conary changeset <pkg> [<oldver>] <newver> <outfile>"
    print "       conary cook    [--prep] [--macros file] <file.recipe>+"
    print "       conary commit       <changeset>"
    print "       conary erase        <pkgname> [<version>]"
    print "       conary localcs      <pkg> <outfile>"
    print "       conary localcommit  <changeset>"
    print "       conary pkglist      <pkgname> [<version>]"
    print "       conary remove       <path>"
    print "       conary replist      <pkgname> [<version>]"
    print "       conary rblist"
    print "       conary rollback     <rollback>"
    print "       conary source       [usage]"
    print "       conary update	   <pkgname> <version>"
    print "           update       <changeset>"
    print "       conary usage"
    print ""
    print "commit flags:  --target-branch <branch>"
    print ""
    print 'common flags:  --config "<item> <value>"'
    print "               --reppath <repository-path>"
    print "               --root <root>"
    print ""
    print "cook flags:    --macros"
    print "               --prep"
    print "               --target-branch <branch>"
    print ""
    print "pkglist flags: --full-versions"
    print "               --ids"
    print "               --ls"
    print "               --sha1s"
    print ""
    print "replist flags: --all"
    print "               --full-versions"
    print "               --ids"
    print "               --info"
    print "               --leaves"
    print "               --ls"
    print "               --sha1s"
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

    cfgMap["reppath"] = "repPath"
    cfgMap["root"] = "root"

    argDef["all"] = 0
    argDef["config"] = 2
    argDef["debug"] = 0
    argDef["full-versions"] = 0
    argDef["ids"] = 0
    argDef["info"] = 0
    argDef["leaves"] = 0
    argDef["ls"] = 0
    argDef["macros"] = 1
    argDef["message"] = 1
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
	prof = hotshot.Profile('conary.prof')
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
	repos = openRepository(cfg.repPath)

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

	repos = openRepository(cfg.repPath)

	cscmd.ChangeSetCommand(repos, cfg, name, outFile, old, new)
    elif (otherArgs[1] == "commit"):
	targetBranch = None
	if argSet.has_key('target-branch'):
	    targetBranch  = argSet['target-branch']
	    del argSet['target-branch']
	if len(otherArgs) < 3: return usage()
	repos = openRepository(cfg.repPath)
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
	buildBranch = None
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

	if argSet.has_key('target-branch'):
	    buildBranch = argSet['target-branch']
	    del argSet['target-branch']

	if argSet: return usage()

	cook.cookCommand(cfg, otherArgs[2:], prep, macros)                
    elif (otherArgs[1] == "erase"):
	if argSet: return usage
	if len(otherArgs) >= 3 and len(otherArgs) <=4:
	    db = openDatabase(cfg.root, cfg.dbPath)

	    args = [db, cfg] + otherArgs[2:]
	    updatecmd.doErase(*args)
	else:
	    return usage()
    elif (otherArgs[1] == "import"):
	if len(otherArgs) != 3 and len(otherArgs) != 3:
	    return usage()

	repos = openRepository(cfg.repPath)
	importrpm.doImport(repos, cfg, otherArgs[2])
    elif (otherArgs[1] == "localcs"):
	if len(otherArgs) != 4 and len(otherArgs) != 4:
	    return usage()

	name = otherArgs[2]
	outFile = otherArgs[3]

	db = database.Database(cfg.root, cfg.dbPath, "r")
	cscmd.LocalChangeSetCommand(db, cfg, name, outFile)
    elif (otherArgs[1] == "localcommit"):
	if len(otherArgs) < 3: return usage()
	db = database.Database(cfg.root, cfg.dbPath, "c")
	for changeSet in otherArgs[2:]:
	    commit.doLocalCommit(db, changeSet)
    elif (otherArgs[1] == "pkglist"):
	ls = argSet.has_key('ls')
	if ls: del argSet['ls']

	ids = argSet.has_key('ids')
	if ids: del argSet['ids']

	sha1s = argSet.has_key('sha1s')
	if sha1s: del argSet['sha1s']

	fullVersions = argSet.has_key('full-versions')
	if fullVersions: del argSet['full-versions']

	db = openDatabase(cfg.root, cfg.dbPath)

	if argSet: return usage()

	if len(otherArgs) >= 2 and len(otherArgs) <= 4:
	    args = [db, cfg, ls, ids, sha1s, fullVersions] + otherArgs[2:]
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

	fullVersions = argSet.has_key('full-versions')
	if fullVersions: del argSet['full-versions']

	ids = argSet.has_key('ids')
	if ids: del argSet['ids']

	info = argSet.has_key('info')
	if info: del argSet['info']

	sha1s = argSet.has_key('sha1s')
	if sha1s: del argSet['sha1s']

	leaves = argSet.has_key('leaves')
	if leaves: del argSet['leaves']

	repos = openRepository(cfg.repPath)

	if argSet: return usage()

	if len(otherArgs) >= 2 and len(otherArgs) <= 4:
	    args = [repos, cfg, all, ls, ids, sha1s, leaves, fullVersions,
		    info] + otherArgs[2:]
	    try:
		queryrep.displayTroves(*args)
	    except IOError, msg:
		sys.stderr.write(msg.strerror + '\n')
		sys.exit(1)
	else:
	    return usage()
    elif (otherArgs[1] == "rblist"):
	if argSet: return usage
	db = openDatabase(cfg.root, cfg.dbPath)
	rollbacks.listRollbacks(db, cfg)
    elif (otherArgs[1] == "remove"):
	if len(otherArgs) != 3: return usage()
	if argSet: return usage
	db = openDatabase(cfg.root, cfg.dbPath)
	fullPath = util.joinPaths(cfg.root, otherArgs[2])
	if os.path.exists(fullPath):
	    os.unlink(fullPath)
	else:
	    log.warning("%s has already been removed", fullPath)
	db.removeFile(otherArgs[2])
    elif (otherArgs[1] == "rollback"):
	if argSet: return usage
	db = openDatabase(cfg.root, cfg.dbPath)
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
	    repos = openRepository(cfg.repPath)
	    db = openDatabase(cfg.root, cfg.dbPath)

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
