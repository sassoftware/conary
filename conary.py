#!/usr/bin/python2.3
# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
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
if sys.version_info < (2, 3):
    print "error: python 2.3 or greater is requried"
    sys.exit(1)

import options
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
import updatecmd
import util
import versions
import xmlrpclib
from repository import netclient

sys.excepthook = util.excepthook

try:
    cfg = conarycfg.ConaryConfiguration()
except conarycfg.ConaryCfgError, e:
    log.error(str(e))
    sys.exit(1)

def usage(rc = 1):
    print "usage: conary branch <newbranch> <branchfrom> [<trove>]"
    print "       conary changeset <pkg> [<oldver>] <newver> <outfile>"
    print "       conary cook [--prep] [--debug-exceptions] [--macros file] <file.recipe|troveName>+"
    print "       conary emerge       <troveName>+"
    print "       conary commit       <changeset>"
    print "       conary erase        <pkgname> [<version>]"
    print "       conary localcs      <pkg> <outfile>"
    print "       conary localcommit  <changeset>"
    print "       conary query        <pkgname> [<version>]"
    print "       conary remove       <path>"
    print "       conary repquery     <pkgname> [<version>]"
    print "       conary rblist"
    print "       conary rollback     <rollback>"
    print "       conary showcs       <changeset>"
    print "       conary source       [usage]"
    print "       conary update       <pkgname> <version>"
    print "              update       <changeset>"
    print "       conary usage"
    print ""
    print "commit flags:  --target-branch <branch>"
    print ""
    print 'common flags:  --build-label <label>'
    print '               --config "<item> <value>"'
    print '               --install-label <label>'
    print "               --root <root>"
    print ""
    print "cook flags:    --macros"
    print "               --prep"
    print "               --resume [policy|<lineno>]"
    print "               --debug-exceptions"
    print "               --target-branch <branch>"
    print ""
    print "pkgquery flags: --full-versions"
    print "                --ids"
    print "                --path <file>"
    print "                --ls"
    print "                --sha1s"
    print ""
    print "repquery flags: --all"
    print "                --full-versions"
    print "                --ids"
    print "                --info"
    print "                --leaves"
    print "                --ls"
    print "                --sha1s"
    print "                --tags"
    print ""
    print "update flags: --keep-existing"
    print "              --replace-files"
    return rc

def openRepository(repMap):
    try:
        return helper.openRepository(repMap)
    except repository.repository.OpenError, e:
	log.error('Unable to open repository %s: %s', path, str(e))
	sys.exit(1)

def openDatabase(root, path):
    return database.Database(root, path)

def realMain():
    argDef = {}
    cfgMap = {}

    cfgMap["build-label"] = "buildLabel"
    cfgMap["install-label"] = "installLabel"
    cfgMap["root"] = "root"

    (NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
    (OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

    argDef["all"] = NO_PARAM
    argDef["config"] = MULT_PARAM
    argDef["debug"] = NO_PARAM
    argDef["debug-exceptions"] = NO_PARAM
    argDef["full-versions"] = NO_PARAM
    argDef["ids"] = NO_PARAM
    argDef["info"] = NO_PARAM
    argDef["keep-existing"] = NO_PARAM
    argDef["leaves"] = NO_PARAM
    argDef["path"] = ONE_PARAM
    argDef["ls"] = NO_PARAM
    argDef["macros"] = ONE_PARAM
    argDef["message"] = ONE_PARAM
    argDef["prep"] = NO_PARAM
    argDef["profile"] = NO_PARAM
    argDef["replace-files"] = NO_PARAM
    argDef["resume"] = OPT_PARAM
    argDef["sha1s"] = NO_PARAM
    argDef["tag-script"] = ONE_PARAM
    argDef["tags"] = NO_PARAM
    argDef["target-branch"] = ONE_PARAM

    argDef.update(srcctl.argDef)

    try:
        argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usage)
    except options.OptionError, e:
        sys.exit(e.val)
    except versions.ParseError, e:
	print >> sys.stderr, e
	sys.exit(1)

    if cfg.installLabel is None:
        print >> sys.stderr, "installLabel is not set"
        sys.exit(1)

    profile = False
    if argSet.has_key('profile'):
	import hotshot
	prof = hotshot.Profile('conary.prof')
	prof.start()
	profile = True
	del argSet['profile']

    if (len(otherArgs) < 2):
	return usage()
    elif (otherArgs[1] == "branch"):
	if argSet: return usage
	if len(otherArgs) < 4 or len(otherArgs) > 5: return usage()
	repos = openRepository(cfg.repositoryMap)

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

	repos = openRepository(cfg.repositoryMap)

	cscmd.ChangeSetCommand(repos, cfg, name, outFile, old, new)
    elif (otherArgs[1] == "commit"):
	targetBranch = None
	if argSet.has_key('target-branch'):
	    targetBranch  = argSet['target-branch']
	    del argSet['target-branch']
	if len(otherArgs) < 3: return usage()
	repos = openRepository(cfg.repositoryMap)
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
	resume = None
	buildBranch = None
	if argSet.has_key('prep'):
	    del argSet['prep']
	    prep = 1

	if argSet.has_key('resume'):
	    resume = argSet['resume']
	    del argSet['resume']
	if argSet.has_key('debug-exceptions'):
	    del argSet['debug-exceptions']
	    cfg.debugRecipeExceptions = True
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

	cook.cookCommand(cfg, otherArgs[2:], prep, macros, resume=resume)                
    elif (otherArgs[1] == "emerge"):
	log.setVerbosity(1)

	if argSet: return usage()

	cook.cookCommand(cfg, otherArgs[2:], False, {}, emerge = True)
    elif (otherArgs[1] == "erase"):
	kwargs = {}

	if argSet.has_key('tag-script'):
	    kwargs['tagScript'] = argSet['tag-script']
	    del argSet['tag-script']

	if argSet: return usage()

	if len(otherArgs) >= 3 and len(otherArgs) <=4:
	    db = openDatabase(cfg.root, cfg.dbPath)

	    args = [db, cfg] + otherArgs[2:]
	    updatecmd.doErase(*args, **kwargs)
	else:
	    return usage()
    elif (otherArgs[1] == "import"):
	if len(otherArgs) != 3 and len(otherArgs) != 3:
	    return usage()

	repos = openRepository(cfg.repositoryMap)
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
    elif (otherArgs[1] == "pkgquery") or (otherArgs[1] == "pq") \
	or (otherArgs[1] == "query") or (otherArgs[1] == "q") \
	or (otherArgs[1] == "pkglist"):
	if otherArgs[1] != "query" and otherArgs[1] != "q":
	    log.warning("Outdated syntax: use query (or just q)")

	if argSet.has_key('path'):
	    path = argSet['path']
	    del argSet['path']
	else:
	    path = None
	
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
	    args = [db, cfg, ls, ids, sha1s, fullVersions, path] + otherArgs[2:]
	    try:
		display.displayTroves(*args)
	    except IOError, msg:
		sys.stderr.write(msg.strerror + '\n')
		return 1
	else:
	    return usage()
    elif (otherArgs[1] == "repquery") or (otherArgs[1] == "rq") \
	    or (otherArgs[1] == "replist"):
	if otherArgs[1] == "replist":
	    log.error("Outdated syntax: use repquery or rq")
	    return 1
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

	tags = argSet.has_key('tags')
	if tags: del argSet['tags']

	sha1s = argSet.has_key('sha1s')
	if sha1s: del argSet['sha1s']

	leaves = argSet.has_key('leaves')
	if leaves: del argSet['leaves']

	repos = openRepository(cfg.repositoryMap)

	if argSet: return usage()

	if len(otherArgs) >= 2 and len(otherArgs) <= 4:
	    args = [repos, cfg, all, ls, ids, sha1s, leaves, fullVersions,
		    info, tags] + otherArgs[2:]
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
    elif (otherArgs[1] == "showcs" or otherArgs[1] == "scs"):
	changesets = otherArgs[2:]
	for changeset in changesets:
	    cs = repository.changeset.ChangeSetFromFile(changeset)
	    cs.formatToFile(cfg, sys.stdout)
    elif (otherArgs[1] == "source" or otherArgs[1] == "src"):
	return srcctl.sourceCommand(cfg, otherArgs[2:], argSet)
    elif (otherArgs[1] == "update"):
	kwargs = {}

	replaceFiles = argSet.has_key('replace-files')
	if replaceFiles:
	    kwargs['replaceFiles'] = True
	    del argSet['replace-files']

	keepExisting = argSet.has_key('keep-existing')
	if keepExisting:
	    kwargs['keepExisting'] = True
	    del argSet['keep-existing']

	if argSet.has_key('tag-script'):
	    kwargs['tagScript'] = argSet['tag-script']
	    del argSet['tag-script']

	if argSet: return usage
	if len(otherArgs) >=3 and len(otherArgs) <= 4:
	    repos = openRepository(cfg.repositoryMap)

	    args = [repos, cfg] + otherArgs[2:]
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
    except netclient.UnknownException, e:
	print >> sys.stderr, \
	    "An unknown exception occured on the repository server:"
	print >> sys.stderr, "\t%s" % str(e)
    except repository.repository.TroveMissing, e:
	print >> sys.stderr, str(e)
    except database.OpenError, e:
	print >> sys.stderr, str(e)
    except repository.repository.OpenError, e:
	print >> sys.stderr, str(e)
	    
if __name__ == "__main__":
    sys.exit(main())
