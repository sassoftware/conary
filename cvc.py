# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from repository import repository
from helper import openRepository
import checkin

argDef = {}
argDef['dir'] = 1

def usage(rc = 1):
    print "usage: conary source add <file> [<file2> <file3> ...]"
    print "       conary source checkout [--dir <dir>] <group> <version>"
    print "       conary source commit"
    print "       conary source diff"
    print "       conary source log [<branch>]"
    print "       conary source newpkg <name>"
    print "       conary source rdiff <name> <oldver> <newver>"
    print "       conary source remove <file> [<file2> <file3> ...]"
    print "       conary source rename <oldfile> <newfile>"
    print "       conary source update <version>"
    print 
    print "commit flags:   --message <msg>"
    return rc

def sourceCommand(cfg, args, argSet):
    if not args:
	return usage()
    elif (args[0] == "add"):
	if len(args) < 2: return usage()
	checkin.addFiles(args[1:])
    elif (args[0] == "checkout"):
	if argSet.has_key("dir"):
	    dir = argSet['dir']
	    del argSet['dir']
	else:
	    dir = None

	if argSet or (len(args) < 2 or len(args) > 3): return usage()
	repos = openRepository(cfg.repositoryMap)

	args = [repos, cfg, dir] + args[1:]
	checkin.checkout(*args)
    elif (args[0] == "commit"):
	message = argSet.get("message", None)
	if message is not None:
	    del argSet['message']

	if argSet or len(args) != 1: return usage()
	repos = openRepository(cfg.repositoryMap)

	checkin.commit(repos, cfg, message)
    elif (args[0] == "diff"):
	if argSet or not args or len(args) > 2: return usage()
	repos = openRepository(cfg.repositoryMap)

	args[0] = repos
	checkin.diff(*args)
    elif (args[0] == "log"):
	if argSet or len(args) > 2: return usage()

	repos = openRepository(cfg.repositoryMap)
	args[0] = repos
	checkin.showLog(*args)
    elif (args[0] == "rdiff"):
	if argSet or len(args) != 4: return usage()
	repos = openRepository(cfg.repositoryMap)

	args[0] = repos
	checkin.rdiff(repos, cfg.buildLabel,  *args[1:])
    elif (args[0] == "remove"):
	if len(args) < 2: return usage()
        for f in args[1:]:
            checkin.removeFile(f)
    elif (args[0] == "rename"):
	if len(args) != 3: return usage()
	checkin.renameFile(args[1], args[2])
    elif (args[0] == "newpkg"):
	if len(args) != 2: return usage()
	
	try:
	    repos = openRepository(cfg.repositoryMap)
	except repository.OpenError:
	    repos = None

	checkin.newPackage(repos, cfg, args[1])
    elif (args[0] == "update"):
	if argSet or not args or len(args) > 2: return usage()
	repos = openRepository(cfg.repositoryMap)

	args[0] = repos
	checkin.updateSrc(*args)
    elif (args[0] == "usage"):
	return usage(rc = 0)
    else:
	return usage()

    return 0
