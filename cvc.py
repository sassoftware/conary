# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import checkin
import repository
import sys

argDef = {}
argDef['dir'] = 1

def usage(rc = 1):
    print "usage: srs source add <file>"
    print "       srs source checkin <file>"
    print "       srs source checkout [--dir <dir>] <group> <version>"
    print "       srs source commit"
    print "       srs source diff"
    print "       srs source newpkg <name>"
    print "       srs source remove"
    print "       srs source update"
    sys.exit(rc)

def sourceCommand(cfg, args, argSet):
    if not args:
	usage()
    elif (args[0] == "add"):
	if len(args) != 2: usage()
	checkin.addFile(args[1])
    elif (args[0] == "checkin"):
	if argSet or len(args) != 2: usage()
	repos = repository.LocalRepository(cfg.reppath, "c")
	checkin.checkin(repos, cfg, args[2])
    elif (args[0] == "checkout"):
	if argSet.has_key("dir"):
	    dir = argSet['dir']
	    del argSet['dir']
	else:
	    dir = None

	if argSet or (len(args) < 2 or len(args) > 3): usage()
	repos = repository.LocalRepository(cfg.reppath, "r")

	args = [repos, cfg, dir] + args[1:]
	checkin.checkout(*args)
    elif (args[0] == "commit"):
	if len(args) != 1: usage()
	repos = repository.LocalRepository(cfg.reppath, "w")

	if argSet or len(args) != 1: usage()
	checkin.commit(repos)
    elif (args[0] == "diff"):
	if len(args) != 1: usage()
	repos = repository.LocalRepository(cfg.reppath, "r")

	if argSet or len(args) != 1: usage()
	checkin.diff(repos)
    elif (args[0] == "remove"):
	if len(args) != 2: usage()
	checkin.removeFile(args[1])
    elif (args[0] == "newpkg"):
	if len(args) != 2: usage()
	repos = repository.LocalRepository(cfg.reppath, "r")

	checkin.newPackage(repos, cfg, args[1])
    elif (args[0] == "update"):
	if len(args) != 1: usage()
	repos = repository.LocalRepository(cfg.reppath, "r")

	if argSet or len(args) != 1: usage()
	checkin.update(repos)
    elif (args[0] == "usage"):
	usage(rc = 0)
    else:
	usage()
