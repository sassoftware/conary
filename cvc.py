# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import checkin
import repository
import sys

def usage(rc = 1):
    print "usage: srs source checkin <file>"
    print "       srs source checkout <group> <version>"
    sys.exit(rc)

def sourceCommand(cfg, args, argSet):
    if not args:
	usage()
    elif (args[0] == "usage"):
	usage(rc = 0)
    elif (args[0] == "checkin"):
	if len(args) != 2: usage()
	repos = repository.LocalRepository(cfg.reppath, "c")
	checkin.checkin(repos, cfg, args[2])
    elif (args[0] == "checkout"):
	if len(args) < 2 or len(args) > 3: usage()
	repos = repository.LocalRepository(cfg.reppath, "r")

	args = [repos, cfg] + args[1:]
	checkin.checkout(*args)
