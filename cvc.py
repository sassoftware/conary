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
"""
Provides the output for the "cvc" subcommands
"""

import sys

from lib import options
from lib import util
from lib import log
from local import database
from build import cook
from repository import netclient
from repository.netclient import NetworkRepositoryClient
import branch
import checkin
import conarycfg
import constants
import repository
import versions
import xmlrpclib

argDef = {}
argDef['dir'] = 1

sys.excepthook = util.genExcepthook()
def usage(rc = 1):
    print "usage: cvc add <file> [<file2> <file3> ...]"
    print "       cvc annotate <file>"
    print "       cvc branch <newbranch> <branchfrom> [<trove>]"
    print "       cvc checkout [--dir <dir>] <trove> <version>"
    print "       cvc commit [--message <message>]"
    print '       cvc cook [--prep] [--debug-exceptions] [--macros file] '
    print '                [--use-flag  "<prefix>.<flag> <bool>"]+ '
    print '                [--use-macro "<macro> <value>"]+ '
    print '                <file.recipe|troveName>+'
    print "       cvc diff"
    print "       cvc log [<branch>]"
    print "       cvc newpkg <name>"
    print "       cvc rdiff <name> <oldver> <newver>"
    print "       cvc remove <file> [<file2> <file3> ...]"
    print "       cvc rename <oldfile> <newfile>"
    print "       cvc update <version>"
    print 
    print 'common flags:  --build-label <label>'
    print '               --config-file <path>'
    print '               --config "<item> <value>"'
    print '               --install-label <label>'
    print "               --root <root>"
    print ""
    print "cook flags:    --macros"
    print "               --noclean"
    print '               --use-flag  "<prefix>.<flag> <bool>"'
    print '               --use-macro "<macro> <value>"'
    print "               --prep"
    print "               --resume [policy|<linenums>]"
    print "               --debug-exceptions"
    print "               --target-branch <branch>"
    print '               --use-flag "<flag> <value>"'
    print ""
    print "commit flags:   --message <msg>"
    
    return rc

def realMain(cfg, argv=sys.argv):
    argDef = {}
    cfgMap = {}

    cfgMap["build-label"] = "buildLabel"

    (NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
    (OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

    argDef["config"] = MULT_PARAM
    argDef["config-file"] = ONE_PARAM
    argDef["debug"] = NO_PARAM
    argDef["debug-exceptions"] = NO_PARAM
    argDef["dir"] = ONE_PARAM
    argDef["use-flag"] = MULT_PARAM
    argDef["use-macro"] = MULT_PARAM
    argDef["macros"] = ONE_PARAM
    argDef["message"] = ONE_PARAM
    argDef["noclean"] = NO_PARAM
    argDef["prep"] = NO_PARAM
    argDef["profile"] = NO_PARAM
    argDef["replace-files"] = NO_PARAM
    argDef["resume"] = OPT_PARAM
    argDef["sha1s"] = NO_PARAM
    argDef["tag-script"] = ONE_PARAM
    argDef["tags"] = NO_PARAM
    argDef["target-branch"] = ONE_PARAM
    argDef["version"] = NO_PARAM

    argDef.update(argDef)

    try:
        argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usage,
                                                argv=argv)
    except options.OptionError, e:
        sys.exit(e.val)
    except versions.ParseError, e:
        print >> sys.stderr, e
        sys.exit(1)

    if argSet.has_key('version'):
        print constants.version
        sys.exit(0)

    sourceCommand(cfg, otherArgs[1:], argSet)

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
	repos = NetworkRepositoryClient(cfg.repositoryMap)

	args = [repos, cfg, dir] + args[1:]
	checkin.checkout(*args)
    elif (args[0] == "branch"):
        if argSet: return usage()
        if len(args) != 4: return usage()
        repos = NetworkRepositoryClient(cfg.repositoryMap)

        args = [repos, ] + args[1:]
        branch.branch(*args)
    elif (args[0] == "commit"):
	message = argSet.get("message", None)
	if message is not None:
	    del argSet['message']

	if argSet or len(args) != 1: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)

	checkin.commit(repos, cfg, message)
    elif (args[0] == "diff"):
	if argSet or not args or len(args) > 2: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)

	args[0] = repos
	checkin.diff(*args)
    elif (args[0] == "annotate"):
	if argSet or len(args) != 2: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)
	args[0] = repos
	checkin.annotate(*args)

    elif (args[0] == "log"):
	if argSet or len(args) > 2: return usage()

	repos = NetworkRepositoryClient(cfg.repositoryMap)
	args[0] = repos
	checkin.showLog(*args)
    elif (args[0] == "rdiff"):
	if argSet or len(args) != 4: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)

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
	    repos = NetworkRepositoryClient(cfg.repositoryMap)
	except repository.OpenError:
	    repos = None

	checkin.newPackage(repos, cfg, args[1])
    elif (args[0] == "update"):
	if argSet or not args or len(args) > 2: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)

	args[0] = repos
	checkin.updateSrc(*args)
    elif (args[0] == "cook"):

        log.setVerbosity(1)
        macros = {}
        prep = 0
        resume = None
        buildBranch = None
        if argSet.has_key('use-flag'):
            for flag in argSet['use-flag']:
                cfg.configLine(flag)
            del argSet['use-flag']

        if argSet.has_key('use-macro'):
            for macro in argSet['use-macro']:
                cfg.configLine('macros.' + macro)
            del argSet['use-macro']

        if argSet.has_key('prep'):
            del argSet['prep']
            prep = 1

        if argSet.has_key('noclean'):
            del argSet['noclean']
            cfg.noClean = True
        else:
            cfg.noClean = False
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
        
        cook.cookCommand(cfg, args[1:], prep, macros, resume=resume)
    elif (args[0] == "usage"):	
        return usage(rc = 0)
    else:
	return usage()

    return 0

def main(argv=sys.argv):
    try:
        if '--skip-default-config' in argv:
            argv = argv[:]
            argv.remove('--skip-default-config')
            cfg = conarycfg.ConaryConfiguration(False)
        else:
            cfg = conarycfg.ConaryConfiguration()
        # reset the excepthook (using cfg values for exception settings)
        sys.excepthook = util.genExcepthook(cfg.dumpStackOnError)
	realMain(cfg, argv)
    except conarycfg.ConaryCfgError, e:
        log.error(str(e))
        sys.exit(1)
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
    except repository.repository.DuplicateBranch, e:
        print >> sys.stderr, str(e)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    sys.exit(main())

