# -*- mode: python -*-
#
# Copyright (c) 2004-2005 rPath, Inc.
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
from build import cook,use,signtrove
import deps
from repository import netclient
from repository.netclient import NetworkRepositoryClient
import branch
import checkin
import conarycfg
import constants
import flavorcfg
import repository
import updatecmd
import versions
import xmlrpclib
from lib import openpgpfile

sys.excepthook = util.genExcepthook()

# mix UpdateCallback and CookCallback, since we use both.
class CheckinCallback(updatecmd.UpdateCallback, cook.CookCallback):
    def __init__(self):
        updatecmd.UpdateCallback.__init__(self)
        cook.CookCallback.__init__(self)

def usage(rc = 1):
    print "usage: cvc add <file> [<file2> <file3> ...]"
    print "       cvc annotate <file>"
    print "       cvc branch <newbranch> <trove>[=<version>][[flavor]]"
    print "       cvc checkout [--dir <dir>] [--trust-threshold <int>]"
    print "                    <trove>[=<version>]"
    print "       cvc clone <target-branch> <trove>[=<version>]"
    print "       cvc commit [--message <message>]"
    print '                  [--signature-key "<fingerprint>"]'
    print "       cvc config"
    print '       cvc cook [--prep] [--debug-exceptions] [--macros file] '
    print '                [--flavor  "<flavor>"] '
    print '                [--signature-key "<fingerprint>"]'
    print '                [--macro "<macro> <value>"]+ '
    print '                <file.recipe|troveName=<version>>[[flavor]]+'
    print '       cvc describe <xml file>'
    print "       cvc diff"
    print "       cvc log [<branch>]"
    print "       cvc newpkg [--dir <dir>] <name>"
    print "       cvc merge"
    print "       cvc rdiff <name> <oldver> <newver>"
    print "       cvc remove <file> [<file2> <file3> ...]"
    print "       cvc rename <oldfile> <newfile>"
    print "       cvc shadow <newshadow> <trove>[=<version>][[flavor]]"
    print '       cvc sign [--signature-key "<fingerprint>"]'
    print '                [--quiet] <trove>[=version][[flavor]]'
    print "                <trove2>[=version2][[flavor2]]..."
    print "       cvc update <version>"
    print 
    print "branch flags:  --sources"
    print
    print 'common flags:  --build-label <label>'
    print '               --config-file <path>'
    print '               --config "<item> <value>"'
    print '               --install-label <label>'
    print "               --root <root>"
    print '               --signature-key "<fingerprint>"'
    print '               --trust-threshold <int>'
    print ""
    print "cook flags:    --macros"
    print "               --no-clean"
    print '               --unknown-flags'
    print '               --no-deps'
    print '               --flavor  "<flavor>"'
    print '               --macro "<macro> <value>"'
    print "               --prep"
    print "               --resume [policy|<linenums>]"
    print "               --debug-exceptions"
    print "               --quiet"
    print ""
    print ""
    print "sign flags:    --quiet"
    print '               --signature-key "<fingerprint>"'
    print ""
    print "commit flags:  --message <msg>"
    print ""
    print "shadow flags:  --sources"
    return rc

def realMain(cfg, argv=sys.argv):
    argDef = {}
    cfgMap = {}

    cfgMap["build-label"] = "buildLabel"
    cfgMap["signature-key"] = "signatureKey"
    cfgMap["trust-threshold"] = "trustThreshold"

    (NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
    (OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

    argDef["config"] = MULT_PARAM
    argDef["config-file"] = ONE_PARAM
    argDef["debug"] = NO_PARAM
    argDef["debug-exceptions"] = NO_PARAM
    argDef["dir"] = ONE_PARAM
    argDef["flavor"] = ONE_PARAM
    argDef["macro"] = MULT_PARAM
    argDef["macros"] = ONE_PARAM
    argDef["message"] = ONE_PARAM
    argDef["no-clean"] = NO_PARAM
    argDef["no-deps"] = NO_PARAM
    argDef["prep"] = NO_PARAM
    argDef["profile"] = NO_PARAM
    argDef["quiet"] = NO_PARAM
    argDef["replace-files"] = NO_PARAM
    argDef["resume"] = OPT_PARAM
    argDef["sha1s"] = NO_PARAM
    argDef["sources"] = NO_PARAM
    argDef["tag-script"] = ONE_PARAM
    argDef["tags"] = NO_PARAM
    argDef["unknown-flags"] = NO_PARAM
    argDef["version"] = NO_PARAM

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

    cfg.initializeFlavors()
    # set the build flavor here, just to set architecture information 
    # which is used when initializing a recipe class
    use.setBuildFlagsFromFlavor(None, cfg.buildFlavor, error=False)
    if 'profile' in argSet:
	del argSet['profile']
	import hotshot
        # XXX note that this profile is currently useless for cook - 
        # hotshot seems to not have the correct frame information about this
        # process.
        # Instead, the conary-cook.prof profile has information about the 
        # forked cook process, which is generally more interesting anyway  
	prof = hotshot.Profile('conary.prof')
	prof.start()
        try:
            sourceCommand(cfg, otherArgs[1:], argSet, profile=True)
        finally:
            prof.stop()
    else:
        sourceCommand(cfg, otherArgs[1:], argSet)

def sourceCommand(cfg, args, argSet, profile=False, callback = None):
    if not callback:
        callback = CheckinCallback()

    if not args:
	return usage()
    elif (args[0] == "add"):
	if len(args) < 2: return usage()
	checkin.addFiles(args[1:])
    elif (args[0] == "checkout") or (args[0] == "co"):
	if argSet.has_key("dir"):
	    dir = argSet['dir']
	    del argSet['dir']
	else:
	    dir = None

	if argSet or (len(args) != 2): return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)

	args = [repos, cfg, dir, args[1], callback]
	checkin.checkout(*args)
    elif (args[0] == "branch" or args[0] == "shadow"):
        extraArgs = { 'makeShadow' : (args[0] == "shadow") }

        extraArgs['sourceTroves'] = argSet.has_key('sources')
        if extraArgs['sourceTroves']:
            del argSet['sources']

        if argSet: return usage()
        if len(args) != 3: return usage()

	repos = NetworkRepositoryClient(cfg.repositoryMap)

        args = [repos, cfg, ] + args[1:] 
        branch.branch(*args, **extraArgs)
    elif (args[0] == "commit") or (args[0] == "ci") # mimic cvs's shortcuts
        level = log.getVerbosity()
        log.setVerbosity(log.INFO)
	message = argSet.get("message", None)
        sourceCheck = True

	if message is not None:
	    del argSet['message']

	if argSet or len(args) != 1: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)

	checkin.commit(repos, cfg, message, callback=callback)
        log.setVerbosity(level)
    elif (args[0] == "config"):
	if argSet: return usage()
	if (len(args) >= 2):
	    return usage()
	else:
	    cfg.display()
    elif (args[0] == "clone"):
        if argSet: return usage()
        if len(args) != 3:
            return usage()

        import clone
        clone.CloneTrove(cfg, args[1], args[2])
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

	checkin.rdiff(repos, cfg.buildLabel,  *args[1:])
    elif (args[0] == "remove"):
	if len(args) < 2: return usage()
        for f in args[1:]:
            checkin.removeFile(f)
    elif (args[0] == "rename"):
	if len(args) != 3: return usage()
	checkin.renameFile(args[1], args[2])
    elif (args[0] == "sign"):
        if len(args) <2: return usage()
        if argSet.has_key('quiet'):
            cfg.quiet = True
            del argSet['quiet']
        signtrove.signTroves(cfg, args[1:])
    elif (args[0] == "newpkg"):
	if argSet.has_key("dir"):
	    dir = argSet['dir']
	    del argSet['dir']
	else:
	    dir = None

	if len(args) != 2 or argSet: return usage()
	
	try:
	    repos = NetworkRepositoryClient(cfg.repositoryMap)
	except repository.repository.OpenError:
	    repos = None

	checkin.newTrove(repos, cfg, args[1], dir = dir)
    elif (args[0] == "merge"):
	if argSet or not args or len(args) > 1: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)
        
	checkin.merge(repos)
    elif (args[0] == "update") or (args[0] == "up"):
	if argSet or not args or len(args) > 2: return usage()
	repos = NetworkRepositoryClient(cfg.repositoryMap)

	args[0] = repos
        kwargs = {'callback': callback}
	checkin.updateSrc(*args, **kwargs)
    elif (args[0] == "cook"):
        level = log.getVerbosity()
        log.setVerbosity(log.DEBUG)
        macros = {}
        prep = 0
        resume = None
        buildBranch = None
        if argSet.has_key('flavor'):
            buildFlavor = deps.deps.parseFlavor(argSet['flavor'])
            cfg.buildFlavor = deps.deps.overrideFlavor(cfg.buildFlavor,
                                                       buildFlavor)
            del argSet['flavor']
        if argSet.has_key('macro'):
            for macro in argSet['macro']:
                cfg.configLine('macros.' + macro)
            del argSet['macro']

        if argSet.has_key('prep'):
            del argSet['prep']
            prep = 1
        if argSet.has_key('no-deps'):
            del argSet['no-deps']
            ignoreDeps = True
        else:
            ignoreDeps = False

        if argSet.has_key('quiet'):
            cfg.quiet = True
            del argSet['quiet']

        if argSet.has_key('no-clean'):
            del argSet['no-clean']
            cfg.noClean = True
        else:
            cfg.noClean = False
        if argSet.has_key('resume'):
            resume = argSet['resume']
            del argSet['resume']
        if argSet.has_key('unknown-flags'):
            unknownFlags = argSet['unknown-flags']
            del argSet['unknown-flags']
        else:
            unknownFlags = False
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

        if argSet: return usage()
        
        cook.cookCommand(cfg, args[1:], prep, macros, resume=resume, 
                         allowUnknownFlags=unknownFlags, ignoreDeps=ignoreDeps,
                         profile=profile)
        log.setVerbosity(level)
    elif (args[0] == "describe"):
        level = log.getVerbosity()
        log.setVerbosity(log.INFO)
        
        xmlSource = args[1]
        state = checkin.SourceStateFromFile("CONARY")
        troveName = state.getName()
        troveBranch = state.getVersion().branch()
       
        log.info("describing trove %s with %s", troveName, xmlSource)
        xmlFile = open(xmlSource)
        xml = xmlFile.read()

        repos = NetworkRepositoryClient(cfg.repositoryMap)
        repos.updateMetadataFromXML(troveName, troveBranch, xml)
        log.setVerbosity(level)
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
    except repository.repository.TroveNotFound, e:
        print >> sys.stderr, str(e)
    except repository.repository.TroveMissing, e:
        print >> sys.stderr, str(e)
    except database.OpenError, e:
        print >> sys.stderr, str(e)
    except repository.repository.OpenError, e:
        print >> sys.stderr, str(e)
    except repository.repository.DuplicateBranch, e:
        print >> sys.stderr, str(e)
    except checkin.CONARYFileMissing, e:
        print >> sys.stderr, str(e)
    except openpgpfile.KeyNotFound, e:
        print >> sys.stderr, str(e)
    except KeyboardInterrupt, e:
        pass


if __name__ == "__main__":
    sys.exit(main())

