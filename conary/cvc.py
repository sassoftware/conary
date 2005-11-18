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

import os
import sys
import xmlrpclib

from conary import branch
from conary import checkin
from conary import conarycfg
from conary import constants
from conary import deps
from conary import flavorcfg
from conary import updatecmd
from conary import versions
from conary.build import cook, use, signtrove
from conary.build import errors as builderrors
from conary.lib import log
from conary.lib import openpgpfile
from conary.lib import options
from conary.lib import util
from conary.local import database
from conary.repository import errors, netclient
from conary.repository.netclient import NetworkRepositoryClient

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
    print "       cvc clone <target-branch> <trove>[=<version>][[flavor]]+ "
    print "                 [--skip-build-info]"
    print "       cvc commit [--message <message>]"
    print '                  [--signature-key "<fingerprint>"]'
    print "       cvc config"
    print "       cvc context [<name>] [--ask]"
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
    print "branch flags:  --binary-only"
    print "               --info"
    print "               --source-only"
    print
    print "clone flags:   --info"
    print "               --skip-build-info"
    print
    print "commit flags:  --message <msg>"
    print 
    print 'common flags:  --build-label <label>'
    print '               --config-file <path>'
    print '               --config "<item> <value>"'
    print '               --context <context>'
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
    print "shadow flags:  --binary-only"
    print "               --info"
    print "               --source-only"
    print ""
    print "sign flags:    --quiet"
    print '               --signature-key "<fingerprint>"'
    return rc

def realMain(cfg, argv=sys.argv):
    argDef = {}
    cfgMap = {}

    cfgMap["build-label"] = "buildLabel"
    cfgMap["signature-key"] = "signatureKey"
    cfgMap["trust-threshold"] = "trustThreshold"

    (NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
    (OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

    argDef["ask"] = NO_PARAM
    argDef["binary-only"] = NO_PARAM
    argDef["config"] = MULT_PARAM
    argDef["config-file"] = ONE_PARAM
    argDef["context"] = ONE_PARAM
    argDef["debug-exceptions"] = NO_PARAM
    argDef["dir"] = ONE_PARAM
    argDef["flavor"] = ONE_PARAM
    argDef["info"] = NO_PARAM
    argDef["macro"] = MULT_PARAM
    argDef["macros"] = ONE_PARAM
    argDef["message"] = ONE_PARAM
    argDef["no-clean"] = NO_PARAM
    argDef["no-deps"] = NO_PARAM
    argDef["prep"] = NO_PARAM
    argDef["profile"] = NO_PARAM
    argDef["quiet"] = NO_PARAM
    argDef["recurse"] = NO_PARAM
    argDef["replace-files"] = NO_PARAM
    argDef["resume"] = OPT_PARAM
    argDef["sha1s"] = NO_PARAM
    argDef["show-passwords"] = NO_PARAM
    argDef["show-contexts"] = NO_PARAM
    argDef["skip-build-info"] = NO_PARAM
    argDef["sources"] = NO_PARAM
    argDef["source-only"] = NO_PARAM
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

    context = cfg.context
    if os.path.exists('CONARY'):
        state = checkin.ConaryStateFromFile('CONARY')
        if state.hasContext():
            context = state.getContext()

    context = os.environ.get('CONARY_CONTEXT', context)
    context = argSet.pop('context', context)

    if context:
        cfg.setContext(context)

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
        makeShadow =  (args[0] == "shadow")
        sourceOnly = argSet.pop('source-only', False)
        binaryOnly = argSet.pop('binary-only', False)
        info = argSet.pop('info', False)

        if argSet: return usage()
        if len(args) < 3: return usage()

	repos = NetworkRepositoryClient(cfg.repositoryMap)
        target = args[1]
        troveSpecs = args[2:]

        branch.branch(repos, cfg, target, troveSpecs, makeShadow = makeShadow, 
                      sourceOnly = sourceOnly, binaryOnly = binaryOnly,
                      info = info)

    elif (args[0] == "commit") or (args[0] == "ci"): # mimic cvs's shortcuts
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
	showPasswords = argSet.pop('show-passwords', False)
	showContexts = argSet.pop('show-contexts', False)
        try:
            prettyPrint = sys.stdout.isatty()
        except AttributeError:
            prettyPrint = False
        cfg.setDisplayOptions(hidePasswords=not showPasswords,
                              showContexts=showContexts,
                              prettyPrint=prettyPrint)
	if argSet: return usage()
	if (len(args) > 2):
	    return usage()
	else:
	    cfg.display()
    elif (args[0] == "clone"):
        if len(args) < 3:
            return usage()

        import clone
        skipBuildInfo = argSet.pop('skip-build-info', False)
        info = argSet.pop('info', False)
        if argSet: return usage()
        clone.CloneTrove(cfg, args[1], args[2:], not skipBuildInfo, info = info)
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
    elif (args[0] == "remove") or (args[0] == "rm"):
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
        recurse = argSet.pop('recurse', False)
        signtrove.signTroves(cfg, args[1:], recurse)
    elif (args[0] == "newpkg"):
        dir = argSet.pop('dir', None)

	if len(args) != 2 or argSet: return usage()
	
	try:
	    repos = NetworkRepositoryClient(cfg.repositoryMap)
	except errors.OpenError:
	    repos = None

	checkin.newTrove(repos, cfg, args[1], dir = dir)
    elif (args[0] == "context"):
        if len(args) > 2:
            return usage()

        ask = argSet.pop('ask', False)
        if len(args) > 1:
            name = args[1]
        else:
            name = None

	checkin.setContext(cfg, name, ask=ask)
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
        state = checkin.ConaryStateFromFile("CONARY").getSourceState()
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
	return realMain(cfg, argv)
    except conarycfg.ConaryCfgError, e:
        log.error(str(e))
        sys.exit(1)
    except xmlrpclib.ProtocolError, e:
        if e.errcode == 403:
            print >> sys.stderr, \
                "remote server denied permission for the requested operation"
        else:
            raise
    except errors.UnknownException, e:
        print >> sys.stderr, \
            "An unknown exception occured on the repository server:"
        print >> sys.stderr, "\t%s" % str(e)
    except errors.RepositoryError, e:
        print >> sys.stderr, str(e)
    except builderrors.BuildError, e:
        print >> sys.stderr, str(e)
    except database.OpenError, e:
        print >> sys.stderr, str(e)
    except checkin.ConaryStateError, e:
        print >> sys.stderr, str(e)
    except openpgpfile.KeyNotFound, e:
        print >> sys.stderr, str(e)
    except KeyboardInterrupt, e:
        pass
    return 1


if __name__ == "__main__":
    sys.exit(main())

