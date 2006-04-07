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

import inspect
import optparse
import os
import sys

from conary import branch
from conary import checkin
from conary import conarycfg
from conary import conaryclient
from conary import constants
from conary import deps
from conary import errors
from conary import state
from conary import updatecmd
from conary import versions
from conary.build import cook, use, signtrove
from conary.lib import cfg
from conary.lib import log
from conary.lib import openpgpkey
from conary.lib import options
from conary.lib import util

sys.excepthook = util.genExcepthook()

# mix UpdateCallback and CookCallback, since we use both.
class CheckinCallback(updatecmd.UpdateCallback, cook.CookCallback):
    def __init__(self, cfg=None):
        updatecmd.UpdateCallback.__init__(self, cfg)
        cook.CookCallback.__init__(self)

def usage(rc = 1):
    print "usage: cvc add <file> [<file2> <file3> ...]"
    print "       cvc annotate <file>"
    print "       cvc branch <newbranch> <trove>[=<version>][[flavor]]"
    print "       cvc checkout <trove>[=<version>]"
    print "       cvc clone <target-branch> <trove>[=<version>][[flavor]]+ "
    print "       cvc commit"
    print "       cvc config"
    print "       cvc context"
    print '       cvc cook <file.recipe|troveName=<version>>[[flavor]]+'
    print '       cvc describe <xml file>'
    print "       cvc diff"
    print "       cvc log"
    print "       cvc newpkg <name>"
    print "       cvc merge"
    print "       cvc rdiff <name> <oldver> <newver>"
    print "       cvc remove <file> [<file2> <file3> ...]"
    print "       cvc rename <oldfile> <newfile>"
    print "       cvc shadow <newshadow> <trove>[=<version>][[flavor]]"
    print '       cvc sign <trove>[=version][[flavor]]+'
    print "       cvc update [<version>]"
    print ""
    print "type 'cvc <command> --help' for command-specific usage"
    return rc

(NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
(OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

supportedCommands = {}
def _register(commandClass):
    inst = commandClass()
    if isinstance(commandClass.commands, str):
        supportedCommands[commandClass.commands] = inst
    else:
        for cmdName in commandClass.commands:
            supportedCommands[cmdName] = inst


class CvcCommand(object):

    paramHelp = ''
    defaultGroup = 'Common Options'

    docs = {'build-label'        : ('Use build label LABEL as default search'
                                    ' loc', 'LABEL'),
            'config'             : ("Set config KEY to VALUE", "'KEY VALUE'"),
            'config-file'        : ("Read PATH config file", "PATH"),
            'context'            : "Set the current context",
            'install-label'      : ("Set the install label", "LABEL"),
            'interactive'        : ('ask questions before performing actions '
                                    'that change system or repository state'),
            'flavors'            : 'Display complete flavors where applicable',
            'full-versions'      : ('Always display complete version strings'),
            'profile'            : optparse.SUPPRESS_HELP,
            'skip-default-config': "Don't read default configs",
            'signature-key'      : ("Use signature key to sign results", 'KEY'),
            'quiet'              : ('do not display extra information when '
                                    'running'),
            'root'               : 'use conary database at location ROOT'
            }

    def __init__(self):
        self.parser = None


    def usage(self, errNo=1):
        if self.parser:
            self.parser.print_help()
        return errNo

    def setParser(self, parser):
        self.parser = parser

    def addParameters(self, argDef):
        d = {}
        d["config"] = MULT_PARAM
        d["config-file"] = ONE_PARAM
        d["context"] = ONE_PARAM
        d["install-label"] = MULT_PARAM
        d["profile"] = NO_PARAM
        d["skip-default-config"] = NO_PARAM
        argDef[self.defaultGroup] = d

    def addConfigOptions(self, cfgMap, argDef):
        cfgMap["build-label"]   = "buildLabel", ONE_PARAM,
        cfgMap['interactive']   = 'interactive', NO_PARAM,
        cfgMap['full-versions'] = 'fullVersions', NO_PARAM
        cfgMap['flavors']       = 'fullFlavors', NO_PARAM
        cfgMap["pubring"]       = "pubRing", ONE_PARAM
        cfgMap["quiet"]         = "quiet", NO_PARAM,
        cfgMap["root"]          = "root", ONE_PARAM,
        cfgMap['signature-key'] = 'signatureKey', ONE_PARAM


        for name, (cfgName, paramType)  in cfgMap.items():
            # if it's a NO_PARAM
            if paramType == NO_PARAM:
                negName = 'no-' + name
                argDef[self.defaultGroup][negName] = NO_PARAM, optparse.SUPPRESS_HELP
                cfgMap[negName] = (cfgName, paramType)

            argDef[self.defaultGroup][name] = paramType

    def processConfigOptions(self, cfg, cfgMap, argSet):
        # command line configuration overrides contexts.
        for (arg, (name, paramType)) in cfgMap.items():
            value = argSet.pop(arg, None)
            if value is not None:
                if arg.startswith('no-'):
                    value = not value

                cfg.configLine("%s %s" % (name, value))

        for line in argSet.pop('config', []):
            cfg.configLine(line)

        l = []
        for labelStr in argSet.get('install-label', []):
            l.append(versions.Label(labelStr))
        if l:
            cfg.installLabelPath = l
            del argSet['install-label']


    def addDocs(self, argDef):
        d = {}
        for class_ in reversed(inspect.getmro(self.__class__)):
            if not hasattr(class_, 'docs'):
                continue
            d.update(class_.docs)

        commandDicts = [argDef]
        while commandDicts:
            commandDict = commandDicts.pop()
            for name, value in commandDict.items():
                if isinstance(value, dict):
                    commandDicts.append(value)
                    continue
                if name in d:
                    if not isinstance(value, (list, tuple)):
                        value = [ value ]
                    else:
                        value = list(value)
                    value.append(d[name])
                    commandDict[name] = value

class AddCommand(CvcCommand):
    commands = ['add']
    paramHelp = '<file> [<file2> <file3> ...]'

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if len(args) < 2: return self.usage()
        checkin.addFiles(args[1:])
_register(AddCommand)

class AnnotateCommand(CvcCommand):
    commands = ['annotate']
    paramHelp = '<file>'
    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if argSet or len(args) != 2: return self.usage()
        args[0] = repos
        checkin.annotate(*args)
_register(AnnotateCommand)



class BranchShadowCommand(CvcCommand):

    commands = ['branch', 'shadow']
    paramHelp = "<newlabel> <trove>[=<version>][[flavor]]+"

    docs = {'binary-only': 'Do not shadow/branch any source components listed',
            'source-only': ('For any binary components listed, shadow/branch'
                            ' their sources instead'),
            'info'       : 'Display info on shadow/branch'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["binary-only"] = NO_PARAM
        argDef["source-only"] = NO_PARAM
        argDef["info"] = NO_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        makeShadow =  (args[0] == "shadow")
        sourceOnly = argSet.pop('source-only', False)
        binaryOnly = argSet.pop('binary-only', False)
        info = argSet.pop('info', False)

        if argSet: return self.usage()
        if len(args) < 3: return self.usage()

        target = args[1]
        troveSpecs = args[2:]

        branch.branch(repos, cfg, target, troveSpecs, makeShadow = makeShadow, 
                      sourceOnly = sourceOnly, binaryOnly = binaryOnly,
                      info = info)
_register(BranchShadowCommand)

class CheckoutCommand(CvcCommand):
    commands = ['checkout', 'co']
    paramHelp = '<trove>[=<version>]'

    docs = {'dir': 'Check out trove in directory DIR'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["dir"] = ONE_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if argSet.has_key("dir"):
            dir = argSet['dir']
            del argSet['dir']
        else:
            dir = None
        if argSet or (len(args) != 2): return self.usage()
        args = [repos, cfg, dir, args[1], callback]
        checkin.checkout(*args)
_register(CheckoutCommand)


class CloneCommand(CvcCommand):

    commands = 'clone'
    paramHelp = '<target-branch> <trove>[=<version>][[flavor]]+'

    docs = { 'skip-build-info' : ('Do not attempt to rewrite version'
                                  'information about how this trove was built'),
             'info'            : 'Do not perform clone',
             'with-sources'    : ('Ensure that any binaries that are being'
                                  ' cloned also have a matching source component')
           }

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["skip-build-info"] = NO_PARAM
        argDef["info"] = NO_PARAM
        argDef["with-sources"] = NO_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if len(args) < 3:
            return self.usage()

        from conary import clone
        skipBuildInfo = argSet.pop('skip-build-info', False)
        info = argSet.pop('info', False)
        cloneSources = argSet.pop('with-sources', False)
        if argSet: return self.usage()
        clone.CloneTrove(cfg, args[1], args[2:], not skipBuildInfo, info = info,
                         cloneSources=cloneSources)
_register(CloneCommand)



class CommitCommand(CvcCommand):

    commands = ['commit', 'ci']

    docs = {'message': 'Use MESSAGE to describe why the commit was performed'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["message"] = '-m', ONE_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        level = log.getVerbosity()
        if level > log.INFO:
            log.setVerbosity(log.INFO)
        message = argSet.get("message", None)
        sourceCheck = True

        if message is not None:
            del argSet['message']

        if argSet or len(args) != 1: return self.usage()

        checkin.commit(repos, cfg, message, callback=callback)
        log.setVerbosity(level)
_register(CommitCommand)

class ConfigCommand(CvcCommand):
    commands = ['config']

    docs = {'show-contexts'  : 'display contexts as well as current config',
            'show-passwords' : 'do not mask passwords'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["show-contexts"] = NO_PARAM
        argDef["show-passwords"] = NO_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        showPasswords = argSet.pop('show-passwords', False)
        showContexts = argSet.pop('show-contexts', False)
        try:
            prettyPrint = sys.stdout.isatty()
        except AttributeError:
            prettyPrint = False
        cfg.setDisplayOptions(hidePasswords=not showPasswords,
                              showContexts=showContexts,
                              prettyPrint=prettyPrint)
        if argSet: return self.usage()
        if (len(args) > 2):
            return self.usage()
        else:
            cfg.display()
_register(ConfigCommand)


class ContextCommand(CvcCommand):
    commands = ['context']
    paramHelp = '[CONTEXT]'

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["ask"] = NO_PARAM

    docs = {'ask' : 'If not defined, create CONTEXT by answering questions'}

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if len(args) > 2:
            return self.usage()

        ask = argSet.pop('ask', False)
        if len(args) > 1:
            name = args[1]
        else:
            name = None

        checkin.setContext(cfg, name, ask=ask)
_register(ContextCommand)


class CookCommand(CvcCommand):
    commands = ['cook']
    paramHelp = '<file.recipe|troveName=<version>>[[flavor]]+'

    docs = {'cross'   : ('set macros for cross-compiling', 
                         '[(local|HOST)--]TARGET'),
            'debug-exceptions' : 'Enter debugger if a recipe fails in conary',
            'flavor'  : 'build the trove with flavor FLAVOR',
            'macro'   : ('set macro NAME to VALUE', "'NAME VALUE'"),
            'macros'  : optparse.SUPPRESS_HELP, # can we get rid of this?
            'no-clean': 'do not remove build directory even if build is'
                        ' successful',
            'no-deps' : 'do not check build requirements',
            'show-deps': 'Show build requires for recipe',
            'prep'    : 'unpack, but do not build',
            'resume'  : ('resume building at given loc (default at failure)', 
                         '[LINENO|policy]'),
            'unknown-flags' : optparse.SUPPRESS_HELP,
           }

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['debug-exceptions'] = NO_PARAM
        argDef['cross'] = ONE_PARAM
        argDef['flavor'] = ONE_PARAM
        argDef['macro'] = MULT_PARAM
        argDef['macros'] = ONE_PARAM
        argDef['no-clean'] = NO_PARAM
        argDef['no-deps'] = NO_PARAM
        argDef['show-deps' ] = NO_PARAM
        argDef['prep'] = NO_PARAM
        argDef['resume'] = OPT_PARAM
        argDef['unknown-flags'] = NO_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        level = log.getVerbosity()
        if level > log.INFO:
            log.setVerbosity(log.INFO)
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
                cfg.configLine('macros ' + macro)
            del argSet['macro']

        if argSet.has_key('prep'):
            del argSet['prep']
            prep = 1
        if argSet.has_key('no-deps'):
            del argSet['no-deps']
            ignoreDeps = True
        else:
            ignoreDeps = False

        showBuildReqs = argSet.pop('show-deps', False)

        if argSet.has_key('quiet'):
            cfg.quiet = True
            del argSet['quiet']

        if 'no-clean' in argSet:
            cfg.cleanAfterCook = False
            del argSet['no-clean']

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
            f.close()
            del f
            del argSet['macros']

        crossCompile = argSet.pop('cross', None)
        if crossCompile:   
            parts = crossCompile.split('--')
            isCrossTool = False

            if len(parts) == 1:
                crossTarget = crossCompile
                crossHost = None
            else:
                crossHost, crossTarget = parts
                if crossHost == 'local':
                    crossHost = None
                    isCrossTool = True

            crossCompile = (crossHost, crossTarget, isCrossTool)

        if argSet: return self.usage()

        cook.cookCommand(cfg, args[1:], prep, macros, resume=resume, 
                         allowUnknownFlags=unknownFlags, ignoreDeps=ignoreDeps,
                         showBuildReqs=showBuildReqs, profile=profile,
                         crossCompile=crossCompile)
        log.setVerbosity(level)
_register(CookCommand)

class DescribeCommand(CvcCommand):
    commands = ['describe']
    paramHelp = '<xml file>'

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        level = log.getVerbosity()
        if level > log.INFO:
            log.setVerbosity(log.INFO)

        xmlSource = args[1]
        conaryState = state.ConaryStateFromFile("CONARY").getSourceState()
        troveName = conaryState.getName()
        troveBranch = conaryState.getVersion().branch()

        log.info("describing trove %s with %s", troveName, xmlSource)
        xmlFile = open(xmlSource)
        xml = xmlFile.read()

        repos.updateMetadataFromXML(troveName, troveBranch, xml)
        log.setVerbosity(level)
_register(DescribeCommand)



class DiffCommand(CvcCommand):
    commands = 'diff'
    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if argSet or not args or len(args) > 2: return self.usage()

        args[0] = repos
        checkin.diff(*args)
_register(DiffCommand)

class LogCommand(CvcCommand):
    commands = 'log'

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if argSet or len(args) > 2: return self.usage()

        args[0] = repos
        checkin.showLog(*args)
_register(LogCommand)

class RdiffCommand(CvcCommand):
    commands = ['rdiff']
    paramHelp = "<name> [<oldver>|-<num>] <newver>"

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if argSet or len(args) != 4: return self.usage()
        checkin.rdiff(repos, cfg.buildLabel,  *args[1:])
_register(RdiffCommand)

class RemoveCommand(CvcCommand):
    commands = ['remove', 'rm']
    paramHelp = "<file> [<file2> <file3> ...]"

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if len(args) < 2: return self.usage()
        for f in args[1:]:
            checkin.removeFile(f)
_register(RemoveCommand)

class RenameCommand(CvcCommand):
    commands = ['rename']
    paramHelp = "<oldfile> <newfile>"

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if len(args) != 3: return self.usage()
        checkin.renameFile(args[1], args[2])
_register(RenameCommand)

class SignCommand(CvcCommand):
    commands = ['sign']
    paramHelp = "<newshadow> <trove>[=<version>][[flavor]]"

    docs = {'recurse' : 'recursively sign child troves'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['recurse'] = NO_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                callback = None):
        if len(args) <2: return self.usage()
        if argSet.has_key('quiet'):
            cfg.quiet = True
            del argSet['quiet']
        recurse = argSet.pop('recurse', False)
        signtrove.signTroves(cfg, args[1:], recurse)
_register(SignCommand)

class NewPkgCommand(CvcCommand):
    commands = ['newpkg']
    paramHelp = '<name>'

    docs = {'dir' : 'create new package in DIR' }

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['dir'] = ONE_PARAM

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                callback = None):
        dir = argSet.pop('dir', None)

        if len(args) != 2 or argSet: return self.usage()
        
        checkin.newTrove(repos, cfg, args[1], dir = dir)
_register(NewPkgCommand)

class MergeCommand(CvcCommand):
    commands = ['merge']

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if argSet or not args or len(args) > 1: return self.usage()

        checkin.merge(repos)
_register(MergeCommand)

class UpdateCommand(CvcCommand):
    commands = ['update', 'up']
    paramHelp = "[<version>]"

    def runCommand(self, repos, cfg, argSet, args, profile = False, 
                   callback = None):
        if argSet or not args or len(args) > 2: return self.usage()

        args[0] = repos
        kwargs = {'callback': callback}
        checkin.updateSrc(*args, **kwargs)
_register(UpdateCommand)

def sourceCommand(cfg, args, argSet, profile=False, callback = None,
                  thisCommand = None):
    if thisCommand is None:
        thisCommand = supportedCommands[args[0]]
    if not callback:
        callback = CheckinCallback(cfg)

    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()
    return thisCommand.runCommand(repos, cfg, argSet, args, profile, callback)

def _getPreCommandOptions(argv, cfg):
    """Allow the user to specify generic flags before they specify the
       command to run.
    """
    cfgMap = {}
    params = {}
    thisCommand = CvcCommand()
    thisCommand.addParameters(params)
    thisCommand.addConfigOptions(cfgMap, params)
    thisCommand.addDocs(params)
    defaultGroup = thisCommand.defaultGroup
    argSet, otherArgs, parser, optionSet = options._processArgs(
                                                params, {}, cfg,
                                                usage,
                                                argv=argv[1:],
                                                version=constants.version,
                                                useHelp=True,
                                                defaultGroup=defaultGroup,
                                                interspersedArgs=False)
    return argSet, [argv[0]] + otherArgs

def realMain(cfg, argv=sys.argv, debugAll=False, 
             debuggerException = errors.InternalConaryError):
    argDef = {}
    if '--version' in argv or '-v' in argv:
        print constants.version
        return

    try:
        # get options before the command.  Only generic options are 
        # allowed.
        argSet, argv = _getPreCommandOptions(argv, cfg)
    except debuggerException:
        raise
    except options.OptionError, e:
        usage()
        print >>sys.stderr, e
        sys.exit(e.val)

    if len(argv) < 2:
        # no command specified
        return usage()

    commandName = argv[1]
    if commandName == 'usage':
        return usage(rc=0)

    if commandName not in supportedCommands:
        return usage()

    params = {}
    cfgMap = {}

    thisCommand = supportedCommands[commandName]
    thisCommand.addParameters(params)
    thisCommand.addConfigOptions(cfgMap, params)
    thisCommand.addDocs(params)

    defaultGroup = thisCommand.defaultGroup
    commandUsage = 'cvc %s %s' % (commandName, thisCommand.paramHelp)

    try:
        newArgSet, otherArgs, parser, optionSet = options._processArgs(
                                                    params, {}, cfg,
                                                    commandUsage,
                                                    argv=argv,
                                                    version=constants.version,
                                                    useHelp=True,
                                                    defaultGroup=defaultGroup)
    except debuggerException, e:
        raise
    except options.OptionError, e:
        e.parser.print_help()
        print >> sys.stderr, e
        sys.exit(e.val)
    except versions.ParseError, e:
        print >> sys.stderr, e
        sys.exit(1)

    argSet.update(newArgSet)

    thisCommand.processConfigOptions(cfg, cfgMap, argSet)

    # the user might have specified --config debugExceptions on the commandline
    sys.excepthook = util.genExcepthook(debug=cfg.debugExceptions,
                                        debugCtrlC=debugAll)

    context = cfg.context
    if os.path.exists('CONARY'):
        conaryState = state.ConaryStateFromFile('CONARY')
        if conaryState.hasContext():
            context = conaryState.getContext()

    context = os.environ.get('CONARY_CONTEXT', context)
    context = argSet.pop('context', context)

    if context:
        cfg.setContext(context)


    if not cfg.buildLabel and cfg.installLabelPath:
        cfg.buildLabel = cfg.installLabelPath[0]

    # now set the debug hook using the potentially new cfg.debugExceptions value
    sys.excepthook = util.genExcepthook(debug=cfg.debugExceptions,
                                        debugCtrlC=debugAll)

    if cfg.installLabelPath:
        cfg.installLabel = cfg.installLabelPath[0]

    cfg.initializeFlavors()

    # set the build flavor here, just to set architecture information 
    # which is used when initializing a recipe class
    use.setBuildFlagsFromFlavor(None, cfg.buildFlavor, error=False)

    profile = False
    if argSet.has_key('profile'):
        import hotshot
        prof = hotshot.Profile('conary.prof')
        prof.start()
        profile = True
        del argSet['profile']

    keyCache = openpgpkey.getKeyCache()
    keyCacheCallback = openpgpkey.KeyCacheCallback(cfg.repositoryMap,
                                                   cfg.pubRing[-1])
    keyCache.setCallback(keyCacheCallback)

    if (len(otherArgs) < 2):
        return usage()

    thisCommand.setParser(parser)

    rv = sourceCommand(cfg, otherArgs[1:], argSet, profile, thisCommand=thisCommand)

    if profile:
        prof.stop()

    if log.errorOccurred():
        sys.exit(1)
    return rv

def main(argv=sys.argv):
    try:
        debugAll = '--debug-all' in argv
        if debugAll:
            argv = argv[:]
            argv.remove('--debug-all')
            debuggerException = Exception
        else:
            debuggerException = errors.InternalConaryError

        if '--skip-default-config' in argv:
            argv = argv[:]
            argv.remove('--skip-default-config')
            ccfg = conarycfg.ConaryConfiguration()
        else:
            ccfg = conarycfg.ConaryConfiguration(readConfigFiles=True)

        if debugAll:
            ccfg.debugExceptions = True

        # reset the excepthook (using cfg values for exception settings)
        sys.excepthook = util.genExcepthook(debug=ccfg.debugExceptions,
                                            debugCtrlC=debugAll)
        return realMain(ccfg, argv, debugAll, debuggerException)
    except debuggerException, err:
        raise
    except (errors.ConaryError, errors.CvcError, cfg.CfgError), e:
        if str(e):
            log.error(str(e))
            sys.exit(1)
        else:
            raise
    except KeyboardInterrupt, e:
        pass
    return 1


if __name__ == "__main__":
    sys.exit(main())

