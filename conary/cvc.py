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
# without any warranty; without even the implied warranty of merchantability
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
from conary import command
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
from conary.lib import openpgpfile
from conary.lib import openpgpkey
from conary.lib import options
from conary.lib import util

sys.excepthook = util.genExcepthook()

# mix UpdateCallback and CookCallback, since we use both.
class CheckinCallback(cook.CookCallback, updatecmd.UpdateCallback):
    def __init__(self, cfg=None):
        updatecmd.UpdateCallback.__init__(self, cfg)
        cook.CookCallback.__init__(self)

_commands = []
def _register(cmd):
    _commands.append(cmd)

(NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
(OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)
STRICT_OPT_PARAM        = options.STRICT_OPT_PARAM
(NORMAL_HELP, VERBOSE_HELP)  = (options.NORMAL_HELP, options.VERBOSE_HELP)

class CvcCommand(command.ConaryCommand):

    docs = { 'signature-key' : (VERBOSE_HELP,
                                "Use signature key to sign results", 'KEY'), }
    commandGroup = 'Information Display'
    def addConfigOptions(self, cfgMap, argDef):
        cfgMap['signature-key'] = 'signatureKey', ONE_PARAM
        command.ConaryCommand.addConfigOptions(self, cfgMap, argDef)

class AddCommand(CvcCommand):
    commands = ['add']
    paramHelp = '<file> [<file2> <file3> ...]'
    help = 'Add a file to be controlled by Conary'
    commandGroup = 'File Operations'

    docs = {'binary' : "Add files as binary - updates will not be merged on cvc up",
            'text' : "Add files as text - updates will be merged"}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["binary"] = NO_PARAM
        argDef["text"] = NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos=None):
        args = args[1:]
        text = argSet.pop('text', False)
        binary = argSet.pop('binary', False)
        if len(args) < 2: return self.usage()
        checkin.addFiles(args[1:], text = text, binary = binary, repos = repos,
                         defaultToText = False)
_register(AddCommand)

class AnnotateCommand(CvcCommand):
    commands = ['annotate']
    paramHelp = '<file>'
    help = 'Show version information for each line in a file'
    hidden = True
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or len(args) != 2: return self.usage()
        args[0] = repos
        checkin.annotate(*args)
_register(AnnotateCommand)



class BranchShadowCommand(CvcCommand):
    commands = ['shadow']
    paramHelp = "<newlabel> <trove>[=<version>][[flavor]]+"
    help = 'Create a shadow in a repository'

    commandGroup = 'Repository Access'
    docs = {'binary-only': 'Do not shadow/branch any source components listed',
            'source-only': ('For any binary components listed, shadow/branch'
                            ' their sources instead'),
            'info'       : 'Display info on shadow/branch'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["binary-only"] = NO_PARAM
        argDef["source-only"] = NO_PARAM
        argDef["info"] = '-i', NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
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
    paramHelp = '<trove>[=<version>]+'
    help = 'Check out a source component'
    commandGroup = 'Repository Access'

    docs = {'dir': 'Check out single trove in directory DIR'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["dir"] = ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet.has_key("dir"):
            dir = argSet['dir']
            del argSet['dir']
        else:
            dir = None
        if argSet or (len(args) < 2) or (dir and len(args) != 2):
            # no args other than --dir, and --dir implies only one trove
            return self.usage()
        coArgs = [repos, cfg, dir, args[1:], callback]
        checkin.checkout(*coArgs)
_register(CheckoutCommand)


class CloneCommand(CvcCommand):

    commands = ['clone']
    paramHelp = '<target-branch> <trove>[=<version>][[flavor]]+'
    help = 'Copy troves from one branch to another in a repository'
    commandGroup = 'Repository Access'
    hidden = True
    docs = { 'skip-build-info' : ('Do not attempt to rewrite version'
                                  'information about how this trove was built'),
             'info'            : 'Do not perform clone',
             'with-sources'    : ('Ensure that any binaries that are being'
                                 ' cloned also have a matching source component'),
             'message'         : ('Use MESSAGE for the changelog entry for'
                                  ' all cloned sources'),
             'full-recurse'    : ('Recursively clone packages included in'
                                  ' groups'),
             'test'            : ('Runs through all the steps of committing'
                                  ' but does not modify the repository')
           }

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["skip-build-info"] = NO_PARAM
        argDef["info"] = '-i', NO_PARAM
        argDef["with-sources"] = NO_PARAM
        argDef["message"] = '-m', ONE_PARAM
        argDef["full-recurse"] = NO_PARAM
        argDef["test"] = NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if len(args) < 3:
            return self.usage()

        from conary import clone
        skipBuildInfo = argSet.pop('skip-build-info', False)
        info = argSet.pop('info', False)
        message = argSet.pop("message", None)
        test = argSet.pop("test", False)
        cloneSources = argSet.pop('with-sources', False)
        fullRecurse = argSet.pop('full-recurse', False)
        if argSet: return self.usage()
        clone.CloneTrove(cfg, args[1], args[2:], not skipBuildInfo, info = info,
                         cloneSources=cloneSources, message = message, 
                         test = test, fullRecurse = fullRecurse)
_register(CloneCommand)



class CommitCommand(CvcCommand):

    commands = ['commit', 'ci']
    help = 'Commit changes to a source component'
    commandGroup = 'Repository Access'
    docs = {'message':'Use MESSAGE to describe why the commit was performed',
            'test':   ('Runs through all the steps of committing but does not '
                       'modify the repository'),
            'logfile': 'Read the commit message from file LOGFILE (use - for '
                       'standard input)',
    }
    # allow "cvc commit -m'foo bar'" to work
    hobbleShortOpts = False
    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["message"] = '-m', ONE_PARAM
        argDef["test"] = NO_PARAM
        argDef["logfile"] = '-l', ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        level = log.getVerbosity()
        message = argSet.pop("message", None)
        test = argSet.pop("test", False)
        logfile = argSet.pop("logfile", None)
        sourceCheck = True

        if argSet or len(args) != 1: return self.usage()

        if message and logfile:
            raise errors.ConaryError("options --message and --logfile are "
                "mutually exclusive")
        if logfile:
            # Read the checkin message from the file
            if logfile == '-':
                message = sys.stdin.read()
            else:
                try:
                    message = open(logfile).read()
                except IOError, e:
                    raise errors.ConaryError("While opening %s: %s" % (
                        e.filename, e.strerror))
            # Get rid of trailing white spaces, they're probably not 
            # intended to be there anyway
            message = message.rstrip()

        checkin.commit(repos, cfg, message, callback=callback, test=test)
        log.setVerbosity(level)
_register(CommitCommand)

class ContextCommand(CvcCommand):
    commands = ['context']
    paramHelp = '[CONTEXT]'
    help = 'Set up a context in the current directory'
    commandGroup = 'Setup Commands'

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["ask"] = NO_PARAM
        argDef["show-passwords"] = NO_PARAM

    docs = {'ask' : 'If not defined, create CONTEXT by answering questions',
            'show-passwords' : 'do not mask passwords'}

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if len(args) > 2:
            return self.usage()

        showPasswords = argSet.pop('show-passwords', False)
        ask = argSet.pop('ask', False)
        if len(args) > 1:
            name = args[1]
        else:
            name = None

        try:
            prettyPrint = sys.stdout.isatty()
        except AttributeError:
            prettyPrint = False
        cfg.setDisplayOptions(hidePasswords=not showPasswords,
                              prettyPrint=prettyPrint)
        checkin.setContext(cfg, name, ask=ask, repos=repos)
_register(ContextCommand)


class CookCommand(CvcCommand):
    commands = ['cook']
    paramHelp = '<file.recipe|troveName=<version>>[[flavor]]+'
    help = 'Build binary package and groups from a recipe'
    commandGroup = 'Recipe Building'

    docs = {'cross'   : (VERBOSE_HELP, 'set macros for cross-compiling', 
                         '[(local|HOST)--]TARGET'),
            'debug-exceptions' : 'Enter debugger if a recipe fails in conary',
            'flavor'  : 'build the trove with flavor FLAVOR',
            'macro'   : ('set macro NAME to VALUE', "'NAME VALUE'"),
            'macros'  : optparse.SUPPRESS_HELP, # can we get rid of this?
            'no-clean': 'do not remove build directory even if build is'
                        ' successful',
            'no-deps': optparse.SUPPRESS_HELP,
            'ignore-buildreqs' : 'do not check build requirements',
            'show-buildreqs': (VERBOSE_HELP,'show build requirements for recipe'),
            'prep'    : 'unpack, but do not build',
            'download': 'download, but do not unpack or build',
            'resume'  : ('resume building at given loc (default at failure)', 
                         '[LINENO|policy]'),
            'unknown-flags' : (VERBOSE_HELP, 
                    'Set all unknown flags that are used in the recipe to False')
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
        argDef['ignore-buildreqs'] = NO_PARAM
        argDef['show-buildreqs' ] = NO_PARAM
        argDef['prep'] = NO_PARAM
        argDef['download'] = NO_PARAM
        argDef['resume'] = STRICT_OPT_PARAM
        argDef['unknown-flags'] = NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        level = log.getVerbosity()
        macros = {}
        prep = 0
        downloadOnly = False
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
        if argSet.has_key('ignore-buildreqs'):
            del argSet['ignore-buildreqs']
            ignoreDeps = True
        elif argSet.has_key('no-deps'):
            del argSet['no-deps']
            ignoreDeps = True
        else:
            ignoreDeps = False

        if argSet.has_key('download'):
            if argSet.has_key('prep') or prep==True:
                log.warn('download and prep should not be used together... prefering download only')
            del argSet['download']
            ignoreDeps = True
            downloadOnly = True

        showBuildReqs = argSet.pop('show-buildreqs', False)

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
                         crossCompile=crossCompile, downloadOnly=downloadOnly)
        log.setVerbosity(level)
_register(CookCommand)

class DescribeCommand(CvcCommand):
    commands = ['describe']
    paramHelp = '<xml file>'
    help = 'Add metadata to a repository from an XML file'
    commandGroup = 'Repository Access'
    hidden = True
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        level = log.getVerbosity()
        if level > log.INFO:
            log.setVerbosity(log.INFO)

        xmlSource = args[1]
        conaryState = state.ConaryStateFromFile("CONARY", repos).getSourceState()
        troveName = conaryState.getName()
        troveBranch = conaryState.getVersion().branch()

        log.info("describing trove %s with %s", troveName, xmlSource)
        xmlFile = open(xmlSource)
        xml = xmlFile.read()

        repos.updateMetadataFromXML(troveName, troveBranch, xml)
        log.setVerbosity(level)
_register(DescribeCommand)



class DiffCommand(CvcCommand):
    commands = ['diff']
    help = 'Show uncommitted changes'
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) > 2: return self.usage()

        args[0] = repos
        checkin.diff(*args)
_register(DiffCommand)

class LogCommand(CvcCommand):
    commands = ['log']
    help = 'Show changelog entries for this source component'
    hidden = True

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or len(args) > 2: return self.usage()

        args[0] = repos
        checkin.showLog(*args)
_register(LogCommand)

class RdiffCommand(CvcCommand):
    commands = ['rdiff']
    paramHelp = "<name> [<oldver>|-<num>] <newver>"
    help = 'Show changes between two versions of a trove in a repository'
    hidden = True
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or len(args) != 4: return self.usage()
        checkin.rdiff(repos, cfg.buildLabel,  *args[1:])
_register(RdiffCommand)

class RefreshCommand(CvcCommand):
    commands = ['refresh']
    paramHelp = '<fileGlob> [<fileGlob2> <fileGlob3> ...]'
    help = 'Refresh files that are automatically downloaded'
    commandGroup = 'File Operations'
    hidden=True

    def runCommand(self, cfg, argSet, args, profile = False,
                   callback = None, repos = None):
        args = args[1:]
        #if len(args) < 2: return self.usage()
        checkin.refresh(repos, cfg, args[1:])
_register(RefreshCommand)

class RemoveCommand(CvcCommand):
    commands = ['remove', 'rm']
    paramHelp = "<file> [<file2> <file3> ...]"
    help = 'Remove a file from Conary control'
    commandGroup = 'File Operations'

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if len(args) < 2: return self.usage()
        for f in args[1:]:
            checkin.removeFile(f, repos=repos)
_register(RemoveCommand)

class RenameCommand(CvcCommand):
    commands = ['rename']
    paramHelp = "<oldfile> <newfile>"
    help = 'Rename a file that is under Conary control'
    commandGroup = 'File Operations'
    hidden = True

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if len(args) != 3: return self.usage()
        checkin.renameFile(args[1], args[2], repos=repos)
_register(RenameCommand)

class SignCommand(CvcCommand):
    commands = ['sign']
    paramHelp = "<newshadow> <trove>[=<version>][[flavor]]"
    help = 'Add a digital signature to troves in a repository'
    docs = {'recurse' : 'recursively sign child troves'}
    commandGroup = 'Repository Access'
    hidden = True

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['recurse'] = NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False,
                   callback = None, repos = None):
        args = args[1:]
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
    help = 'Set up the directory for creating a new package'
    commandGroup = 'Setup Commands'
    docs = {'dir' : 'create new package in DIR',
            'template' : 'set recipe template to use'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['dir'] = ONE_PARAM
        argDef['template'] = ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False,
                   callback = None, repos = None):
        args = args[1:]
        dir = argSet.pop('dir', None)
        template = argSet.pop('template', None)

        if len(args) != 2 or argSet: return self.usage()

        checkin.newTrove(repos, cfg, args[1], dir = dir, template = template)
_register(NewPkgCommand)

class MergeCommand(CvcCommand):
    commands = ['merge']
    help = 'Merge changes made in a parent branch into the current directory'
    commandGroup = 'File Operations'
    hidden = True
    def runCommand(self, cfg, argSet, args, profile = False,
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) > 2: return self.usage()
        if len(args) == 2:
            kw = dict(versionSpec=args[1])
        else:
            kw = {}
        checkin.merge(repos, **kw)
_register(MergeCommand)

class SetCommand(CvcCommand):

    commands = ['set']
    paramHelp = "<path>+"
    help = 'Set the properties of a file under Conary control'
    commandGroup = 'File Operations'
    hidden=True
    docs = {'text'       : ('Mark the given files as text files'),
            'binary'     : ('Mark the given files as binary files') }

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["binary"] = NO_PARAM
        argDef["text"] = NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        binary = argSet.pop('binary', False)
        text = argSet.pop('text', False)

        if binary and text:
            log.error("files cannot be both binary and text")
            return 1

        if argSet: return self.usage()
        if len(args) < 2: return self.usage()

        checkin.setFileFlags(repos, args[1:], text = text, binary = binary)

_register(SetCommand)

class StatCommand(CvcCommand):
    
    commands = ['status', 'stat', 'st']
    help = 'Show changed files in the working directory'
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) > 2: return self.usage()

        args[0] = repos
        checkin.stat_(*args)
_register(StatCommand)

class UpdateCommand(CvcCommand):
    commands = ['update', 'up']
    paramHelp = "[<version>]"
    help = 'Update files in current directory to a different version'
    commandGroup = 'File Operations'

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) > 2: return self.usage()

        args[0] = repos
        kwargs = {'callback': callback}
        checkin.updateSrc(*args, **kwargs)
_register(UpdateCommand)


class CvcMain(command.MainHandler):
    name = 'cvc'
    abstractCommand = CvcCommand
    configClass = conarycfg.ConaryConfiguration

    version = constants.version
    commandList = _commands
    hobbleShortOpts = True

    def usage(self, rc=1, showAll=False):
        print 'Conary Version Control (cvc)'
        if not showAll:
            print
            print 'Common Commands (use "cvc help" for the full list)'
        return options.MainHandler.usage(self, rc, showAll=showAll)

    def runCommand(self, thisCommand, cfg, argSet, args, debugAll=False):
        client = conaryclient.ConaryClient(cfg)
        repos = client.getRepos()
        callback = CheckinCallback(cfg)

        if not cfg.buildLabel and cfg.installLabelPath:
            cfg.buildLabel = cfg.installLabelPath[0]

        sys.excepthook = util.genExcepthook(debug=cfg.debugExceptions,
                                            debugCtrlC=debugAll)

        if cfg.installLabelPath:
            cfg.installLabel = cfg.installLabelPath[0]

        cfg.initializeFlavors()
        log.setMinVerbosity(log.INFO)

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

        rv = options.MainHandler.runCommand(self, thisCommand,
                                            cfg, argSet, args,
                                            callback=callback,
                                            repos=client.getRepos())

        if profile:
            prof.stop()
        if log.errorOccurred():
            sys.exit(1)
        return rv


def sourceCommand(cfg, args, argSet, profile=False, callback = None,
                  thisCommand = None):
    if thisCommand is None:
        thisCommand = CvcMain()._supportedCommands[args[0]]
    if not callback:
        callback = CheckinCallback(cfg)

    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()
    return thisCommand.runCommand(cfg, argSet, [ 'cvc' ] + list(args),
                                  profile=profile,
                                  callback=callback,
                                  repos=repos)

def main(argv=sys.argv):
    try:
        argv = list(argv)
        debugAll = '--debug-all' in argv
        if debugAll:
            debuggerException = Exception
            argv.remove('--debug-all')
        else:
            debuggerException = errors.InternalConaryError

        cvcMain = CvcMain()
        ccfg = cvcMain.getConfigFile(argv)
        if debugAll:
            ccfg.debugExceptions = True
            ccfg.debugRecipeExceptions = True

        # reset the excepthook (using cfg values for exception settings)
        sys.excepthook = util.genExcepthook(debug=ccfg.debugExceptions,
                                            debugCtrlC=debugAll)
        return cvcMain.main(argv, debuggerException, debugAll=debugAll,
                            cfg=ccfg)
    except debuggerException, err:
        raise
    except (errors.ConaryError, errors.CvcError, cfg.CfgError,
            openpgpfile.PGPError), e:
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

