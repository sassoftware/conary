# -*- mode: python -*-
#
# Copyright (c) 2004-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
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
from conary import errors, keymgmt
from conary import state
from conary import updatecmd
from conary import versions
from conary.build import cook, use, signtrove, derive, explain
from conary.build import errors as builderrors
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

    def missingFiles(self, missingFiles):
        print "Warning: The following files are missing:"
        for mp in missingFiles:
            print mp[4]
        return True

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

class ExplainCommand(CvcCommand):
    commands = ['explain']
    paramHelp = 'method'
    help = 'Display Conary recipe documentation'

    def runCommand(self, cfg, argSet, args, profile = False,
                   callback = None, repos=None):
        if len(args) < 3:
            return explain.docAll(cfg)
        return explain.docObject(cfg, args[2])
_register(ExplainCommand)

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
             'to-file'   : (VERBOSE_HELP, 'Write changeset to file instead of'
                                          ' committing to the repository'),
            'info'       : 'Display info on shadow/branch'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["binary-only"] = NO_PARAM
        argDef["source-only"] = NO_PARAM
        argDef["info"] = '-i', NO_PARAM
        argDef["to-file"] = ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        makeShadow =  (args[0] == "shadow")
        sourceOnly = argSet.pop('source-only', False)
        binaryOnly = argSet.pop('binary-only', False)
        targetFile = argSet.pop("to-file", None)
        info = argSet.pop('info', False)

        if argSet: return self.usage()
        if len(args) < 3: return self.usage()

        target = args[1]
        troveSpecs = args[2:]

        branch.branch(repos, cfg, target, troveSpecs, makeShadow = makeShadow, 
                      sourceOnly = sourceOnly, binaryOnly = binaryOnly,
                      info = info, targetFile = targetFile)
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
                                  ' information about how this trove was built'),
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

class PromoteCommand(CvcCommand):

    commands = ['promote']
    paramHelp = '<trove>[=<version>][[flavor]]+ <label>--<label>+'
    help = 'Copy troves from one label to another in a repository'
    commandGroup = 'Repository Access'
    hidden = True
    docs = { 'info'            : 'Do not perform promotion',

             'skip-build-info' : ('Do not attempt to rewrite version'
                                 ' information about how this trove was built'),
             'message'         : ('Use MESSAGE for the changelog entry for'
                                  ' all cloned sources'),
             'test'            : ('Runs through all the steps of committing'
                                  ' but does not modify the repository'),
             'without-sources'    : (VERBOSE_HELP,
                                     'Do not clone sources for the binaries'
                                     ' being cloned'),
             'default-only'    : (VERBOSE_HELP, 'EXPERIMENTAL - '
                                   ' Clones only those components'
                                   ' that are installed by default.'),
             'to-file'    : (VERBOSE_HELP, 'Write changeset to file instead of'
                                           ' committing to the repository'),
             'all-flavors' : (VERBOSE_HELP, 'Promote all flavors of a'
                                            ' package/group at the same time'
                                            ' (now the default)'),
             'exact-flavors' : (VERBOSE_HELP, 'Specified flavors must match'
                                              'the package/group flavors'
                                              'exactly to promote')
           }

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["skip-build-info"] = NO_PARAM
        argDef["info"] = '-i', NO_PARAM
        argDef["message"] = '-m', ONE_PARAM
        argDef["test"] = NO_PARAM
        argDef["all-flavors"] = NO_PARAM
        argDef["exact-flavors"] = NO_PARAM
        argDef["without-sources"] = NO_PARAM
        argDef["with-sources"] = NO_PARAM
        argDef["default-only"] = NO_PARAM
        argDef["to-file"] = ONE_PARAM
        argDef["exact-flavors"] = NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[2:]
        troveSpecs = []
        labelList = []
        for arg in args:
            if '--' in arg:
                labelList.append(arg.split('--', 1))
            else:
                troveSpecs.append(arg)
        if not labelList or not troveSpecs:
            return self.usage()

        from conary import clone
        skipBuildInfo = argSet.pop('skip-build-info', False)
        info = argSet.pop('info', False)
        message = argSet.pop("message", None)
        test = argSet.pop("test", False)
        allFlavors = argSet.pop("all-flavors", True)
        cloneSources = not argSet.pop("without-sources", False)
        argSet.pop("with-sources", False)
        targetFile = argSet.pop("to-file", False)
        defaultOnly = argSet.pop("default-only", False)
        exactFlavors = argSet.pop("exact-flavors", False)
        clone.promoteTroves(cfg, troveSpecs, labelList,
                            skipBuildInfo=skipBuildInfo,
                            info = info, message = message, test = test,
                            cloneSources=cloneSources, allFlavors=allFlavors,
                            cloneOnlyByDefaultTroves=defaultOnly,
                            targetFile=targetFile, exactFlavors=exactFlavors)
_register(PromoteCommand)


class CommitCommand(CvcCommand):

    commands = ['commit', 'ci']
    help = 'Commit changes to a source component'
    commandGroup = 'Repository Access'
    docs = {'message':'Use MESSAGE to describe why the commit was performed',
            'test':   ('Runs through all the steps of committing but does not '
                       'modify the repository'),
            'log-file':'Read the commit message from file LOGFILE (use - for '
                       'standard input)',
    }
    # allow "cvc commit -m'foo bar'" to work
    hobbleShortOpts = False
    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["message"] = '-m', ONE_PARAM
        argDef["test"] = NO_PARAM
        argDef["log-file"] = '-l', ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        level = log.getVerbosity()
        message = argSet.pop("message", None)
        test = argSet.pop("test", False)
        logfile = argSet.pop("log-file", None)
        sourceCheck = True

        if argSet or len(args) != 1: return self.usage()

        if message and logfile:
            raise errors.ConaryError("options --message and --log-file are "
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
        argDef['allow-flavor-change'] = NO_PARAM

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
            buildFlavor = deps.deps.parseFlavor(argSet['flavor'],
                                                raiseError=True)
            cfg.buildFlavor = deps.deps.overrideFlavor(cfg.buildFlavor,
                                                       buildFlavor)
            del argSet['flavor']

        if argSet.has_key('macros'):
            f = open(argSet['macros'])
            for row in f:
                row = row.strip()
                if not row or row[0] == '#':
                    continue
                cfg.configLine('macros ' + row.strip())
            f.close()
            del f
            del argSet['macros']

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

        allowFlavorChange = argSet.pop('allow-flavor-change', False)

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

        groupOptions = cook.GroupCookOptions(alwaysBumpCount=True,
                                 errorOnFlavorChange=not allowFlavorChange,
                                 shortenFlavors=cfg.shortenGroupFlavors)

        try:
            cook.cookCommand(cfg, args[1:], prep, macros, resume=resume, 
                         allowUnknownFlags=unknownFlags, ignoreDeps=ignoreDeps,
                         showBuildReqs=showBuildReqs, profile=profile,
                         crossCompile=crossCompile, downloadOnly=downloadOnly,
                         groupOptions=groupOptions)
        except builderrors.GroupFlavorChangedError, err:
            err.args = (err.args[0] +
                        '\n(Add the --allow-flavor-change flag to override this error)\n',)
            raise
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

class DeriveCommand(CvcCommand):
    commands = ['derive']
    hidden = True
    paramHelp = "<trove>[=<version>][[flavor]]"
    help = 'Aggregation command to shadow, check out and alter a recipe'
    commandGroup = 'Repository Access'

    docs = {'dir' : 'Derive single trove and check out in directory DIR',
            'extract': 'extract parent trove into _ROOT_ subdir for editing',
            'target': 'target label which the derived package should be shadowed to (defaults to buildLabel)',
            'info': 'Display info on shadow'}

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef["dir"] = ONE_PARAM
        argDef["info"] = '-i', NO_PARAM
        argDef['extract'] = NO_PARAM
        argDef['target'] = ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False,
                   callback = None, repos = None):
        args = args[1:]
        checkoutDir = argSet.pop('dir', None)
        extract = argSet.pop('extract', False)
        targetLabel = argSet.pop('target', None)
        info = prep = False
        if argSet.has_key('info'):
            del argSet['info']
            info = True

        if argSet or len(args) != 2:
            return self.usage()

        troveSpec = args[1]

        # we already know there's exactly one troveSpec
        if extract and ':source' in troveSpec.split('=')[0]:
            # usage of --extract-dir requires specification of a binary trove
            return self.usage()
        if targetLabel:
            try:
                targetLabel = versions.Label(targetLabel)
            except:
                return self.usage()
        else:
            targetLabel = cfg.buildLabel

        callback = derive.DeriveCallback(cfg)
        derive.derive(repos, cfg, targetLabel, troveSpec,
                checkoutDir = checkoutDir, extract = extract,
                info = info, callback = callback)
_register(DeriveCommand)

class DiffCommand(CvcCommand):
    commands = ['diff']
    help = 'Show uncommitted changes'
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) > 2: return self.usage()

        args[0] = repos
        return checkin.diff(*args)
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
    commands = ['rename', 'mv']
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

class AddKeyCommand(CvcCommand):
    commands = ['addkey']
    paramHelp = '<user>'
    help = 'Adds a public key from stdin to a repository'
    commandGroup = 'Key Management'
    docs = {'server'       : 'Repository server to retrieve keys from' }
    hidden = True

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['server'] = ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        if len(args) == 3:
            user = args[2]
        elif len(args) == 2:
            user = None
        else:
            return self.usage()

        server = argSet.pop('server', None)
        keymgmt.addKey(cfg, server, user)
_register(AddKeyCommand)

class GetKeyCommand(CvcCommand):
    commands = ['getkey']
    paramHelp = 'fingerprint'
    help = 'Retrieves a specified public key from a repository'
    commandGroup = 'Key Management'
    docs = {'server'       : 'Repository server to retrieve keys from' }
    hidden = True

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['server'] = ONE_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        if len(args) != 3:
            return self.usage()
        else:
            fingerprint = args[2]

        server = argSet.pop('server', None)
        keymgmt.showKey(cfg, server, fingerprint)
_register(GetKeyCommand)

class ListKeysCommand(CvcCommand):
    commands = ['listkeys']
    paramHelp = '[user]'
    help = 'Lists the public key fingerprints for a specified user'
    commandGroup = 'Key Management'
    docs = {'fingerprints' : 'Display fingerprints of keys',
            'server'       : 'Repository server to retrieve keys from' }
    hidden = True

    def addParameters(self, argDef):
        CvcCommand.addParameters(self, argDef)
        argDef['server'] = ONE_PARAM
        argDef['fingerprints'] = NO_PARAM

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        if len(args) > 3:
            return self.usage()
        elif len(args) == 3:
            user = args[2]
        else:
            user = None

        server = argSet.pop('server', None)
        showFps = 'fingerprints' in argSet

        keymgmt.displayKeys(cfg, server, user, showFingerprints = showFps)
_register(ListKeysCommand)

class RevertCommand(CvcCommand):
    commands = ['revert']
    help = 'Revert local changes to one or more files'
    commandGroup = 'File Operations'
    paramHelp = "[<file> <file2> <file3> ...]"
    def runCommand(self, cfg, argSet, args, profile = False,
                   callback = None, repos = None):
        if argSet: return self.usage()

        checkin.revert(repos, args[2:])
_register(RevertCommand)

_register(DiffCommand)
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
    help = 'Set a directory for creating a new package'
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
        checkin.merge(cfg, repos, **kw)
_register(MergeCommand)

class MarkRemovedCommand(CvcCommand):
    commands = [ 'markremoved' ]
    commandGroup = 'Hidden Commands'
    hidden = True

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) != 2: return self.usage()
        checkin.markRemoved(cfg, repos, args[1])
_register(MarkRemovedCommand)

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
    
    commands = ['stat', 'st']
    help = 'Show changed files in the working directory'
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) > 2: return self.usage()

        args[0] = repos
        checkin.stat_(*args)
_register(StatCommand)

class StatCommand(CvcCommand):
    
    commands = ['status', 'stat', 'st']
    help = 'Show changed files in the working directory'
    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[1:]
        if argSet or not args or len(args) > 1: return self.usage()

        args[0] = repos
        checkin.stat_(*args)
_register(StatCommand)

class UpdateCommand(CvcCommand):
    commands = ['update', 'up']
    paramHelp = "[<dir>=<version>]*"
    help = 'Update files in one or more directories to a different version'
    commandGroup = 'File Operations'

    def runCommand(self, cfg, argSet, args, profile = False, 
                   callback = None, repos = None):
        args = args[2:]
        if argSet: return self.usage()

        kwargs = {'callback': callback}
        checkin.updateSrc(repos, versionList = args, **kwargs)
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
        else:
            cfg.installLabel = None

        cfg.initializeFlavors()
        log.setMinVerbosity(log.INFO)
        log.resetErrorOccurred()

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
        repos = conaryclient.ConaryClient(cfg).getRepos()
        keyCacheCallback = openpgpkey.KeyCacheCallback(repos,
                                                       cfg)
        keyCache.setCallback(keyCacheCallback)

        rv = options.MainHandler.runCommand(self, thisCommand,
                                            cfg, argSet, args,
                                            callback=callback,
                                            repos=client.getRepos())

        if profile:
            prof.stop()
        if log.errorOccurred():
            sys.exit(2)
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
    sys.stdout = util.FileIgnoreEpipe(sys.stdout)
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
            sys.exit(2)
        else:
            raise
    except KeyboardInterrupt, e:
        pass
    return 1


if __name__ == "__main__":
    sys.exit(main())

