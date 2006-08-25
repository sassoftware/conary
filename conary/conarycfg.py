#
# Copyright (c) 2004-2006 rPath, Inc.
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
Implements conaryrc handling.
"""
import fnmatch
import os
import sys
import xml
import re
import traceback

from conary.deps import deps, arch
from conary.lib import util
from conary.lib.cfg import *
from conary import errors
from conary import versions
from conary import flavorcfg

# ----------- conary specific types
    

class UserInformation(list):
    def find(self, server):
        for (serverGlob, user, password) in self:
            # this is case insensitve, which is perfect for hostnames
            if fnmatch.fnmatch(server, serverGlob):
                return user, password

        return None

    def extend(self, itemList):
        # Look for the first item which globs to this, and insert the new
        # item before it. That makes sure find always matches on the
        # most-specific instance
        for newItem in itemList:
            self.append(newItem)

    def append(self, newItem):
        location = None
        for i, (serverGlob, user, password) in enumerate(self):
            if fnmatch.fnmatch(newItem[0], serverGlob):
                location = i
                break

        if location is None:
            list.append(self, newItem)
        else:
            self.insert(location, newItem)

    def addServerGlob(self, serverGlob, user, password):
        self.append((serverGlob, user, password))

class CfgUserInfoItem(CfgType):
    def parseString(self, str):
        val = str.split()
        if len(val) < 2 or len(val) > 3:
            raise ParseError, ("%s:%s: expected <hostglob> <user> "
                               "<password> for configuration value %s"
                                            % (File, self.lineno, key))
        elif len(val) == 2:
            return (val[0], val[1], None)
        else:
            return tuple(val)

    def format(self, val, displayOptions=None):
        serverGlob, user, password = val
        if password is None: 
            return '%s %s' % (serverGlob, user)
        elif displayOptions.get('hidePasswords'):
            return '%s %s <password>' % (serverGlob, user)
        else:
            return '%s %s %s' % (serverGlob, user, password)

class CfgUserInfo(CfgList):

    def __init__(self, default=[]):
        CfgList.__init__(self, CfgUserInfoItem, UserInformation,
                         default = default)

class CfgLabel(CfgType):

    def format(self, val, displayOptions=None):
        return val.asString()

    def parseString(self, val):
        try:
            return versions.Label(val)
        except versions.ParseError, e:
            raise ParseError, e

class CfgRepoMapEntry(CfgType):

    def parseString(self, str):
        match = re.match('https?://([^:]*):[^@]*@([^/:]*)(?::.*)?/.*', str)
        if match is not None:
            user, server = match.groups()
            raise ParseError, ('repositoryMap entries should not contain '
                               'user names and passwords; use '
                               '"user %s %s <password>" instead' % 
                               (server, user))

        return CfgType.parseString(self, str)

    def format(self, val, displayOptions=None):
        if displayOptions.get('hidePasswords'):
            return re.sub('(https?://)[^:]*:[^@]*@(.*)', 
                          r'\1<user>:<password>@\2', val)
        else:
            return val

class RepoMap(dict):

    def getNoPass(self, key):
        return re.sub('(https?://)[^:]*:[^@]*@(.*)', r'\1\2', self[key])

class CfgRepoMap(CfgDict):
    def __init__(self, default={}):
        CfgDict.__init__(self, CfgRepoMapEntry, dictType=RepoMap,
                         default=default)

class CfgFlavor(CfgType):

    default = deps.Flavor()

    def copy(self, val):
        return val.copy()

    def parseString(self, val):
        try:
            f = deps.parseFlavor(val)
        except Exception, e:
            raise ParseError, e
        if f is None:
            raise ParseError, 'Invalid flavor %s' % val
        return f

    def format(self, val, displayOptions=None):
        val = ', '.join(deps.formatFlavor(val).split(','))

        if displayOptions and displayOptions.get('prettyPrint', False):
            val = ('\n%26s'%'').join(textwrap.wrap(val, 48))

        return val


class CfgFingerPrintMapItem(CfgType):
    def parseString(self, val):
        val = val.split(None, 1)
        label = val[0]
        try:
            # compile label to verify that it is valid
            re.compile(label)
        except Exception, e:
            raise ParseError, "Invalid regexp: '%s': " % label + str(e)

        if len(val) == 1 or not val[1] or val[1].lower() == 'none':
            fingerprint = None
        else:
            # remove all whitespace
            fingerprint = ''.join(val[1].split())
        return label, fingerprint

    def format(self, val, displayOptions=None):
        # val[1] may be None
        return ' '.join([val[0], str(val[1])])

class CfgFingerPrintMap(CfgList):
    def __init__(self, default={}):
        CfgList.__init__(self, CfgFingerPrintMapItem, default=default)


class CfgFingerPrint(CfgType):
    def parseString(self, val):
        val = val.replace(' ', '')
        if not val or val.lower() == 'none':
            return None
        return val

class CfgLabelList(list):

    def __repr__(self):
        return "CfgLabelList(%s)" % list.__repr__(self)

    def __getslice__(self, i, j):
        return CfgLabelList(list.__getslice__(self, i, j))

    def versionPriority(self, first, second):
        return self.priority(first.trailingLabel(), second.trailingLabel())

    def priority(self, first, second):
        # returns -1 if the first label occurs earlier in the list than
        # the second label does; None if either or both labels are missing
        # from the path. If the labels are identical and both are in the
        # path, we return 0 (I don't know how useful that is, but what the
        # heck)
        firstIdx = None
        secondIdx = None

        for i, l in enumerate(self):
            if firstIdx is None and l == first:
                firstIdx = i
            if secondIdx is None and l == second:
                secondIdx = i

        if firstIdx is None or secondIdx is None:
            return None 

        return cmp(firstIdx, secondIdx)

CfgInstallLabelPath = CfgLineList(CfgLabel, listType = CfgLabelList)

class ConaryContext(ConfigSection):
    """ Conary uses context to let the value of particular config parameters
        be set based on a keyword that can be set at the command line.
        Configuartion values that are set in a context are overridden 
        by the values in the context that have been set.  Values that are 
        unset in the context do not override the default config values.
    """

    buildFlavor           =  CfgFlavor
    buildLabel            =  CfgLabel
    buildPath             =  None
    contact               =  None
    environment           =  CfgDict(CfgString)
    excludeTroves         =  CfgRegExpList
    flavor                =  CfgList(CfgFlavor)
    lookaside             =  CfgPath
    installLabelPath      =  CfgInstallLabelPath
    name                  =  None
    recipeTemplate        =  None
    repositoryMap         =  CfgRepoMap
    root                  =  CfgPath
    signatureKey          =  CfgFingerPrint
    signatureKeyMap       =  CfgFingerPrintMap
    siteConfigPath        =  CfgPathList
    user                  =  CfgUserInfo

    def _resetSigMap(self):
        self.signatureKeyMap = []

    def __init__(self, *args, **kw):
        ConfigSection.__init__(self, *args, **kw)
        self.addListener('signatureKey', lambda *args: self._resetSigMap())

    def displayKey(self, cfgItem, value, out=None):
        if not value:
            return 
        cfgItem.write(out, value, self._displayOptions)

class ConaryConfiguration(SectionedConfigFile):
    archDirs              =  (CfgPathList, ('/etc/conary/arch',
                                            '/etc/conary/distro/arch',
                                            '~/.conary/arch'))
    autoResolve           =  (CfgBool, False)
    autoResolvePackages   =  (CfgBool, True)
    buildPath             =  (CfgPath, '~/conary/builds')
    cleanAfterCook        =  (CfgBool, True)
    context		  =  None
    dbPath                =  '/var/lib/conarydb'
    debugExceptions       =  (CfgBool, False)
    debugRecipeExceptions =  (CfgBool, False)
    defaultMacros         =  (CfgPathList, ('/etc/conary/macros',
                                            '~/.conary/macros'))
    emergeUser            =  (CfgString, 'emerge')
    enforceManagedPolicy  =  (CfgBool, True)
    entitlementDirectory  =  (CfgPath, '/etc/conary/entitlements')
    environment           =  CfgDict(CfgString)
    fullVersions          =  CfgBool
    fullFlavors           =  CfgBool
    localRollbacks        =  CfgBool
    interactive           =  (CfgBool, False)
    logFile               =  (CfgPathList, ('/var/log/conary',
                                            '~/.conary/log',))
    lookaside             =  (CfgPath, '~/conary/cache')
    macros                =  CfgDict(CfgString)
    mirrorDirs            =  (CfgPathList, ('~/.conary/mirrors',
                                            '/etc/conary/distro/mirrors',
                                            '/etc/conary/mirrors',))
    quiet		  =  CfgBool
    pinTroves		  =  CfgRegExpList
    policyDirs            =  (CfgPathList, ('/usr/lib/conary/policy',
                                            '/etc/conary/policy',
                                            '~/.conary/policy'))
    pubRing               =  (CfgPathList, [ \
        ('/etc/conary/pubring.gpg',
         '~/.gnupg/pubring.gpg')[int(bool(os.getuid()))]])
    uploadRateLimit       =  (CfgInt, 0)
    downloadRateLimit     =  (CfgInt, 0)
    root                  =  (CfgPath, '/')
    resolveLevel          =  (CfgInt, 2)
    recipeTemplateDirs    =  (CfgPathList, ('~/.conary/recipeTemplates',
                                            '/etc/conary/recipeTemplates'))
    showLabels            =  CfgBool
    showComponents        =  CfgBool
    siteConfigPath        =  (CfgPathList, ('/etc/conary/site',
                                            '/etc/conary/distro/site',
                                            '~/.conary/site'))
    sourceSearchDir       =  (CfgPath, '.')
    threaded              =  (CfgBool, True)
    tmpDir                =  (CfgPath, '/var/tmp')
    trustThreshold        =  (CfgInt, 0)
    updateThreshold       =  (CfgInt, 10)
    useDirs               =  (CfgPathList, ('/etc/conary/use',
                                            '/etc/conary/distro/use',
                                            '~/.conary/use'))


    # this allows a new section to be created on the fly with the type 
    # ConaryContext
    _allowNewSections     = True
    _defaultSectionType   =  ConaryContext

    def __init__(self, readConfigFiles = False):
	SectionedConfigFile.__init__(self)

        for info in ConaryContext._getConfigOptions():
            if info[0] not in self:
                self.addConfigOption(*info)

        self.addListener('signatureKey', lambda *args: self._resetSigMap())

	if readConfigFiles:
	    self.readFiles()
        util.settempdir(self.tmpDir)
  
    def readFiles(self):
	self.read("/etc/conaryrc", exception=False)
	if os.environ.has_key("HOME"):
	    self.read(os.environ["HOME"] + "/" + ".conaryrc", exception=False)
	self.read("conaryrc", exception=False)
  
    def setContext(self, name):
        """ Copy the config values from the context named name (if any)
            into the main config file.  Returns False if not such config
            file found.
        """
        if not self.hasSection(name):
            return False
        self.context = name
        context = self.getSection(name)

        for key, value in context.iteritems():
            if isinstance(value, deps.Flavor):
                    self.__dict__[key] = value
            elif value:
                if isinstance(value, dict):
                    self.__dict__[key].update(value)
                else:
                    self.__dict__[key] = value
        return True

    def getContext(self, name):
        if not self.hasSection(name):
            return False
        return self.getSection(name)

    def displayContext(self, out=None):
        if out is None:
            out = sys.stdout
        if self.context:
            out.write('[%s]\n' % self.context)
            context = self.getContext(self.context)
            context.setDisplayOptions(**self._displayOptions)
            context.display(out)
        else:
            out.write('No context set.\n')

    def _writeSection(self, name, options):
        return self.getDisplayOption('showContexts', False)

    def requireInstallLabelPath(self):
        if not self.installLabelPath:
            print >> sys.stderr, "installLabelPath is not set"
            sys.exit(1)

    def _resetSigMap(self):
        self.signatureKeyMap = []

    def initializeFlavors(self):
        self.flavorConfig = flavorcfg.FlavorConfig(self.useDirs, 
                                                   self.archDirs)
        if self.flavor == []:
            self.flavor = [deps.Flavor()]

        self.flavor = self.flavorConfig.toDependency(override=self.flavor)

        newFlavors = []
        hasIns = False
        
        # if any flavor has an instruction set, don't merge
        for flavor in self.flavor:
            if deps.DEP_CLASS_IS in flavor.getDepClasses():
                hasIns = True
                break

        if not hasIns:
            # use all the flavors for the main arch first
            for depList in arch.currentArch:
                for flavor in self.flavor:
                    insSet = deps.Flavor()
                    for dep in depList:
                        insSet.addDep(deps.InstructionSetDependency, dep)
                    newFlavor = flavor.copy()
                    newFlavor.union(insSet)
                    newFlavors.append(newFlavor)
            self.flavor = newFlavors

        # buildFlavor is installFlavor + overrides
        self.buildFlavor = deps.overrideFlavor(self.flavor[0], 
                                                    self.buildFlavor)
	self.flavorConfig.populateBuildFlags()

def selectSignatureKey(cfg, label):
    if not cfg.signatureKeyMap:
        return cfg.signatureKey
    label = str(label)
    if "local@local" in label:
        label = str(cfg.buildLabel)
    for sigLabel, fingerprint in cfg.signatureKeyMap:
        if re.match(sigLabel, label):
            return fingerprint
    return cfg.signatureKey

def emitEntitlement(serverName, className, key):

    # XXX This probably should be emitted using a real XML DOM writer,
    # but this will probably do for now. And yes, all that mess is required
    # to be well-formed and valid XML.
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>
<!DOCTYPE entitlement [
    <!ELEMENT entitlement (server, class, key)>
    <!ELEMENT server (#PCDATA)>
    <!ELEMENT class (#PCDATA)>
    <!ELEMENT key (#PCDATA)>
]>
<entitlement>
    <server>%s</server>
    <class>%s</class>
    <key>%s</key>
</entitlement>
""" % (serverName, className, key)

def loadEntitlementFromString(xmlContent, serverName, source='<override>'):
    p = EntitlementParser()

    # wrap this in an <entitlement> top level tag (making it optional
    # [but recommended!] in the entitlement itself)
    #
    # XXX This synthetic wrapping should probably be made obsolete; everyone
    # should use emitEntitlement, which does the right thing.
    try:
        if '<entitlement>' not in xmlContent:
            p.parse("<entitlement>" + xmlContent + "</entitlement>")
        else:
            p.parse(xmlContent)

        try:
            entServer = p['server']
            entClass = p['class']
            entKey = p['key']
        except KeyError:
            raise errors.ConaryError("Entitlement incomplete.  Entitlements"
                                     " must include 'server', 'class', and"
                                     " 'key' values")
    except Exception, err:
        raise errors.ConaryError("Malformed entitlement for %s at %s:"
                                 " %s" % (serverName, source, err))

    if entServer != serverName: 
        raise errors.ConaryError("Entitlement at %s is for server '%s', "
                         "should be for '%s'" % (source, entServer, serverName))

    return (entClass, entKey)

def loadEntitlementFromProgram(fullPath, serverName):
    """ Executes the given file to generate an entitlement.
        The executable must print to stdout a full valid entitlement xml
        blob.
    """
    readFd, writeFd = os.pipe()
    stdErrRead, stdErrWrite = os.pipe()
    childPid = os.fork()
    if not childPid:
        try:
            try:
                os.close(readFd)
                os.close(sys.stdin.fileno())

                # both error and stderr are redirected  - the entitlement
                # should be on stdout, and error info should be 
                # on stderr.
                os.dup2(writeFd, sys.stdout.fileno())
                os.dup2(stdErrWrite, sys.stderr.fileno())
                os.close(writeFd)
                os.close(stdErrWrite)
                os.execl(fullPath, fullPath, serverName)
            except Exception, err:
                traceback.print_exc(sys.stderr)
        finally:
            os._exit(1)
    os.close(writeFd)
    os.close(stdErrWrite)

    # read in from pipes.  When they're closed,
    # the child process should have exited.
    output = []
    errorOutput = []
    buf = os.read(readFd, 1024)
    errBuf = os.read(stdErrRead, 1024)

    while buf or errBuf:
        if buf:
            output.append(buf)
            buf = os.read(readFd, 1024)
        if errBuf:
            errorOutput.append(errBuf)
            errBuf = os.read(stdErrRead, 1024)

    pid, status = os.waitpid(childPid, 0)

    errMsg = ''
    if os.WIFEXITED(status) and os.WEXITSTATUS(status):
        errMsg = ('Entitlement generator at "%s"'
                  ' died with exit status %d' % (fullPath,
                                                 os.WEXITSTATUS(status)))
    elif os.WIFSIGNALED(status):
        errMsg = ('Entitlement generator at "%s"'
                  ' died with signal %d' % (fullPath, os.WTERMSIG(status)))
    else:
        errMsg = ''

    if errMsg:
        if errorOutput:
            errMsg += ' - stderr output follows:\n%s' % ''.join(errorOutput)
        else:
            errMsg += ' - no output on stderr'
        raise errors.ConaryError(errMsg)

    # looks like we generated an entitlement - they're still the possibility
    # that the entitlement is broken.
    xmlContent = ''.join(output)
    return loadEntitlementFromString(xmlContent, serverName, fullPath)


def loadEntitlement(dirName, serverName):
    # XXX this should be replaced with a real xml parser

    if not dirName:
        # XXX
        # this is a hack for the repository server which doesn't support
        # entitlements, but needs to stop cross talking anyway
        return None

    fullPath = os.path.join(dirName, serverName)

    p = EntitlementParser()
    if not os.access(fullPath, os.R_OK):
        return None

    if os.access(fullPath, os.X_OK):
        return loadEntitlementFromProgram(fullPath, serverName)
    elif os.access(fullPath, os.R_OK):
        return loadEntitlementFromString(open(fullPath).read(), serverName,
                                         fullPath)
    else:
        return None

class EntitlementParser(dict):

    def StartElementHandler(self, name, attrs):
        if name not in [ 'entitlement', 'server', 'class', 'key' ]:
            raise SyntaxError
        self.state.append(str(name))
        self.data = None

    def EndElementHandler(self, name):
        state = self.state.pop()
        # str() converts from unicode
        self[state] = str(self.data)

    def CharacterDataHandler(self, data):
        self.data = data

    def parse(self, s):
        self.state = []
        return self.p.Parse(s)

    def __init__(self):
        self.p = xml.parsers.expat.ParserCreate()
        self.p.StartElementHandler = self.StartElementHandler
        self.p.EndElementHandler = self.EndElementHandler
        self.p.CharacterDataHandler = self.CharacterDataHandler
        dict.__init__(self)

