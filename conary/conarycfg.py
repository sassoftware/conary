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
Implements conaryrc handling.
"""
import fnmatch
import os
import sys
import copy

from conary.build import use
from conary.deps import deps, arch
from conary.lib import log, util
from conary.lib.cfg import *
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
            return '%s %s <password>' % (serverGlob, user, password)

class CfgUserInfo(CfgList):
    def __init__(self, default=[]):
        CfgList.__init__(self, CfgUserInfoItem, UserInformation)

class CfgLabel(CfgType):

    def format(self, val, displayOptions=None):
        return val.asString()

    def parseString(self, val):
        try:
            return versions.Label(val)
        except versions.ParseError, e:
            raise ParseError, e

class CfgRepoMapEntry(CfgType):
        
    def format(self, val, displayOptions=None):
        if displayOptions.get('hidePasswords'):
            return re.sub('(https?://)[^:]*:[^@]*@(.*)', 
                         r'\1<user>:<password>@\2', val)
        else:
            return val

class CfgRepoMap(CfgDict):
    def __init__(self, default={}):
        CfgDict.__init__(self, CfgRepoMapEntry, default=default)

class CfgFlavor(CfgType):

    default = deps.DependencySet()

    def copy(self, val):
        return val.copy()

    def parseString(self, val):
        try:
            return deps.parseFlavor(val)
        except Exception, e:
            raise ParseError, e

    def format(self, val, displayOptions=None):
        val = ', '.join(deps.formatFlavor(val).split(','))

        if displayOptions.get('prettyPrint', False):
            val = ('\n%26s'%'').join(textwrap.wrap(val, 48))

        return val


class CfgFingerPrintMapItem(CfgType):
    
    def parseString(self, val):
        val = val.split(None, 1)
        label = val[0]

        if len(val) == 1 or not val[1] or val[1].lower() == 'none':
            fingerprint = None
        else:
            # remove all whitespace
            fingerprint = ''.join(val[1].split())
        return label, fingerprint

    def format(self, val, displayOptions=None):
        return ' '.join(label, fingerprint)

class CfgFingerPrintMap(CfgList):
    def __init__(self, default={}):
        CfgList.__init__(self, CfgFingerPrintMapItem, default=default)


class CfgFingerPrint(CfgType):
    def parseString(self, val):
        val = val.replace(' ', '')
        if not val or val.lower() == 'none':
            return None
        return val
            
    
CfgInstallLabelPath = CfgLineList(CfgLabel, ' ')
    

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
    excludeTroves         =  CfgRegExpList
    flavor                =  CfgList(CfgFlavor)
    installLabelPath      =  CfgInstallLabelPath
    name                  =  None
    repositoryMap         =  CfgRepoMap
    signatureKey          =  CfgFingerPrint
    signatureKeyMap       =  CfgFingerPrintMap
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
    buildPath             =  '/var/tmp/conary/builds'
    context		  =  None
    dbPath                =  '/var/lib/conarydb'
    debugExceptions       =  (CfgBool, True)
    debugRecipeExceptions =  CfgBool
    fullVersions          =  CfgBool
    fullFlavors           =  CfgBool 
    localRollbacks        =  CfgBool
    interactive           =  (CfgBool, False)
    logFile               =  (CfgPath, '/var/log/conary')
    lookaside             =  (CfgPath, '/var/cache/conary')
    macros                =  CfgDict(CfgString)
    quiet		  =  CfgBool
    pinTroves		  =  CfgRegExpList
    pubRing               =  (CfgPathList, '/etc/conary/pubring.gpg')
    root                  =  (CfgPath, '/')
    sourceSearchDir       =  (CfgPath, '.')
    threaded              =  (CfgBool, True)
    tmpDir                =  (CfgPath, '/var/tmp')
    trustThreshold        =  (CfgInt, 0)
    updateThreshold       =  (CfgInt, 10)
    useDirs               =  (CfgPathList, ('/etc/conary/use',
                                            '/etc/conary/distro/use',
                                            '~/.conary/use'))

    _sectionType          =  ConaryContext

    def __init__(self, readConfigFiles=True):
	SectionedConfigFile.__init__(self)

        for info in ConaryContext._getConfigOptions():
            self.addConfigOption(*info)

        self.addListener('signatureKey', lambda *args: self._resetSigMap())

	if readConfigFiles:
	    self.readFiles()
            self.entitlements = loadEntitlements('/etc/conary/entitlements')
        util.settempdir(self.tmpDir)
  
    def readFiles(self):
	self.read("/etc/conaryrc")
	if os.environ.has_key("HOME"):
	    self.read(os.environ["HOME"] + "/" + ".conaryrc")
	self.read("conaryrc")
  
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
            if value:
                if isinstance(value, dict):
                    self.__dict__[key].update(value)
                else:
                    self.__dict__[key] = value
        return True

    def getContext(self, name):
        return self.getSection(name)

    def displayContext(self, out=None):
        if out is None:
            out = sys.stdout
        out.write('[%s]\n' % self.context)
        self.getContext(self.context).display(out)

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
            self.flavor = [deps.DependencySet()]

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
                    insSet = deps.DependencySet()
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
    for sigLabel, fingerprint in cfg.signatureKeyMap:
        if re.match(sigLabel, label):
            return fingerprint
    return cfg.signatureKey

def _loadSingleEntitlement(f):
    # XXX this should be replaced with a real xml parser
    contents = "".join([ x[:-1] for x in f.readlines()])
    key = None
    keyGroup = None

    tokens = []

    while contents:
        if contents[0] == '<':
            i = contents.find('>')
            tag = contents[1:i]
            tag.strip()
            contents = contents[i + 1:]
            tokens.append(tag)
        else:
            i = contents.find('<')
            if i == -1:
                # okay by xml, not by us
                raise SyntaxError
            else:
                tokens.append(contents[:i])
                contents = contents[i:]

    d = {}
    while tokens:
        openTag = tokens.pop(0)
        contents = tokens.pop(0)
        closeTag = tokens.pop(0)

        if closeTag != '/' and closeTag[1:] != openTag:
            raise SyntaxError

        d[openTag] = contents

    if not 'class' in d or not 'key' in d: 
        raise SyntaxError

    entServer = d.pop('server')
    entClass = d.pop('class')
    endKey = d.pop('key')

    if d: raise SyntaxError

    return (entServer, entClass, endKey)

def loadEntitlements(dirname):
    if not os.path.isdir(dirname):
        # that's okay
        return {}

    paths = os.listdir(dirname)

    entList= []

    for path in paths:
        if path[0] == '.': continue

        fullPath = os.path.join(dirname, path)
        entList.append(_loadSingleEntitlement(open(fullPath, "r")))

    return dict((x[0], (x[1], x[2])) for x in entList)
