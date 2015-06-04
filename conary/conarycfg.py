#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Implements conaryrc handling.
"""
import fnmatch
import os
import sys
import xml
import re
import tempfile
import textwrap
import traceback
import urllib

from conary.deps import deps, arch
from conary.lib import util, api
from conary.lib.cfg import ParseError, SectionedConfigFile, ConfigSection
from conary.lib.cfgtypes import (CfgType, CfgString, CfgBool, CfgPath, CfgEnum,
        CfgList, CfgDict, CfgLineList, CfgPathList, CfgRegExpList, CfgInt,
        CfgBytes)
from conary.lib.http import proxy_map
from conary import errors
from conary import versions
from conary import flavorcfg
from conary.repository import transport

# ----------- conary specific types

class ServerGlobList(object):

    multipleMatches = False

    def __init__(self, items=()):
        self._exact = {}
        self._globs = []
        self._caseMap = {}
        self.extend(items)

    def find(self, server):
        server = server.lower()
        l = list(self._exact.get(server, ()))
        if l and not self.multipleMatches:
            assert len(l) == 1
            return l[0]
        for (serverGlob, item) in self._globs:
            if fnmatch.fnmatchcase(server, serverGlob):
                if not self.multipleMatches:
                    return item
                l.append(item)

        if not self.multipleMatches:
            return None

        return l

    def append(self, newItem):
        newGlob, newInfo = newItem
        newGlobLower = newGlob.lower()
        self._caseMap[newGlobLower] = newGlob
        newGlob = newGlobLower
        if not any(x in newGlob for x in '*?['):
            # Exact matches go in a hash for quick add and lookup
            if self.multipleMatches and newGlob in self._exact:
                self._exact[newGlob].append(newInfo)
            else:
                self._exact[newGlob] = [newInfo]
        else:
            # Globs are put in a list that is scanned after exact matches
            location = None
            removeOld = False
            for i, (serverGlob, info) in enumerate(self._globs):
                if fnmatch.fnmatch(newItem[0], serverGlob):
                    if not self.multipleMatches and serverGlob == newItem[0]:
                        removeOld = True
                    location = i
                    break

            if location is None:
                self._globs.append(newItem)
            elif removeOld:
                self._globs[location] = newItem
            else:
                self._globs.insert(location, newItem)

    def extend(self, itemList):
        # Look for the first item which globs to this, and insert the new
        # item before it. That makes sure find always matches on the
        # most-specific instance
        for newItem in reversed(list(itemList)):
            self.append(newItem)

    def clear(self):
        self._exact.clear()
        self._caseMap.clear()
        del self._globs[:]

    def __iter__(self):
        for serverGlob, infoList in sorted(self._exact.iteritems()):
            for info in infoList:
                yield self._caseMap[serverGlob], info
        for serverGlob, info in self._globs:
            yield self._caseMap[serverGlob], info

    def __nonzero__(self):
        return bool(self._exact) or bool(self._globs)

    def __eq__(self, other):
        return list(self) == list(other)

    def __ne__(self, other):
        return list(self) != list(other)


class UserInformation(ServerGlobList):

    def __iter__(self):
        for x in ServerGlobList.__iter__(self):
            yield (x[0], x[1][0], x[1][1])

    def addServerGlob(self, *args):
        # handle (glob, name, passwd) and transform to (glob, (name, passwd))a
        if len(args) == 3:
            args = args[0], (args[1], args[2])
        ServerGlobList.append(self, args)

    def addServerGlobs(self, items):
        for item in reversed(list(items)):
            self.addServerGlob(*item)

    def extend(self, other):
        for item in other:
            self.addServerGlob(*item)

    def append(self, item):
        self.addServerGlob(*item)

    def __init__(self, initVal = None):
        ServerGlobList.__init__(self)
        if initVal is not None:
            for val in initVal:
                self.addServerGlob(*val)

class CfgUserInfoItem(CfgType):
    def parseString(self, str):
        val = str.split()
        if len(val) < 2 or len(val) > 3:
            raise ParseError("expected <hostglob> <user> [<password>]")
        elif len(val) == 2:
            return (val[0], val[1], None)
        else:
            pw = (val[2] is not None and util.ProtectedString(val[2])) or None
            return (val[0], val[1], pw)

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

    def set(self, curVal, newVal):
        curVal.extend(newVal)
        return curVal

class EntitlementList(ServerGlobList):

    multipleMatches = True

    def addEntitlement(self, serverGlob, entitlement, entClass = None):
        self.append((serverGlob, (entClass, util.ProtectedString(entitlement))))

class CfgEntitlementItem(CfgType):
    def parseString(self, str):
        val = str.split()
        if len(val) == 3:
            # Output from an entitlement file, which still has a class
            import warnings
            warnings.warn("\nExpected an entitlement line with no entitlement "
                "class.\nEntitlement classes will be ignored in the future.\n"
                "Please change the 'entitlement %s' config line to\n"
                "'entitlement %s %s'" % (str, val[0], val[2]),
                DeprecationWarning)
            return (val[0], (val[1], util.ProtectedString(val[2])))
        elif len(val) != 2:
            raise ParseError("expected <hostglob> <entitlement>")

        return (val[0], (None, util.ProtectedString(val[1])))

    def format(self, val, displayOptions=None):
        if val[1][0] is None:
            return '%s %s' % (val[0], val[1][1])
        else:
            return '%s %s %s' % (val[0], val[1][0], val[1][1])

class CfgEntitlement(CfgList):

    def __init__(self, default=[]):
        CfgList.__init__(self, CfgEntitlementItem, EntitlementList,
                         default = default)

    def set(self, curVal, newVal):
        curVal.extend(newVal)
        return curVal

class CfgLabel(CfgType):

    def format(self, val, displayOptions=None):
        return val.asString()

    def parseString(self, val):
        try:
            return versions.Label(val)
        except versions.ParseError, e:
            raise ParseError, e

class CfgDependencyClass(CfgType):

    def format(self, val, displayOptions=None):
        return val.tagName

    def parseString(self, val):
        klass = deps.dependencyClassesByName.get(val, None)
        if klass is None:
            raise ParseError('unknown dependency class: %s' % val)

        return klass

class CfgRepoMapEntry(CfgType):

    def parseString(self, str):
        val = str.split()
        if len(val) != 2:
            raise ParseError("expected <hostglob> <url>")

        match = re.match('https?://([^:]*):[^@]*@([^/:]*)(?::.*)?/.*', val[1])
        if match is not None:
            user, server = match.groups()
            raise ParseError, ('repositoryMap entries should not contain '
                               'user names and passwords; use '
                               '"user %s %s <password>" instead' %
                               (server, user))

        return (val[0], val[1])

    def format(self, val, displayOptions=None):
        return '%-25s %s' % (val[0], val[1])

class RepoMap(ServerGlobList):

    # Pretend to be a dict; repositorymap's used to be dicts and this should
    # ease the transition.

    def __setitem__(self, key, val):
        self.append((key, val))

    def __getitem__(self, key):
        return self.find(key)

    def has_key(self, key):
        r = self.find(key)
        if r is None:
            return False
        return True

    def update(self, other):
        for key, val in other.iteritems():
            self.append((key, val))

    def iteritems(self):
        return iter(self)

    def items(self):
        return self

    def keys(self):
        return [ x[0] for x in self ]

    def iterkeys(self):
        return ( x[0] for x in self )

    def values(self):
        return [ x[1] for x in self ]

    def itervalues(self):
        return ( x[1] for x in self )

    def get(self, key, default):
        r = self.find(key)
        if r is None:
            return default

        return r

    def __init__(self, repoMap=[]):
        if hasattr(repoMap, 'iteritems'):
            ServerGlobList.__init__(self)
            self.update(repoMap)
        else:
            ServerGlobList.__init__(self, repoMap)

class CfgRepoMap(CfgList):
    def __init__(self, default=[]):
        CfgList.__init__(self, CfgRepoMapEntry, RepoMap, default=default)

    def set(self, curVal, newVal):
        curVal.extend(newVal)
        return curVal


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

class ProxyEntry(CfgType):

    def parseString(self, str):
        match = re.match('https?://.*', str)
        if match is None:
            raise ParseError('Invalid proxy url %s' % str)

        return CfgType.parseString(self, str)

class CfgProxy(CfgDict):

    def updateFromString(self, val, str):
        suppProtocols = ['http', 'https']
        vlist = str.split()
        if len(vlist) > 2:
            raise ParseError("Too many arguments for proxy configuration '%s'"
                             % str)
        if not vlist:
            raise ParseError("Arguments required for proxy configuration")
        if len(vlist) == 2:
            if vlist[0] not in suppProtocols:
                raise ParseError('Unknown proxy procotol %s' % vlist[0])
            if vlist[1] == "None":
                # Special value to turn proxy values off
                if vlist[0] in val:
                    del val[vlist[0]]
                return val
            return CfgDict.updateFromString(self, val, str)

        # At this point, len(vlist) == 1
        # Fix it up
        try:
            protocol, rest = str.split(':', 1)
        except ValueError:
            # : not in the value
            if str == "None":
                # Special value that turns off the proxy
                for protocol in suppProtocols:
                    if protocol in val:
                        del val[protocol]
                return val
            raise ParseError("Invalid proxy configuration value %s" % str)

        # This next test duplicates the work done by ProxyEntry.parseString,
        # but it's pretty cheap to do here since we already have the protocol
        # parsed out
        if protocol not in suppProtocols:
                raise ParseError('Unknown proxy procotol %s' % protocol)

        CfgDict.updateFromString(self, val, 'http http:' + rest)
        CfgDict.updateFromString(self, val, 'https https:' + rest)
        return val

    def __init__(self, default={}):
        CfgDict.__init__(self, ProxyEntry, default=default)

CfgInstallLabelPath = CfgLineList(CfgLabel, listType = CfgLabelList)
CfgDependencyClassList = CfgLineList(CfgDependencyClass)


class CfgSearchPathItem(CfgType):
    def parseString(self, item):
        return item
CfgSearchPath = CfgLineList(CfgSearchPathItem)


class CfgProxyMap(CfgType):
    default = proxy_map.ProxyMap()

    def updateFromString(self, val, string):
        parts = string.split()
        if parts == ['[]']:
            val.clear()
            return val
        if len(parts) < 2:
            raise ParseError("Expected: proxyMap <pattern> "
                    "[http://proxy1|DIRECT] [proxy2 ...]")
        pattern, targets = parts[0], parts[1:]
        val.addStrategy(pattern, targets)
        return val

    def setFromString(self, val, string):
        pm = val.__class__()
        return self.updateFromString(pm, string)

    def toStrings(self, value, displayOptions):
        for pattern, targets in value.filterList:
            yield ' '.join([str(pattern)] + [str(x) for x in targets])


class CfgCapsuleSync(CfgEnum):
    validValues = [ 'false', 'clean', 'pin', 'update' ]

    def parseString(self, val):
        if val.lower() == 'true':
            # Backwards compatibility
            val = 'clean'
        return CfgEnum.parseString(self, val)


def _getDefaultPublicKeyrings():
    publicKeyrings = []
    # If we are root, don't use the keyring in $HOME, since a process started
    # under sudo will have $HOME set to the old user's (CNY-2630)

    # CNY-2722: look up the directory with getpwuid, instead of using $HOME

    try:
        import pwd
        ent = pwd.getpwuid(os.getuid())
        pwDir = ent[5]
        # If home dir doesn't exist, don't bother
        if os.path.isdir(pwDir):
            publicKeyrings.append(os.path.join(pwDir, '.gnupg', 'pubring.gpg'))
    except KeyError:
        pass

    publicKeyrings.append('/etc/conary/pubring.gpg')
    return publicKeyrings


def _getDefaultTempDir():
    if os.name == 'posix':
        return '/var/tmp'
    else:
        return tempfile.gettempdir()


class ConaryContext(ConfigSection):
    """ Conary uses context to let the value of particular config parameters
        be set based on a keyword that can be set at the command line.
        Configuartion values that are set in a context are overridden
        by the values in the context that have been set.  Values that are
        unset in the context do not override the default config values.
    """
    archDirs              =  (CfgPathList, ('/etc/conary/arch',
                                            '/etc/conary/distro/arch',
                                            '~/.conary/arch'))
    autoLoadRecipes       =  CfgList(CfgString)
    autoResolve           =  (CfgBool, False)
    autoResolvePackages   =  (CfgBool, True)
    buildFlavor           =  CfgFlavor
    buildLabel            =  CfgLabel
    buildPath             =  (CfgPath, '~/conary/builds')
    cleanAfterCook        =  (CfgBool, True)
    commitRelativeChangeset = (CfgBool, False)
    componentDirs         =  (CfgPathList, ('/etc/conary/components',
                                            '/etc/conary/distro/components',
                                            '~/.conary/components'))
    configComponent       =  (CfgBool, True)
    contact               =  None
    context               =  None
    dbPath                =  '/var/lib/conarydb'
    debugExceptions       =  (CfgBool, False)
    debugRecipeExceptions =  (CfgBool, False)
    defaultMacros         =  (CfgPathList, ('/etc/conary/macros',
                                            '/etc/conary/macros.d/*',
                                            '~/.conary/macros'))
    emergeUser            =  (CfgString, 'emerge')
    enableRPMVersionDeps  =  (CfgBool, True)
    enforceManagedPolicy  =  (CfgBool, True)
    entitlement           =  CfgEntitlement
    entitlementDirectory  =  (CfgPath, '/etc/conary/entitlements')
    environment           =  CfgDict(CfgString)
    excludeTroves         =  CfgRegExpList
    flavor                =  CfgList(CfgFlavor)
    flavorPreferences     =  CfgList(CfgFlavor)
    fullVersions          =  CfgBool
    fullFlavors           =  CfgBool
    localRollbacks        =  CfgBool
    keepRequired          =  CfgBool
    ignoreDependencies    =  (CfgDependencyClassList,
                              [ deps.AbiDependency, deps.RpmLibDependencies])
    installLabelPath      =  CfgInstallLabelPath
    interactive           =  (CfgBool, False)
    logFile               =  (CfgPathList, ('/var/log/conary',
                                            '~/.conary/log',))
    lookaside             =  (CfgPath, '~/conary/cache')
    macros                =  CfgDict(CfgString)
    mirrorDirs            =  (CfgPathList, ('~/.conary/mirrors',
                                            '/etc/conary/distro/mirrors',
                                            '/etc/conary/mirrors',))
    modelPath             =  '/etc/conary/system-model'
    name                  =  None
    quiet                 =  CfgBool
    pinTroves             =  CfgRegExpList
    policyDirs            =  (CfgPathList, ('/usr/lib/conary/policy',
                                            '/usr/lib/conary/distro/policy',
                                            '/etc/conary/policy',
                                            '~/.conary/policy'))
    shortenGroupFlavors   =  CfgBool
    syncCapsuleDatabase   =  (CfgCapsuleSync, 'update')
    # Upstream Conary proxy
    conaryProxy           =  CfgProxy
    # HTTP proxy
    proxy                 =  CfgProxy
    proxyMap              =  CfgProxyMap
    connectAttempts       = (CfgInt, 3, "Number of connection attempts to make "
            "for outbound HTTP requests.")
    downloadAttempts      = (CfgInt, 3, "Number of attempts to restart an "
            "interrupted download")
    downloadRetryThreshold = (CfgBytes('M'), 10000000,
            "Reset the download attempt count if at least this many megabytes "
            "have been transferred since the last failure")
    downloadRetryTrim     =  (CfgBytes('k'), 1000000,
            "If a download is reattempted, trim this many kilobytes off the "
            "end of what was previously downloaded. 0 disables this feature.")
    # The first keyring in the list is writable, and is used for storing the
    # keys that are not present on the system-wide keyring. Always expect
    # Conary to write to the first keyring.
    pubRing               =  (CfgPathList, _getDefaultPublicKeyrings())
    uploadRateLimit       =  (CfgBytes(perSecond=True), 0,
            "Upload rate limit, in bytes per second")
    downloadRateLimit     =  (CfgBytes(perSecond=True), 0,
            "Download rate limit, in bytes per second")

    recipeTemplate        =  None
    groupTemplate         =  None
    factoryTemplate       =  None
    repositoryMap         =  CfgRepoMap
    resolveLevel          =  (CfgInt, 2)
    root                  =  (CfgPath, '/')
    recipeTemplateDirs    =  (CfgPathList, ('~/.conary/recipeTemplates',
                                            '/etc/conary/recipeTemplates'))
    showLabels            =  CfgBool
    showComponents        =  CfgBool
    searchPath            =  CfgSearchPath
    signatureKey          =  CfgFingerPrint
    signatureKeyMap       =  CfgFingerPrintMap
    siteConfigPath        =  (CfgPathList, ('/etc/conary/site',
                                            '/etc/conary/distro/site',
                                            '~/.conary/site'))
    sourceSearchDir       =  (CfgPath, '.')
    threaded              =  (CfgBool, True)
    downloadFirst         =  (CfgBool, False)
    tmpDir                =  (CfgPath, _getDefaultTempDir())
    trustThreshold        =  (CfgInt, 0)
    trustedCerts          =  (CfgPathList, (),
            'List of CA certificates which are trusted to identify a remote '
            'repository using SSL. Entries may be files, dirs, or globs.')
    trustedKeys           =  (CfgList(CfgString), [])
    updateThreshold       =  (CfgInt, 15)
    useDirs               =  (CfgPathList, ('/etc/conary/use',
                                            '/etc/conary/distro/use',
                                            '~/.conary/use'))
    user                  =  CfgUserInfo
    baseClassDir          =  (CfgPath, '/usr/share/conary/baseclasses')
    verifyDirsNoNewFiles  =  (CfgPathList, ('/proc', '/sys', '/home', '/dev',
                                            '/mnt', '/tmp', '/var',
                                            '/media', '/initrd' ))
    windowsBuildService   = CfgString

    systemIdScript        = CfgPath

    def _resetSigMap(self):
        self.resetToDefault('signatureKeyMap')

    def __init__(self, *args, **kw):
        ConfigSection.__init__(self, *args, **kw)
        self.addListener('signatureKey', lambda *args: self._resetSigMap())

    def _writeKey(self, out, cfgItem, value, options):
        # Suppress all default values, as opposed to the default behavior which
        # only suppresses defaults that are None
        name = cfgItem.name
        if name not in self._values or self._values[name].isDefault():
            return
        ConfigSection._writeKey(self, out, cfgItem, value, options)


class ConaryConfiguration(SectionedConfigFile):

    # Inherit all context options
    _cfg_bases = (ConaryContext,)

    # this allows a new section to be created on the fly with the type
    # ConaryContext
    _allowNewSections     = True
    _defaultSectionType   =  ConaryContext

    @api.publicApi
    def __init__(self, readConfigFiles = False, ignoreErrors = False,
                 readProxyValuesFirst=True):
        """
        Initialize a ConaryConfiguration object

        @param readConfigFiles: If True, read /etc/conaryrc and entitlements
        files
        @type readConfigFiles: bool

        @param ignoreErrors: If True, ParseError exceptions will not be raised
        @type ignoreErrors: bool

        @param readProxyValuesFirst: If True, parse local config files for
        proxy settings and apply them before further configuration.
        @type readProxyValuesFirst: bool

        @raises ParseError: Raised if configuration syntax is invalid and
        ignoreErrors is False.
        """
        SectionedConfigFile.__init__(self)
        self._ignoreErrors = ignoreErrors

        self.addListener('signatureKey', lambda *args: self._resetSigMap())

        if readConfigFiles:
            if readProxyValuesFirst:
                self.limitToKeys('conaryProxy', 'proxy')
                self.ignoreUrlIncludes()
                self.readFiles()
                self.limitToKeys(False)
                self.ignoreUrlIncludes(False)

            self.readFiles()
            # Entitlement files are config files
            self.readEntitlementDirectory()

        util.settempdir(self.tmpDir)

    def getProxyMap(self):
        return getProxyMap(self)

    def _getOpener(self):
        return transport.URLOpener(proxyMap=self.getProxyMap(),
                connectAttempts=self.connectAttempts)

    def readEntitlementDirectory(self):
        if not os.path.isdir(self.entitlementDirectory):
            return

        try:
            files = os.listdir(self.entitlementDirectory)
        except OSError:
            return
        for basename in files:
            try:
                if os.path.isfile(os.path.join(self.entitlementDirectory,
                                               basename)):
                    ent = loadEntitlement(self.entitlementDirectory, basename)
                    if not ent:
                        continue
                    self.entitlement.addEntitlement(ent[0], ent[2],
                                                    entClass = ent[1])
            except OSError:
                return

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

        for key, ctxval in context._values.iteritems():
            if ctxval.isDefault():
                continue
            newval = self._cow(key)
            newval.updateFromContext(ctxval)
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
        # NOTE - conary doesn't use this check anymore.  Kept for
        # backwards compatibility.
        if not self.installLabelPath:
            print >> sys.stderr, "installLabelPath is not set"
            sys.exit(1)

    def _resetSigMap(self):
        self.resetToDefault('signatureKeyMap')

    def initializeFlavors(self):
        """
        Initialize flavor preferences based on files typically
        found in /etc/conary/arch (archDirs) and /etc/conary/use

        @raises RuntimeError: Raised if use flags conflict in
        a way which cannot be reconciled
        (see L{deps.DependencyClass.MergeFlags})

        """
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
        if self.isDefault('flavorPreferences'):
            self.flavorPreferences = arch.getFlavorPreferencesFromFlavor(
                                                                self.flavor[0])
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

def emitEntitlement(serverName, className = None, key = None, timeout = None,
                    retryOnTimeout = None):

    # XXX This probably should be emitted using a real XML DOM writer,
    # but this will probably do for now. And yes, all that mess is required
    # to be well-formed and valid XML.
    if className is None:
        classInfo = ""
    else:
        classInfo = "<class>%s</class>" % className

    s = """<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>
<!DOCTYPE entitlement [
    <!ELEMENT entitlement (server, class, key)>
    <!ELEMENT server (#PCDATA)>
    <!ELEMENT class (#PCDATA)>
    <!ELEMENT key (#PCDATA)>
    <!ELEMENT timeout EMPTY>
    <!ATTLIST
    timeout retry (True|False) "True"
    val CDATA #IMPLIED>
]>
<entitlement>
    <server>%s</server>
    %s
    <key>%s</key>
""" % (serverName, classInfo, key)

    if timeout is not None or retryOnTimeout is not None:
        s += "    <timeout "
        if timeout is not None:
            s += 'val="%d" ' % timeout
        if retryOnTimeout:
            s += 'retry="True" '
        elif retryOnTimeout is not None:
            s += 'retry="False" '

        s += '/>\n'

    s += "</entitlement>\n"

    return s

def loadEntitlementFromString(xmlContent, *args, **kw):
    # handle old callers
    source=kw.get('source', '<override>')
    serverName = kw.get('serverName', None)
    if len(args):
        if len(args) == 1:
            source = args[0]
        elif len(args) == 2:
            serverName = args[0]
            source = args[1]
        else:
            raise TypeError('loadEntitlementFromString() takes exactly 1 argument (%d given)' %len(args))

    if serverName:
        import warnings
        warnings.warn("The serverName argument to loadEntitlementFromString "
                      "has been deprecated", DeprecationWarning)

    returnTimeout = kw.pop('returnTimeout', False)

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
            entClass = p.get('class', None)
            entKey = p['key']
        except KeyError:
            raise errors.ConaryError("Entitlement incomplete.  Entitlements"
                                     " must include 'server', 'class', and"
                                     " 'key' values")
    except Exception, err:
        raise errors.ConaryError("Malformed entitlement at %s:"
                                 " %s" % (source, err))


    if returnTimeout:
        return (p['server'], entClass, entKey, p['timeout'], p['retry'])

    return (p['server'], entClass, entKey)

def loadEntitlementFromProgram(fullPath, serverName):
    """ Executes the given file to generate an entitlement.
        The executable must print to stdout a full valid entitlement xml
        blob.
    """
    readFd, writeFd = os.pipe()
    stdErrRead, stdErrWrite = os.pipe()
    childPid = os.fork()
    if not childPid:
        nullFd = os.open("/dev/null", os.O_RDONLY)
        try:
            try:
                os.close(readFd)
                # switch stdin to /dev/null
                os.dup2(nullFd, 0)
                os.close(nullFd)

                # both error and stderr are redirected  - the entitlement
                # should be on stdout, and error info should be
                # on stderr.
                os.dup2(writeFd, 1)
                os.dup2(stdErrWrite, 2)
                os.close(writeFd)
                os.close(stdErrWrite)
                util.massCloseFileDescriptors(3, 252)
                os.execl(fullPath, fullPath, serverName)
            except Exception:
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
    os.close(readFd)
    os.close(stdErrRead)

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
    return loadEntitlementFromString(xmlContent, fullPath)


def loadEntitlement(dirName, serverName):
    if not dirName:
        # XXX
        # this is a hack for the repository server which doesn't support
        # entitlements, but needs to stop cross talking anyway
        return None

    fullPath = os.path.join(dirName, serverName)

    if not os.access(fullPath, os.R_OK):
        return None

    if os.access(fullPath, os.X_OK):
        return loadEntitlementFromProgram(fullPath,
                                          '<executable %s>' % fullPath)
    elif os.access(fullPath, os.R_OK):
        return loadEntitlementFromString(open(fullPath).read(), fullPath)
    else:
        return None

class EntitlementParser(dict):

    def StartElementHandler(self, name, attrs):
        if name not in [ 'entitlement', 'server', 'class', 'key', 'timeout' ]:
            raise SyntaxError
        self.state.append((str(name), attrs))
        self.data = None

    def EndElementHandler(self, name):
        state, attrs = self.state.pop()
        if state == 'timeout':
            self['retry'] = (str(attrs['retry']) == 'True')
            if 'val' in attrs:
                self['timeout'] = int(attrs['val'])
        else:
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
        self['retry'] = True
        self['timeout'] = None

def getProxyFromConfig(cfg):
    """Get the proper proxy configuration variable from the supplied config
    object"""

    # Is there a conaryProxy defined?
    proxy = {}
    for k, v in cfg.conaryProxy.iteritems():
        # Munge http.* to conary.* to flag the transport layer that
        # we're using a Conary proxy
        v = 'conary' + v[4:]
        proxy[k] = v
    if proxy:
        return proxy
    return cfg.proxy

def getProxyMap(cfg):
    """
    Return the proxyMap, or create it from old-style proxy/conaryProxy
    entries.
    """
    if cfg.proxyMap:
        return cfg.proxyMap

    # This creates a new proxyMap instance. We don't want to override the
    # config's proxyMap, since old consumers of the API may modify the settings
    # in-place and expect the changes to take effect.
    proxyDict = urllib.getproxies()
    proxyDict.update(cfg.proxy)
    if hasattr(cfg, 'conaryProxy'):
        for scheme, url in cfg.conaryProxy.items():
            if url.startswith('http:'):
                url = 'conary:' + url[5:]
            elif url.startswith('https:'):
                url = 'conarys:' + url[6:]
            proxyDict[scheme] = url
    return proxy_map.ProxyMap.fromDict(proxyDict)


# These are regrettably part of the module's published API
# pyflakes=ignore
from conary.lib.cfgtypes import (CfgType, CfgString, CfgPath, CfgInt, CfgBool,
        CfgRegExp, CfgSignedRegExp, CfgEnum, CfgCallBack, CfgLineList,
        CfgQuotedLineList, CfgList, CfgDict, CfgEnumDict, CfgRegExpList,
        CfgSignedRegExpList, CfgError, CfgEnvironmentError,
        RegularExpressionList, SignedRegularExpressionList, CfgPathList)
# pyflakes=ignore
from conary.lib.cfg import ConfigFile, ConfigSection, SectionedConfigFile
