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
Implements troves (packages, components, etc.) for the repository
"""

import itertools
import struct

from conary import changelog
from conary import errors
from conary import files
from conary import streams
from conary import versions
from conary.deps import deps
from conary.lib import misc, sha1helper
from conary.lib.openpgpfile import KeyNotFound, TRUST_UNTRUSTED
from conary.lib import openpgpkey
from conary.streams import ByteStream
from conary.streams import DependenciesStream, FlavorsStream
from conary.streams import FrozenVersionStream
from conary.streams import SMALL, LARGE
from conary.streams import StringVersionStream

TROVE_VERSION=10
# the difference between 10 and 11 is that the REMOVED type appeared, 
# and we allow group redirects; 11 is used *only* for those situations
TROVE_VERSION_1_1=11

def troveIsCollection(troveName):
    return not(":" in troveName or troveName.startswith("fileset-"))

def troveIsPackage(troveName):
    return troveIsCollection(troveName) and not troveName.startswith('group-')

def troveIsComponent(troveName):
    return ":" in troveName

def troveNameIsValid(troveName):
    return not True in (x in troveName for x in '/[]!~,:=()')

class TroveTuple(streams.StreamSet):
    _SINGLE_TROVE_TUP_NAME    = 0
    _SINGLE_TROVE_TUP_VERSION = 1
    _SINGLE_TROVE_TUP_FLAVOR  = 2

    ignoreUnknown = True
    streamDict = {
        _SINGLE_TROVE_TUP_NAME    : (SMALL, streams.StringStream,        'name'    ),
        _SINGLE_TROVE_TUP_VERSION : (SMALL, streams.StringVersionStream, 'version' ),
        _SINGLE_TROVE_TUP_FLAVOR  : (SMALL, streams.FlavorsStream,       'flavor'  )
    }

    def __cmp__(self, other):
        first = self.name()
        second = other.name()

        if first == second:
            first = self.freeze()
            second = other.freeze()

        return cmp(first, second)

    def __hash__(self):
        return hash((self.name(), self.version(), self.flavor()))

class TroveTupleList(streams.StreamCollection):
    streamDict = { 1 : TroveTuple }

    def add(self, name, version, flavor):
        dep = TroveTuple()
        dep.name.set(name)
        dep.version.set(version)
        dep.flavor.set(flavor)
        self.addStream(1, dep)

    def remove(self, tup):
        self.delStream(1, tup)

    def iter(self):
        return ( x[1] for x in self.iterAll() )

class OptionalFlavorStream(streams.FlavorsStream):

    def freeze(self, skipSet = None):
        if self.deps is None:
            return '\0'

        return streams.FlavorsStream.freeze(self)

    def thaw(self, s):
        if s == '\0':
            self.deps = None
        else:
            streams.FlavorsStream.thaw(self, s)

    def diff(self, other, skipSet = None):
        if self.deps is None and other.deps is None:
            return ''
        elif self.deps is None:
            return '\0';

        return streams.FlavorsStream.diff(self, other)

    def set(self, val):
        # None is okay
        self.deps = val

class SingleTroveRedirect(streams.StreamSet):
    _SINGLE_REDIRECT_NAME   = 1
    _SINGLE_REDIRECT_BRANCH = 2
    _SINGLE_REDIRECT_FLAVOR = 3

    ignoreUnknown = True
    streamDict = {
        _SINGLE_REDIRECT_NAME   : 
                (SMALL, streams.StringStream,        'name'    ),
        _SINGLE_REDIRECT_BRANCH : 
                (SMALL, streams.StringVersionStream, 'branch' ),
        _SINGLE_REDIRECT_FLAVOR : 
                (SMALL, OptionalFlavorStream,        'flavor'  )
    }

    def __cmp__(self, other):
        first = self.name()
        second = other.name()

        if first == second:
            first = self.freeze()
            second = other.freeze()

        return cmp(first, second)

    def __hash__(self):
        return hash((self.name(), self.branch(), self.flavor()))

class TroveRedirectList(streams.StreamCollection):
    streamDict = { 1 : SingleTroveRedirect }

    def add(self, name, branch, flavor):
        dep = SingleTroveRedirect()
        dep.name.set(name)
        dep.branch.set(branch)
        dep.flavor.set(flavor)
        self.addStream(1, dep)

    def addRedirectObject(self, o):
        self.addStream(1, o)

    def remove(self, tup):
        self.delStream(1, tup)

    def reset(self):
        self.thaw("")

    def iter(self):
        return ( x[1] for x in self.iterAll() )

class LabelPath(streams.OrderedStringsStream):
    pass

class BuildDependencies(TroveTupleList):
    pass

class PolicyProviders(TroveTupleList):
    pass

class LoadedTroves(TroveTupleList):
    pass

class PathHashes(set, streams.InfoStream):

    """
    A set of 8 bytes hashes from the first 8 bytes of the md5 of each path.
    """

    def __eq__(self, other, skipSet = None):
        return set.__eq__(self, other)

    def __ne__(self, other, skipSet = None):
        return set.__ne__(self, other)

    def freeze(self, skipSet = None):
        return struct.pack("8s" * len(self), *sorted(self))

    def thaw(self, s):
        self.clear()
        items = struct.unpack("8s" * (len(s) / 8), s)
        for item in items:
            self.add(item)

    def diff(self, them):
        additions = self - them
        removals = them - self
        s = struct.pack("!I" + "8s" * (len(additions) + len(removals)),
                        *itertools.chain((len(additions),), additions, 
                                         removals))
        return s

    def twm(self, diff, base):
        assert(self == base)
        items = struct.unpack("!I" + "8s" * ((len(diff) - 4) / 8), diff)
        additions = items[1:items[0] + 1]
        removals = items[items[0] + 1:]

        self.difference_update(removals)
        self.update(additions)

    def add(self, item):
        assert(len(item) == 8)
        set.add(self, item)

    def addPath(self, path):
        self.add(sha1helper.md5String(path)[0:8])

    def compatibleWith(self, other):
        return not(self & other)

    def __init__(self, val = None):
        set.__init__(self)
        if val is not None:
            self.thaw(val)

_DIGSIG_FINGERPRINT   = 0
_DIGSIG_SIGNATURE     = 1
_DIGSIG_TIMESTAMP     = 2

class DigitalSignature(streams.StreamSet):
    streamDict = {
        _DIGSIG_FINGERPRINT  : (SMALL, streams.StringStream,   'fingerprint' ),
        _DIGSIG_SIGNATURE    : (SMALL, streams.StringStream,   'signature'   ),
        _DIGSIG_TIMESTAMP    : (SMALL, streams.IntStream,      'timestamp'   ),
    }

    def _mpiToLong(self, data):
        length = ((ord(data[0]) << 8) + ord(data[1]) + 7) / 8
        r = 0L
        for i in range(length):
            r <<= 8
            r += ord(data[i + 2])
        return r

    def _longToMpi(self, data):
        bits = data
        l = 0
        while bits:
            bits /= 2
            l += 1
        r = chr((l >> 8) & 0xFF) + chr(l & 0xFF)
        buf = ''
        while data:
            buf = chr(data & 0xFF) + buf
            data = data >> 8
        return r + buf

    def set(self, val):
        self.fingerprint.set(val[0])
        self.timestamp.set(val[1])
        numMPIs = len(val[2])
        buf = ''
        for i in range(0,len(val[2])):
            buf += self._longToMpi(val[2][i])
        self.signature.set(chr(numMPIs) + buf)

    def get(self):
        data = self.signature()
        numMPIs = ord(data[0])
        index = 1
        mpiList = []
        for i in range(0,numMPIs):
            lengthMPI = ((ord(data[index]) * 256) +
                         (ord(data[index + 1]) + 7)) / 8 + 2
            mpiList.append(self._mpiToLong(data[index:index + lengthMPI]))
            index += lengthMPI
        return (self.fingerprint(), self.timestamp(), tuple(mpiList))

_DIGSIGS_DIGSIGNATURE     = 1
# since digital signatures can be added to the server over time, we
# should always send the whole stream collection as a
# AbsoluteStreamCollection
class DigitalSignatures(streams.AbsoluteStreamCollection):
    streamDict = { _DIGSIGS_DIGSIGNATURE: DigitalSignature }

    def add(self, val):
        signature = DigitalSignature()
        signature.set(val)
        self.addStream(_DIGSIGS_DIGSIGNATURE, signature)

    def iter(self):
        for signature in self.getStreams(_DIGSIGS_DIGSIGNATURE):
            yield signature.get()

    # this function is for convenience. It reduces code duplication in
    # netclient and netauth (since I need to pass the frozen form thru
    # xmlrpc)
    def getSignature(self, keyId):
        for sig in self.iter():
            if keyId in sig[0]:
                return sig
        raise KeyNotFound('Signature by key: %s does not exist' %keyId)

_TROVESIG_SHA1   = 0
_TROVESIG_DIGSIG = 1

class TroveSignatures(streams.StreamSet):
    ignoreUnknown = True
    streamDict = {
        _TROVESIG_SHA1      : ( SMALL, streams.AbsoluteSha1Stream, 'sha1'   ),
        _TROVESIG_DIGSIG    : ( LARGE, DigitalSignatures,     'digitalSigs' ),
    }

    # this code needs to be called any time we're making a derived
    # trove esp. shadows. since some info in the trove gets changed
    # we cannot allow the signatures to persist.
    def reset(self):
        self.digitalSigs = DigitalSignatures()
        self.sha1 = streams.AbsoluteSha1Stream()

    def freeze(self, skipSet = {}):
        return streams.StreamSet.freeze(self, skipSet = skipSet)

_TROVE_FLAG_ISCOLLECTION = 1 << 0

class TroveFlagsStream(streams.NumericStream):

    __slots__ = "val"
    format = "B"

    def isCollection(self, set = None):
	return self._isFlag(_TROVE_FLAG_ISCOLLECTION, set)

    def _isFlag(self, flag, set):
	if set != None:
            if self.val is None:
                self.val = 0x0
	    if set:
		self.val |= flag
	    else:
		self.val &= ~(flag)

	return (self.val and self.val & flag)

_TROVEINFO_TAG_SIZE           =  0
_TROVEINFO_TAG_SOURCENAME     =  1
_TROVEINFO_TAG_BUILDTIME      =  2
_TROVEINFO_TAG_CONARYVER      =  3
_TROVEINFO_TAG_BUILDDEPS      =  4
_TROVEINFO_TAG_LOADEDTROVES   =  5
_TROVEINFO_TAG_INSTALLBUCKET  =  6          # unused as of 0.62.16
_TROVEINFO_TAG_FLAGS          =  7
_TROVEINFO_TAG_CLONEDFROM     =  8
_TROVEINFO_TAG_SIGS           =  9
_TROVEINFO_TAG_PATH_HASHES    = 10 
_TROVEINFO_TAG_LABEL_PATH     = 11 
_TROVEINFO_TAG_POLICY_PROV    = 12
_TROVEINFO_TAG_TROVEVERSION   = 13
_TROVEINFO_TAG_INCOMPLETE     = 14

class TroveInfo(streams.StreamSet):
    ignoreUnknown = True
    streamDict = {
        _TROVEINFO_TAG_SIZE          : (SMALL, streams.LongLongStream,'size'        ),
        _TROVEINFO_TAG_SOURCENAME    : (SMALL, streams.StringStream,  'sourceName'  ),
        _TROVEINFO_TAG_BUILDTIME     : (SMALL, streams.LongLongStream,'buildTime'   ),
        _TROVEINFO_TAG_CONARYVER     : (SMALL, streams.StringStream, 'conaryVersion'),
        _TROVEINFO_TAG_BUILDDEPS     : (LARGE, BuildDependencies,    'buildReqs'    ),
        _TROVEINFO_TAG_LOADEDTROVES  : (LARGE, LoadedTroves,         'loadedTroves' ),
        #_TROVEINFO_TAG_INSTALLBUCKET : ( InstallBucket,       'installBucket'),
        _TROVEINFO_TAG_FLAGS         : (SMALL, TroveFlagsStream,     'flags'        ),
        _TROVEINFO_TAG_CLONEDFROM    : (SMALL, StringVersionStream,  'clonedFrom'   ),
        _TROVEINFO_TAG_SIGS          : (LARGE, TroveSignatures,      'sigs'         ),
        _TROVEINFO_TAG_PATH_HASHES   : (LARGE, PathHashes,           'pathHashes'   ),
        _TROVEINFO_TAG_LABEL_PATH    : (SMALL, LabelPath,            'labelPath'   ),
        _TROVEINFO_TAG_POLICY_PROV   : (LARGE, PolicyProviders,      'policyProviders'),
        _TROVEINFO_TAG_TROVEVERSION  : (SMALL, streams.IntStream,    'troveVersion'   ),
        _TROVEINFO_TAG_INCOMPLETE    : (SMALL, streams.ByteStream,   'incomplete'   )
    }


class TroveRefsTrovesStream(dict, streams.InfoStream):

    """
    Defines a dict which represents the troves referenced by a trove. Each
    entry maps a (troveName, version, flavor) tuple to a byDefault (boolean) 
    value.

    It can be frozen (to allow signatures to be calculated), but the other
    stream methods are not provided. The frozen form is intended to be
    easily extended if that becomes necessary at some later point.
    """

    def freeze(self, skipSet = {}):
        """
        Frozen form is a sequence of:
            total entry size, excluding these two bytes (2 bytes)
            troveName length (2 bytes)
            troveName
            version string length (2 bytes)
            version string
            flavor string length (2 bytes)
            flavor string
            byDefault value (1 byte, 0 or 1)

        This whole thing is sorted by the string value of each entry. Sorting
        this way is a bit odd, but it's simple and well-defined.
        """
        l = []
        for ((name, version, flavor), byDefault) in self.iteritems():
            v = version.asString()
            f = flavor.freeze()
            s = (struct.pack("!H", len(name)) + name +
                 struct.pack("!H", len(v)) + v +
                 struct.pack("!H", len(f)) + f +
                 struct.pack("B", byDefault))
            l.append(struct.pack("!H", len(s)) + s)

        l.sort()

        return "".join(l)

    def copy(self):
        new = TroveRefsTrovesStream()
        for key, val in self.iteritems():
            new[key] = val

        return new

class TroveRefsFilesStream(dict, streams.InfoStream):

    """
    Defines a dict which represents the files referenced by a trove. Each
    entry maps a pathId to a (path, fileId, version) tuple.

    It can be frozen (to allow signatures to be calculated), but the other
    stream methods are not provided. The frozen form is slightly more 
    complicated then probably seems necessary, but it's designed to allow more
    information to be added to each entry if it becomes necessary without
    affecting old troves (so the signatures of old troves will still be
    easily computable).
    """

    def freeze(self, skipSet = {}):
        """
        Frozen form is a sequence of:
            total entry size, excluding these two bytes (2 bytes)
            pathId (16 bytes)
            fileId (20 bytes)
            pathLen (2 bytes)
            path
            versionLen (2 bytes)
            version string

        This whole thing is sorted by the string value of each entry. Sorting
        this way is a bit odd, but it's simple and well-defined.
        """
        l = []
        for (pathId, (path, fileId, version)) in self.iteritems():
            v = version.asString()
            s = (pathId + fileId +
                     struct.pack("!H", len(path)) + path +
                     struct.pack("!H", len(v)) + v)
            l.append(struct.pack("!H", len(s)) + s)

        l.sort()

        return ''.join(l)

    def copy(self):
        new = TroveRefsFilesStream()
        for key, val in self.iteritems():
            new[key] = val

        return new

_STREAM_TRV_NAME            = 0
_STREAM_TRV_VERSION         = 1
_STREAM_TRV_FLAVOR          = 2
_STREAM_TRV_CHANGELOG       = 3
_STREAM_TRV_TROVEINFO       = 4
_STREAM_TRV_PROVIDES        = 5
_STREAM_TRV_REQUIRES        = 6
_STREAM_TRV_STRONG_TROVES   = 7
_STREAM_TRV_FILES           = 8
_STREAM_TRV_TYPE            = 9
_STREAM_TRV_SIGS            = 10
_STREAM_TRV_WEAK_TROVES     = 11
_STREAM_TRV_REDIRECTS       = 12

TROVE_TYPE_NORMAL          = 0
TROVE_TYPE_REDIRECT        = 1
TROVE_TYPE_REMOVED         = 2

class Trove(streams.StreamSet):
    """
    Troves are groups of files and other troves, which are included by
    reference. By convention, "component" often refers to a trove with
    files but no other trove, while a "packages" means a trove with other
    troves but no files. While this object allows any mix of file and
    package inclusion, in practice conary doesn't allow it.

    Trove is a stream primarily to allow it to be frozen and have a signature 
    computed. It does provide a nice level of consistency as well. If it were 
    a true stream, diff() would return a string instead of an object (a 
    TroveChangeSet), but that string would be difficult to handle (and
    Conary often directly manipulates TroveChangeSet objects))
    """
    streamDict = { 
        _STREAM_TRV_NAME          : 
                    (SMALL, streams.StringStream,        "name"         ),
        _STREAM_TRV_VERSION       : 
                    (SMALL, streams.FrozenVersionStream, "version"      ), 
        _STREAM_TRV_FLAVOR        : 
                    (LARGE, streams.FlavorsStream,       "flavor"       ), 
        _STREAM_TRV_PROVIDES      : 
                    (LARGE, streams.DependenciesStream,  "provides"     ), 
        _STREAM_TRV_REQUIRES      : 
                    (LARGE, streams.DependenciesStream,  "requires"     ), 
        _STREAM_TRV_CHANGELOG     : 
                    (LARGE, changelog.ChangeLog,         "changeLog"    ), 
        _STREAM_TRV_TROVEINFO     : 
                    (LARGE, TroveInfo,                   "troveInfo"    ), 
        _STREAM_TRV_STRONG_TROVES : 
                    (LARGE, TroveRefsTrovesStream,       "strongTroves" ), 
        _STREAM_TRV_WEAK_TROVES   : 
                    (LARGE, TroveRefsTrovesStream,       "weakTroves"   ), 
        _STREAM_TRV_FILES         : 
                    (LARGE, TroveRefsFilesStream,        "idMap"        ), 
        _STREAM_TRV_TYPE          :
                    (SMALL, ByteStream,                  "type"         ),
        _STREAM_TRV_REDIRECTS     :
                    (SMALL, TroveRedirectList,           "redirects"    ),
    }
    ignoreUnknown = False

    # the memory savings from slots isn't all that interesting here, but it
    # makes sure we don't add data to troves and forget to make it part
    # of the stream
    __slots__ = [ "name", "version", "flavor", "provides", "requires",
                  "changeLog", "troveInfo", "strongTroves", "weakTroves",
                  "idMap", "type", "redirects" ]

    def __repr__(self):
        return "trove.Trove('%s', %s)" % (self.name(), repr(self.version()))

    def _sigString(self):
        return streams.StreamSet.freeze(self,
                                        skipSet = { 'sigs' : True,
                                                    'versionStrings' : True,
                                                    'incomplete' : True,
                                                    'pathHashes' : True })
    def addDigitalSignature(self, keyId, skipIntegrityChecks = False):
        """
        Computes a new signature for this trove and stores it.

        @param keyId: ID of the key use for signing
        @type keyId: str
        """
        sha1_orig = self.troveInfo.sigs.sha1()
        self.computeSignatures()
        assert(skipIntegrityChecks or not sha1_orig or 
               sha1_orig == self.troveInfo.sigs.sha1())

        keyCache = openpgpkey.getKeyCache()
        key = keyCache.getPrivateKey(keyId)
        sig = key.signString(self.troveInfo.sigs.sha1())
        self.troveInfo.sigs.digitalSigs.add(sig)

    def addPrecomputedDigitalSignature(self, sig):
        """
        Adds a previously computed signature. This allows signatures to be
        added to troves.

        @param sig: Signature to add
        @type sig: DigitalSignature
        """
        sha1_orig = self.troveInfo.sigs.sha1()
        sha1_new = self.computeSignatures()
        if sha1_orig:
            assert(sha1_orig == sha1_new)

        signature = DigitalSignature()
        signature.set(sig)
        self.troveInfo.sigs.digitalSigs.addStream(_DIGSIGS_DIGSIGNATURE, 
                                                  signature)

    def getDigitalSignature(self, keyId):
        """
        Returns the signature created by the key whose keyId is passed.

        @param keyId: Id for the key whose signature is returns
        @type keyId: str
        @rtype: DigitalSignature
        """
        return self.troveInfo.sigs.digitalSigs.getSignature(keyId)

    def verifyDigitalSignatures(self, threshold = 0, keyCache = None):
        """
        Verifies the digial signature(s) for the trove against the keys
        contained in keyCache.

        The highest trust level verified is return along with the list of
        keys which were unavailable. A trust level of zero means no trusted
        keys were available. Invalid signatures raise 
        DigitalSignatureVerificationError.

        @param threshold: trust level required; trust levels below this result
        in DigitalSignatureVerificationError
        @type threshold: int
        @param keyCache: cache of keys to verify against
        @type keyCache: openpgpkey.OpenPGPKeyFileCache
        @rtype: (int, list)
        """
        version = self.getVersion()
        serverName = None
        if isinstance(version, (versions.VersionSequence, versions.Label)):
            serverName = version.getHost()
        missingKeys = []
        badFingerprints = []
        maxTrust = TRUST_UNTRUSTED
        sha1_orig = self.troveInfo.sigs.sha1()
        sha1_new = self.computeSignatures(store=False)
        if sha1_orig:
            assert(sha1_orig == sha1_new)

        if keyCache is None:
            keyCache = openpgpkey.getKeyCache()
        for signature in self.troveInfo.sigs.digitalSigs.iter():
            try:
                key = keyCache.getPublicKey(signature[0],
                                            serverName=serverName,
                                            # don't warn about missing gpg
                                            # if the threshold is <= 0
                                            warn=(threshold > 0))
            except KeyNotFound:
                missingKeys.append(signature[0])
                continue
            lev = key.verifyString(self.troveInfo.sigs.sha1(), signature)
            if lev == -1:
                badFingerprints.append(key.getFingerprint())
            maxTrust = max(lev,maxTrust)

        if missingKeys and threshold > 0:
            from conary.lib import log
            log.warning('Unable to import or load the public keys needed '
                        'to verify digital signatures.  A public key '
                        'is needed for: %s', ', '.join(missingKeys))
        if len(badFingerprints):
            raise DigitalSignatureVerificationError(
                    "Trove signatures made by the following keys are bad: %s" 
                            % (' '.join(badFingerprints)))
        if maxTrust < threshold:
            raise DigitalSignatureVerificationError(
                    "Trove does not meet minimum trust level: %s" 
                            % self.getName())
        return maxTrust, missingKeys

    def invalidateSignatures(self):
        self.troveInfo.sigs.reset()

    def computeSignatures(self, store = True):
        """
        Recomputes the sha1 signature of this trove.

        @param store: The newly computed sha1 is stored as the sha1 for this 
        trove.
        @type store: boolean
        @rtype: string
        """
        s = self._sigString()
        sha1 = sha1helper.sha1String(s)

        if store:
            self.troveInfo.sigs.sha1.set(sha1)

        return sha1

    def verifySignatures(self):
        """
        Verifies the sha1 signature of this trove.

        @rtype: boolean
        """

        s = self._sigString()
        sha1 = sha1helper.sha1String(s)

        return sha1 == self.troveInfo.sigs.sha1()

    def copy(self, classOverride = None):
        if not classOverride:
            classOverride = self.__class__

        new = classOverride(self.name(),
                            self.version().copy(),
                            self.flavor().copy(),
                            None,
                            type = self.type(),
                            setVersion = False)
        new.idMap = self.idMap.copy()
        new.strongTroves = self.strongTroves.copy()
        new.weakTroves = self.weakTroves.copy()
        new.provides.thaw(self.provides.freeze())
        new.requires.thaw(self.requires.freeze())
        new.changeLog = changelog.ChangeLog(self.changeLog.freeze())
        new.troveInfo.thaw(self.troveInfo.freeze())
        return new

    def getName(self):
        return self.name()
    
    def getVersion(self):
        return self.version()

    def getNameVersionFlavor(self):
        return self.name(), self.version(), self.flavor()
    
    def changeVersion(self, version):
        self.version.set(version)

    def changeChangeLog(self, cl):
	self.changeLog.thaw(cl.freeze())

    def changeFlavor(self, flavor):
        self.flavor.set(flavor)

    def getSigs(self):
        return self.troveInfo.sigs

    def isRedirect(self):
        return self.type() == TROVE_TYPE_REDIRECT

    def isRemoved(self):
        return self.type() == TROVE_TYPE_REMOVED

    def getType(self):
        return self.type()

    def addFile(self, pathId, path, version, fileId):
	assert(len(pathId) == 16)
	assert(fileId is None or len(fileId) == 20)
        assert(not self.type())
	self.idMap[pathId] = (path, fileId, version)

    def computePathHashes(self):
        for path, fileId, version in self.idMap.itervalues():
            self.troveInfo.pathHashes.addPath(path)

    # pathId is the only thing that must be here; the other fields could
    # be None
    def updateFile(self, pathId, path, version, fileId):
	(origPath, origFileId, origVersion) = self.idMap[pathId]

	if not path:
	    path = origPath

	if not version:
	    version = origVersion
	    
	if not fileId:
	    fileId = origFileId
	    
	self.idMap[pathId] = (path, fileId, version)

    def removeFile(self, pathId):   
	del self.idMap[pathId]

	return self.idMap.iteritems()

    def iterFileList(self):
	# don't use idMap.iteritems() here; we don't want to exposure
	# our internal format
	for (theId, (path, fileId, version)) in self.idMap.iteritems():
	    yield (theId, path, fileId, version)

    def emptyFileList(self):
        return len(self.idMap) == 0

    def getFile(self, pathId):
        x = self.idMap[pathId]
	return (x[0], x[1], x[2])

    def hasFile(self, pathId):
	return self.idMap.has_key(pathId)

    def hasFiles(self):
        return len(self.idMap) != 0

    def addTrove(self, name, version, flavor, presentOkay = False,
                 byDefault = True, weakRef = False):
	"""
	Adds a single version of a trove.

	@param name: name of the trove
	@type name: str
	@param version: version of the trove
	@type version: versions.Version
	@param flavor: flavor of the trove to include
	@type flavor: deps.deps.Flavor
	@param presentOkay: replace if this is a duplicate, don't complain
	@type presentOkay: boolean
	"""
        if weakRef:
            troveGroup = self.weakTroves
        else:
            troveGroup = self.strongTroves
        key = (name, version, flavor)
	if not presentOkay and key in troveGroup:
	    raise TroveError, "duplicate trove included in %s" % self.name()

        troveGroup[key] = byDefault

    def delTrove(self, name, version, flavor, missingOkay, weakRef = False):
	"""
	Removes a single version of a trove.

	@param name: name of the trove
	@type name: str
	@param version: version of the trove
	@type version: versions.Version
	@param flavor: flavor of the trove to include
	@type flavor: deps.deps.Flavor
	@param missingOkay: should we raise an error if the version isn't
	part of this trove?
	@type missingOkay: boolean
	"""
        key = (name, version, flavor)
        if weakRef and key in self.weakTroves:
	    del self.weakTroves[key]
	elif not weakRef and key in self.strongTroves:
	    del self.strongTroves[key]
	elif missingOkay:
	    pass
	else:
	    # FIXME, we should have better text here
	    raise TroveError

    def iterTroveList(self, strongRefs = False, weakRefs = False):
	"""
	Returns a generator for (name, version, flavor) ordered pairs, 
	listing all of the trove in the group, along with their versions. 

	@rtype: list
	"""
        assert(strongRefs or weakRefs)
        if strongRefs:
            for key in self.strongTroves.iterkeys():
                yield key

        if weakRefs:
            for key in self.weakTroves.iterkeys():
                yield key

    def iterTroveListInfo(self):
	"""
	Returns a generator for (name, version, flavor), byDefault, isStrong

	@rtype: list
	"""

        for item, byDefault in self.strongTroves.iteritems():
            yield item, byDefault, True

        for item, byDefault in self.weakTroves.iteritems():
            yield item, byDefault, False

    def isStrongReference(self, name, version, flavor):
        key = (name, version, flavor)
        rc = self.strongTroves.get(key, None)
        if rc is None:
            return False
        return True

    def includeTroveByDefault(self, name, version, flavor):
        key = (name, version, flavor)
        rc = self.strongTroves.get(key, None)
        if rc is None:
            rc = self.weakTroves[key]

        return rc

    def addRedirect(self, toName, toBranch, toFlavor):
        assert(self.type() == TROVE_TYPE_REDIRECT)
        assert(isinstance(toBranch, versions.Branch))
        self.redirects.add(toName, toBranch, toFlavor)

    def iterRedirects(self):
        for o in self.redirects.iter():
            yield o.name(), o.branch(), o.flavor()

    def compatibleWith(self, other):
        return self.troveInfo.pathHashes.compatibleWith(
                                            other.troveInfo.pathHashes)

    def hasTrove(self, name, version, flavor):
        key = (name, version, flavor)
	return (key in self.strongTroves) or (key in self.weakTroves)

    # returns a dictionary mapping a pathId to a (path, version, trvName) tuple
    def applyChangeSet(self, trvCs, skipIntegrityChecks = False, 
                       allowIncomplete = False, skipFiles = False):
	"""
	Updates the trove from the changes specified in a change set.
	Returns a dictionary, indexed by pathId, which gives the
	(path, version, troveName) for that file. This method assumes
        there are no conflicts.

	@param trvCs: change set
	@type trvCs: TroveChangeSet
        @param skipIntegrityChecks: Normally sha1 signatures are confirmed
        after a merge. In some cases (notably where version numbers are
        being changed), this check needs to be skipped.
        @type skipIntegrityChecks: boolean
	@rtype: dict
	"""

        # If we skipFiles, we have to also skipIntegrityChecks
        assert(not skipFiles or skipIntegrityChecks)

	self.type.set(trvCs.getType())
        if self.type():
            # we don't explicitly remove files for non-normal troves
            self.idMap = TroveRefsFilesStream()

	fileMap = {}

        if not skipFiles:
            for (pathId, path, fileId, fileVersion) in trvCs.getNewFileList():
                self.addFile(pathId, path, fileVersion, fileId)
                fileMap[pathId] = self.idMap[pathId] + \
                                    (self.name(), None, None, None)

            for (pathId, path, fileId, fileVersion) in \
                                                    trvCs.getChangedFileList():
                (oldPath, oldFileId, oldVersion) = self.idMap[pathId]
                self.updateFile(pathId, path, fileVersion, fileId)
                # look up the path/version in self.idMap as the ones here
                # could be None
                fileMap[pathId] = self.idMap[pathId] + \
                                (self.name(), oldPath, oldFileId, oldVersion)

            for pathId in trvCs.getOldFileList():
                self.removeFile(pathId)

	self.mergeTroveListChanges(
              trvCs.iterChangedTroves(strongRefs = True,  weakRefs = False),
              trvCs.iterChangedTroves(strongRefs = False, weakRefs = True))
	self.flavor.set(trvCs.getNewFlavor())
	self.changeLog = trvCs.getChangeLog()
	self.setProvides(trvCs.getProvides())
	self.setRequires(trvCs.getRequires())
	self.changeVersion(trvCs.getNewVersion())
	self.changeFlavor(trvCs.getNewFlavor())

        self.redirects.reset()
        for info in trvCs.getRedirects().iter():
            self.redirects.addRedirectObject(info)

        if not trvCs.getOldVersion():
            self.troveInfo = TroveInfo(trvCs.getTroveInfoDiff())
        else:
            self.troveInfo.twm(trvCs.getTroveInfoDiff(), self.troveInfo)

        # We can't be incomplete after a merge. If we were, it means
        # we merged something incomplete against something complete
        # (which is bad) or something incomplete against something
        # incomplete (which, again, is bad)
        if not allowIncomplete and not self.getVersion().isOnLocalHost():
            assert(not self.troveInfo.incomplete())

        if TROVE_VERSION < self.troveInfo.troveVersion() and \
           TROVE_VERSION_1_1 < self.troveInfo.troveVersion():
            self.troveInfo.incomplete.set(1)
        elif self.troveInfo.incomplete() is None:
            # old troves don't have an incomplete flag - we want it to 
            # be set to either 1 or 0 for all troves.
            self.troveInfo.incomplete.set(0)

        # NOTE: Checking for incomplete here is very wrong. It works because
        # incomplete troves can't appear on the server (thanks to an assertion
        # keeping them off).
        if self.troveInfo.incomplete():
            pass
            # we don't warn here because the warning would show up 
            # everywhere we call getTrove as opposed to only when installing
            # from conary.util import log
            #log.warning('Not checking integrity of trove %s with new schema version %s' % (self.getName(), self.troveInfo.troveVersion()))
        elif not skipIntegrityChecks:
            # if we have a sha1 in our troveinfo, verify it
            if self.troveInfo.sigs.sha1():
                if not self.verifySignatures():
                    raise TroveIntegrityError(self.getName(), self.getVersion(),
                                              self.getFlavor())
            else:
                # from conary.util import log
                #log.warning('changeset does not contain a sha1 checksum')
                pass

        assert((not self.idMap) or 
               (not(self.strongTroves and not self.weakTroves)))

	return fileMap

    def mergeTroveListChanges(self, strongChangeList, weakChangeList, 
                              redundantOkay = False):
        """
        Merges a set of changes to the included trove list into this
        trove.

        @param changeList: A list or generator specifying a set of
        trove changes; this is the same as returned by
        TroveChangeSet.iterChangedTroves()
        @type changeList: (name, list) tuple
        @param redundantOkay: Redundant changes are normally considered 
        errors
        @type redundantOkay: boolean
        """

        for (changeList, weakRef, troveDict) in \
                    ( (strongChangeList, False, self.strongTroves),
                      (weakChangeList,   True,  self.weakTroves) ):
            for (name, l) in changeList:
                for (oper, version, flavor, byDefault) in l:
                    if oper == '+':
                        self.addTrove(name, version, flavor,
                                      presentOkay = redundantOkay,
                                      byDefault = byDefault,
                                      weakRef = weakRef)

                    elif oper == "-":
                        self.delTrove(name, version, flavor,
                                               missingOkay = redundantOkay,
                                               weakRef = weakRef)
                    elif oper == "~":
                        troveDict[(name, version, flavor)] = byDefault
                    else:
                        assert(0)
    
    def __eq__(self, them):
	"""
	Compare two troves for equality. This is an expensive operation,
	and shouldn't really be done. It's handy for testing the database
	though.
	"""
        if them is None:
            return False
	if self.getName() != them.getName():
	    return False
	if self.getVersion() != them.getVersion():
	    return False
	if self.getFlavor() != them.getFlavor():
	    return False
	if self.type() != them.type():
	    return False

	(csg, pcl, fcl) = self.diff(them)
	return (not pcl) and (not fcl) and (not csg.getOldFileList()) \
            and self.getRequires() == them.getRequires() \
            and self.getProvides() == them.getProvides() \
            and self.getTroveInfo() == them.getTroveInfo() \
            and set(self.iterRedirects()) == set(them.iterRedirects()) \
            and not([x for x in csg.iterChangedTroves()])


    def __ne__(self, them):
	return not self == them

    def diff(self, them, absolute = 0, getPathHashes = None):
	"""
	Generates a change set between them (considered the old
	version) and this instance. We return the change set, a list
	of other trove diffs which should be included for this change
	set to be complete, and a list of file change sets which need
	to be included.  The list of trove changes is of the form
	(trvName, oldVersion, newVersion, oldFlavor, newFlavor).  If
	absolute is True, oldVersion is always None and absolute diffs
	can be used.  Otherwise, absolute versions are not necessary,
	and oldVersion of None means the trove is new. The list of
	file changes is a list of (pathId, oldVersion, newVersion,
	oldPath, newPath) tuples, where newPath is the path to the
	file in this trove.

	@param them: object to generate a change set from (may be None)
	@type them: Group
	@param absolute: tells if this is a new group or an absolute change
	when them is None
	@type absolute: boolean
	@rtype: (TroveChangeSet, fileChangeList, troveChangeList)
	"""

        def _iterInfo(d, name):
            for flavor, verList in d[name].iteritems():
                for ver in verList:
                    yield (name, ver, flavor)
        
        def _infoByBranch(infoSet):
            byBr = {}
            for info in infoSet:
                d = byBr.setdefault(info[1].branch(), {})
                d.setdefault(info[2], []).append(info[1])

            return byBr

        def troveSetDiff(ourDict, theirDict, weakRefs):
            ourSet = set(ourDict)

            if theirDict:
                theirSet = set(theirDict)
                sameSet = ourSet & theirSet
                addedSet = ourSet - sameSet
                removedSet = theirSet - sameSet
            else:
                sameSet = set()
                addedSet = ourSet
                removedSet = set()

            for key in sameSet:
                if ourDict[key] != theirDict[key]:
                    chgSet.changedTrove(key[0], key[1], key[2], ourDict[key], 
                                        weakRef = weakRefs)

            for key in addedSet:
                chgSet.newTroveVersion(key[0], key[1], key[2], ourDict[key],
                                       weakRef = weakRefs)

            for key in removedSet:
                chgSet.oldTroveVersion(key[0], key[1], key[2],
                                       weakRef = weakRefs)

        # def diff() begins here

	assert(not them or self.name() == them.name())
        assert((not self.idMap) or (not self.strongTroves) or 
               (not self.weakTroves))
        assert((not them) or (not them.idMap) or (not them.strongTroves or
               (not them.weakTroves)))

	# find all of the file ids which have been added, removed, and
	# stayed the same
	if them:
            troveInfoDiff = self.troveInfo.diff(them.troveInfo)
            if troveInfoDiff is None:
                troveInfoDiff = ""

	    themMap = them.idMap
	    chgSet = TroveChangeSet(self.name(), self.changeLog,
                                    them.getVersion(),
                                    self.getVersion(),
                                    them.getFlavor(), self.getFlavor(),
                                    them.getSigs(), self.getSigs(),
                                    absolute = False,
                                    type = self.type(),
                                    troveInfoDiff = troveInfoDiff)
	else:
	    themMap = {}
	    chgSet = TroveChangeSet(self.name(), self.changeLog,
				      None, self.getVersion(),
				      None, self.getFlavor(),
                                      None, self.getSigs(),
				      absolute = absolute,
                                      type = self.type(),
                                      troveInfoDiff = self.troveInfo.freeze())

	# dependency and flavor information is always included in total;
	# this lets us do dependency checking w/o having to load troves
	# on the client
        chgSet.setRequires(self.requires())
        chgSet.setProvides(self.provides())
        chgSet.setRedirects(self.redirects)

	removedIds = []
	addedIds = []
	sameIds = {}
	filesNeeded = []

        if not self.type():
            # we just ignore file information for nonnormal troves
            allIds = self.idMap.keys() + themMap.keys()
            for pathId in allIds:
                inSelf = self.idMap.has_key(pathId)
                inThem = themMap.has_key(pathId)
                if inSelf and inThem:
                    sameIds[pathId] = None
                elif inSelf:
                    addedIds.append(pathId)
                else:
                    removedIds.append(pathId)

            for pathId in removedIds:
                chgSet.oldFile(pathId)

            for pathId in addedIds:
                (selfPath, selfFileId, selfVersion) = self.idMap[pathId]
                filesNeeded.append((pathId, None, None, selfFileId, 
                                    selfVersion))
                chgSet.newFile(pathId, selfPath, selfFileId, selfVersion)

            for pathId in sameIds.keys():
                (selfPath, selfFileId, selfVersion) = self.idMap[pathId]
                (themPath, themFileId, themVersion) = themMap[pathId]

                newPath = None
                newVersion = None

                if selfPath != themPath:
                    newPath = selfPath

                if selfVersion != themVersion or themFileId != selfFileId:
                    newVersion = selfVersion
                    filesNeeded.append((pathId, themFileId, themVersion, 
                                        selfFileId, selfVersion))

                if newPath or newVersion:
                    chgSet.changedFile(pathId, newPath, selfFileId, newVersion)

	# now handle the troves we include
        if them:
            troveSetDiff(self.strongTroves, them.strongTroves, False)
            troveSetDiff(self.weakTroves, them.weakTroves, True)
        else:
            troveSetDiff(self.strongTroves, None, False)
            troveSetDiff(self.weakTroves, None, True)

	added = {}
	removed = {}
        oldTroves = []

        for name, chgList in \
                chgSet.iterChangedTroves(strongRefs = True):
            for (how, version, flavor, byDefault) in chgList:
                if how == '+':
                    whichD = added
                elif how == '-':
                    whichD = removed
                    oldTroves.append((name, version, flavor))
                else:
                    continue

                d = whichD.setdefault(name, set())
                d.add((version, flavor))


        trvList = []
        if added or removed:
            if absolute:
                for name in added.keys():
                    for version, flavor in added[name]:
                        trvList.append((name, (None, None), (version, flavor),
                                        True))

            else:
                trvList = self._diffPackages(added, removed, getPathHashes)
        return (chgSet, filesNeeded, trvList)

    def _diffPackages(self, addedDict, removedDict, getPathHashes):
        """
            Matches up the list of troves that have been added to those
            that were removed.  Matches are done by name first,
            then by heuristics based on branches, flavors, and path hashes.
        """
        # NOTE: the matches are actually created by _makeMatch.
        # Most functions deal with lists that are (version, flavor)
        # lists, and have the name passed in as a separate argument.

        def _makeMatch(name, oldInfo, newInfo, trvList,
                       addedList, removedList):
            """ 
                NOTE: this function has side effects!
                Removes items from global addedDict and removedDict as
                necessary to avoid the info from being used in another
                match, also removes them from the local addedList and
                removedList for the same reason.

                Finally appends the match to trvList, the result that will
                be returned from _diffPackages
            """
            trvList.append((name, oldInfo, newInfo, False))
            if newInfo and newInfo[0]:
                if not addedList is addedDict[name]:
                    addedList.remove(newInfo)
                addedDict[name].remove(newInfo)
            if oldInfo and oldInfo[0]:
                if not removedList is removedDict[name]:
                    removedList.remove(oldInfo)
                removedDict[name].remove(oldInfo)

        def _getCompsByPackage(addedList, removedList):
            """
                Sorts components into addedByPackage, removedByPackage
                packageTup -> [packageTup, compTup, ...] lists.
            """
            # Collates the lists into dicts by package.
            # Returns dict of addedByPackage, removedByPackage.
            addedByPackage = {}
            removedByPackage = {}
            for troveD, packageD in ((addedList, addedByPackage),
                                     (removedList, removedByPackage)):
                for name in troveD:
                    if troveIsCollection(name):
                        packageName = name
                    else:
                        packageName = name.split(':')[0]

                    for version, flavor in troveD[name]:
                        l = packageD.setdefault((packageName, version, flavor),
                                                [])
                        l.append((name, version, flavor))
            return addedByPackage, removedByPackage

        def _getPathHashOverlaps(name, allAdded, allRemoved,
                                 addedByPackage, removedByPackage,
                                 getPathHashesFn):
            """
                Gets overlaps for path hashes for a particular name.
                allAdded, allRemoved are lists of (version, flavor) tuples
                for this name.
                If the name is for a collection, then the path hashes
                for that collection are based on all the components
                for that collection (that are in this update).
            """ 
            if troveIsCollection(name):
                newTroves = (addedByPackage[name, x[0], x[1]] for x in allAdded)
                newTroves = list(itertools.chain(*newTroves))
                oldTroves = (removedByPackage[name, x[0], x[1]]
                             for x in allRemoved)
                oldTroves = list(itertools.chain(*oldTroves))
            else:
                newTroves = [ (name, x[0], x[1]) for x in allAdded ]
                oldTroves = [ (name, x[0], x[1]) for x in allRemoved ]

            oldHashes = getPathHashesFn(oldTroves, old = True)
            oldHashes = dict(itertools.izip(oldTroves, oldHashes))
            newHashes = getPathHashesFn(newTroves)
            newHashes = dict(itertools.izip(newTroves, newHashes))

            overlaps = {}
            if troveIsCollection(name):
                for version, flavor in allAdded:
                    for component in addedByPackage[name, version, flavor]:
                        newPathHash = newHashes[component]
                        if newPathHash:
                            newHashes[name, version, flavor] |= newPathHash

                for version, flavor in allRemoved:
                    for component in removedByPackage[name, version, flavor]:
                        oldPathHash = oldHashes[component]
                        if oldPathHash:
                            oldHashes[name, version, flavor] |= oldPathHash

            for newInfo in newTroves:
                if newInfo[0] != name:
                    continue
                newHash = newHashes[newInfo]

                # we store overlaps by version, flavor not by  name, version, 
                # flavor
                newInfo = (newInfo[1], newInfo[2])
                for oldInfo in oldTroves:
                    if oldInfo[0] != name:
                        continue
                    oldHash = oldHashes[oldInfo]
                    oldInfo = (oldInfo[1], oldInfo[2])
                    if newHash & oldHash:
                        found = True
                        # just mark by version, flavor
                        overlaps.setdefault(newInfo, [])
                        overlaps[newInfo].append(oldInfo)
                        overlaps.setdefault(oldInfo, [])
                        overlaps[oldInfo].append(newInfo)
            return overlaps

        def _checkPathHashMatch(name, addedList, removedList, trvList,
                                pathHashMatches, flavorCache,
                                requireCompatible=False):
            """
                NOTE: This function may update addedList, removedList, and
                the global addedDict and removedDict.

                Finds matches for troves based on path hashes.  Also finds
                troves that match paths with other packages _outside_ of the
                given lists, and returns those lists as "delayed" troves - 
                troves that if possible should not be matched up within the
                given list.
            """
            delayedAdds = []
            delayedRemoves = []

            for added in list(addedList):
                removedPathMatches = pathHashMatches.get(added, None)
                if not removedPathMatches:
                    continue

                # filter by the list we were passed in - other matches
                # are not allowed at this time.
                inMatches = [ x for x in removedPathMatches if x in removedList]

                if requireCompatible:
                    # filter by flavor - only allow matches when the flavor
                    # is compatible
                    inMatches = [ x for x in inMatches
                                 if flavorCache.matches(x[1], added[1])]


                # matches that are _outside_ of this compatibility
                # check are a good reason to not pick a package if 
                # otherwise the selection would be arbitrary
                outMatches = [ x for x in removedPathMatches
                               if x not in inMatches ]
                assert(len(inMatches) + len(outMatches) \
                        == len(removedPathMatches))

                # find matches from the removed trove to other added troves.
                # Any such matches means we can't use the pathhash info 
                # reliably.
                reverseMatches = []
                for inMatch in inMatches:
                    addedPathMatches = pathHashMatches[inMatch]
                    reverseMatches = [ x for x in addedPathMatches
                                        if x in addedList ]
                    if requireCompatible:
                        reverseMatches = [ x for x in reverseMatches
                                     if flavorCache.matches(x[1], added[1]) ]

                if len(reverseMatches) > 1 or len(inMatches) > 1:
                    # we've got conflicting information about how
                    # to match up these troves.  Therefore, just don't
                    # use path hashes to match up.
                    continue
                elif len(inMatches) == 1:
                    removed = inMatches[0]
                    _makeMatch(name, removed, added, trvList,
                               addedList, removedList)
                    continue
                else:
                    # No in matches, but there is a match with a different
                    # flavor.  Delay this trove's match as long as possible.
                    delayedAdds.append(added)
            for removed in removedList:
                addedPathMatches = pathHashMatches.get(removed, None)
                if not addedPathMatches:
                    continue

                # filter by the list we were passed in - other matches
                # are not allowed at this time.
                inMatches = [ x for x in addedPathMatches if x in addedList]

                if requireCompatible:
                    # filter by flavor - only allow matches when the flavor
                    # is compatible
                    inMatches = [ x for x in inMatches
                                 if flavorCache.matches(x[1], removed[1])]
                if inMatches:
                    continue
                else:
                    delayedRemoves.append(removed)

            return delayedAdds, delayedRemoves

        def _matchByFlavorAndVersion(name, addedList, removedList, trvList,
                                     flavorCache, requireCompatible=False):
            """
                NOTE: This function has no side effects.
                Matches up addedList and removedList, first by flavor scoring,
                then by version scoring within that branch.

                If requireCompatible is True, then we don't perform any
                matches where the flavors have a NEG_INF value.

                returns matches in order of "goodness" - the first match
                is best, second match is second best, etc.
            """
            if not addedList or not removedList:
                return []
            addedByFlavor = {}
            removedByFlavor = {}
            for (version, flavor) in addedList:
                addedByFlavor.setdefault(flavor, []).append(version)
            for (version, flavor) in removedList:
                removedByFlavor.setdefault(flavor, []).append(version)

            changePair = []

            # score every new flavor against every old flavor 
            # to find the best match.  Doing anything less
            # may result in incorrect flavor lineups
            scoredValues = []
            for newFlavor in addedByFlavor:
                for oldFlavor in removedByFlavor:
                    score = scoreCache[oldFlavor, newFlavor]
                    if not requireCompatible or score > scoreCache.NEG_INF:
                        scoredValues.append((score, oldFlavor, newFlavor))
            scoredValues.sort(key = lambda x: x[0], reverse=True)

            # go through scored values in order, from highest to lowest,
            # picking off 
            for (score, oldFlavor, newFlavor) in scoredValues:
                if (newFlavor not in addedByFlavor 
                    or oldFlavor not in removedByFlavor):
                    continue
                newVersions = addedByFlavor.pop(newFlavor)
                oldVersions = removedByFlavor.pop(oldFlavor)
                changePair.append((newVersions,
                                   newFlavor, oldVersions, oldFlavor))

            matches = []
            # go through changePair and try and match things up by versions
            for (newVersionList, newFlavor, oldVersionList, oldFlavor) \
                                                            in changePair:
                oldInfoList = []
                for version in oldVersionList:
                    info = (version, oldFlavor)
                    if info in removedList:
                        oldInfoList.append(info)

                newInfoList = []
                for version in newVersionList:
                    info = (version, newFlavor)
                    if info in addedList:
                        newInfoList.append(info)

                versionMatches = _versionMatch(oldInfoList, newInfoList)

                for oldInfo, newInfo in versionMatches:
                    if not oldInfo[0] or not newInfo[0]:
                        # doesn't match anything in this flavor grouping
                        continue
                    matches.append((oldInfo, newInfo))

            return matches

        def _versionMatch(oldInfoSet, newInfoSet):
            # Match by version; use the closeness measure for items on
            # different branches and the timestamps for the same branch. If the
            # same version exists twice here, it means the flavors are
            # incompatible or tied; in either case the flavor won't help us
            # much.
            matches = []
            byBranch = {}
            # we need copies we can update
            oldInfoSet = set(oldInfoSet)
            newInfoSet = set(newInfoSet)

            for newInfo in newInfoSet:
                for oldInfo in oldInfoSet:
                    if newInfo[0].branch() == oldInfo[0].branch():
                        l = byBranch.setdefault(newInfo, [])
                        l.append(((oldInfo[0].trailingRevision(), oldInfo)))

            # pass 1, find things on the same branch
            for newInfo, oldInfoList in sorted(byBranch.items(), reverse=True):
                # take the newest (by timestamp) item from oldInfoList which
                # hasn't been matched to anything else 
                oldInfoList.sort(reverse=True)

                for revision, oldInfo in oldInfoList:
                    if oldInfo not in oldInfoSet: continue
                    matches.append((oldInfo, newInfo))
                    oldInfoSet.remove(oldInfo)
                    newInfoSet.remove(newInfo)
                    break

            del byBranch

            # pass 2, match across branches -- we know there is nothing left
            # on the same branch anymore
            scored = []
            for newInfo in newInfoSet:
                for oldInfo in oldInfoSet:
                    score = newInfo[0].closeness(oldInfo[0])
                    # score 0 are have nothing in common
                    if not score: continue
                    scored.append((score, oldInfo, newInfo))

            # high scores are better
            scored.sort()
            scored.reverse()
            for score, oldInfo, newInfo in scored:
                if oldInfo not in oldInfoSet: continue
                if newInfo not in newInfoSet: continue

                matches.append((oldInfo, newInfo))
                oldInfoSet.remove(oldInfo)
                newInfoSet.remove(newInfo)

            # the dregs are left. we do straight matching by timestamp here;
            # newest goes with newest, etc
            newList = []
            oldList = []
            for newInfo in newInfoSet:
                newList.append((newInfo[0].trailingRevision(), newInfo))
            newList.sort(reverse=True)

            for oldInfo in oldInfoSet:
                oldList.append((oldInfo[0].trailingRevision(), oldInfo))
            oldList.sort(reverse=True)

            for ((oldTs, oldInfo), (newTs, newInfo)) in \
                                            itertools.izip(oldList, newList):
                matches.append((oldInfo, newInfo))

            return matches

        def _matchList(name, addedList, removedList, trvList, pathHashOverlap,
                       scoreCache, requireCompatible=False):
            """
                NOTE: this function may remove items from addedList or
                removedList, and addedDict and removedDict, as matches are
                made.

                Called repeatedly with different pairs of addedLists
                and removedLists.  Checks for a good path hash match, or
                and then matches by flavor and version, prefering paths without
                other potential overlaps with other troves outside of this
                addedList and removedList.
            """
            if not addedList or not removedList:
                return []
            addDelays, removeDelays = _checkPathHashMatch(name, addedList, 
                                          removedList, 
                                          trvList,
                                          pathHashOverlap,
                                          scoreCache,
                                          requireCompatible=requireCompatible)
            # Now we go through a whole bunch of work to deal with this 
            # case:  two foo:runtimes, 1 and 2,  exist on branch a, but only
            # one new foo:runtime is being updated on branch a.  However,
            # a second foo:runtime on branch b that shares paths with a/1 
            # is also being added.
            # We match by branch first, so what we need to do is _delay_ 
            # matching a/1 if possible, that is prefer to match a/2 to match
            # with the new update on a.
            # In this case, a/1 will be in the removeDelays lists.
            # The algorithm could get even more complicated in this case:
            # We have 2 potential overlaps between a and b, but only
            # one slot for delays.  In that case, we just drop the delays
            # - better than trying to make the algorithm more complicated
            # by matching between all the troves, and then remove the worst 
            # matches that are also delayed troves and delay them, allowing 
            # them to match up with the trove we want (commented out code
            # to do that is below, just in case).

            newAddedList = addedList
            newRemovedList = removedList

            possibleDelays = abs(len(addedList) - len(removedList))
            if len(addedList) > len(removedList):
                # we have more new troves than old troves - we'll
                # be able to delay matching a new trove, but there's
                # no way to delay matching a remove trove - they all have
                # matches.
                removeDelays = []
                rematchesNeeded = max(len(addDelays) - possibleDelays, 0)
                if not rematchesNeeded:
                    newAddedList = [ x for x in addedList if x not in addDelays]
                # else bail - we don't have enough slots to delay
                # all the troves we want to delay.
            elif len(addedList) < len(removedList):
                addDelays = []
                rematchesNeeded = max(len(removeDelays) - possibleDelays, 0)
                if not rematchesNeeded:
                    newRemovedList = [ x for x in removedList 
                                       if x not in removeDelays]
                # else bail - we don't have enough slots to delay
                # all the troves we want to delay.

            # NOTE: turn off the algorithm below, which tries to handle 
            # dealing with only have one slot available for delays and
            # two troves to delay.  It's unlikely to occur and not worth
            # dealing with afaict.
            addDelays = removeDelays = []
            rematchesNeeded = 0

            matches = _matchByFlavorAndVersion(name, newAddedList,
                                           newRemovedList, trvList,
                                           scoreCache,
                                           requireCompatible=requireCompatible)
            assert(not rematchesNeeded)
            for oldInfo, newInfo in matches:
                _makeMatch(name, oldInfo, newInfo, trvList,
                           addedList, removedList)
            return
            # we've got a worst-case scenario here - too many troves match
            # troves on other labels.

            # sort from worst to best
            #badAdds = []
            #badRemoves = []
            #notFound = addDelays + removeDelays
            #for oldInfo, newInfo in reversed(matches):
            #    if oldInfo in removeDelays:
            #        notFound.remove(oldInfo)
            #        if rematchesNeeded > 0:
            #            badRemoves.append(oldInfo)
            #            rematchesNeeded -= 1
            #    elif newInfo in addDelays:
            #        notFound.remove(newInfo)
            #        if rematchesNeeded > 0:
            #            badAdds.append(newInfo)
            #            rematchesNeeded -= 1

            #if badAdds or badRemoves or notFound:
            #    changed = False
            #    if notFound:
            #        if addDelays:
            #            newAddedList = [ x for x in newAddedList
            #                             if x not in notFound ]
            #            badAdds = badAdds[:-(len(notFound))]
            #        else:
            #            newRemovedList = [ x for x in newRemovedList
            #                               if x not in notFound ]
            #            badRemoves = badRemoves[:-(len(notFound))]

            #    if badAdds:
            #        newAddedList = [ x for x in newAddedList
            #                         if x not in badAdds]
            #        changed = True
            #    else:
            #        newRemovedList = [ x for x in newRemovedList
            #                           if x not in badRemoves ]
            #        changed = True
            #    if changed:
            #        matches = _matchByFlavorAndVersion(name, newAddedList,
            #                             newRemovedList, trvList,
            #                             scoreCache,
            #                             requireCompatible=requireCompatible)
            #for oldInfo, newInfo in matches:
            #    _makeMatch(name, oldInfo, newInfo, trvList,
            #               addedList, removedList)

        ### def _diffPackages starts here.

        # use added and removed to assemble a list of trove diffs which need
        # to go along with this change set

        trvList = []

        for name in addedDict.keys():
            if not name in removedDict:
                # there isn't anything which disappeared that has the same
                # name; this must be a new addition
                for version, newFlavor in addedDict[name]:
                    trvList.append((name, (None, None), (version, newFlavor),
                                    False))
                del addedDict[name]
        for name in removedDict.keys():
            if not name in addedDict:
                # there isn't anything which disappeared that has the same
                # name; this must be a new addition
                for version, oldFlavor in removedDict[name]:
                    trvList.append((name, (version, oldFlavor), (None, None),
                                    False))
                del removedDict[name]
        if not addedDict and not removedDict:
            return trvList

        (addedByPackage,
         removedByPackage) = _getCompsByPackage(addedDict, removedDict)

        scoreCache = FlavorScoreCache()

        # match packages/groups first, then components that are not
        # matched as part of that.
        for name in addedDict:
            if name not in addedDict or name not in removedDict:
                continue

            if not addedDict[name] or not removedDict[name]:
                continue

            if getPathHashes:
                overlaps = _getPathHashOverlaps(name, 
                                                addedDict[name],
                                                removedDict[name],
                                                addedByPackage,
                                                removedByPackage,
                                                getPathHashes)
            else:
                overlaps = {}


            addedByBranch = {}
            removedByBranch = {}
            for version, flavor in addedDict[name]:
                addedByBranch.setdefault(version.branch(), []).append(
                                                            (version, flavor))
            for version, flavor in removedDict[name]:
                removedByBranch.setdefault(version.branch(), []).append(
                                                            (version, flavor))
            for branch, branchAdded in addedByBranch.iteritems():
                branchRemoved = removedByBranch.get(branch, [])
                if not branchRemoved:
                    continue
                # 1. match troves on the same branch with compatible flavors.
                _matchList(name, branchAdded, branchRemoved, trvList,
                           overlaps, scoreCache, requireCompatible=True)

            # 2. match troves with compatible flavors on different
            #    branches
            _matchList(name, addedDict[name], removedDict[name], trvList,
                       overlaps, scoreCache, requireCompatible=True)

            addedByBranch = {}
            removedByBranch = {}
            for version, flavor in addedDict[name]:
                addedByBranch.setdefault(version.branch(), []).append(
                                                            (version, flavor))
            for version, flavor in removedDict[name]:
                removedByBranch.setdefault(version.branch(), []).append(
                                               (version, flavor))

            for branch, branchAdded in addedByBranch.iteritems():
                branchRemoved = removedByBranch.get(branch, [])
                if not branchRemoved:
                    continue
                # 3. match troves on the same branch without compatible flavors.
                _matchList(name, branchAdded, branchRemoved, trvList,
                           overlaps, scoreCache, requireCompatible=False)

            # 4. match remaining troves.
            _matchList(name, addedDict[name], removedDict[name], trvList,
                       overlaps, scoreCache, requireCompatible=False)

        for name in removedDict:
            # these don't have additions to go with them
            for oldInfo in removedDict[name]:
                trvList.append((name, oldInfo, (None, None), False))

        for name in addedDict:
            # these don't have removals to go with them
            for newInfo in addedDict[name]:
                trvList.append((name, (None, None), newInfo, False))

        return trvList

    def setProvides(self, provides):
        self.provides.set(provides)

    def setRequires(self, requires):
        self.requires.set(requires)

    def getProvides(self):
        return self.provides()

    def getRequires(self):
        return self.requires()

    def getFlavor(self):
        return self.flavor()

    def getChangeLog(self):
        return self.changeLog

    def getTroveInfo(self):
        return self.troveInfo

    def getSize(self):
        return self.troveInfo.size()

    def setSize(self, sz):
        return self.troveInfo.size.set(sz)

    def getSourceName(self):
        return self.troveInfo.sourceName()

    def setSourceName(self, nm):
        return self.troveInfo.sourceName.set(nm)

    def getBuildTime(self):
        return self.troveInfo.buildTime()

    def setBuildTime(self, nm):
        return self.troveInfo.buildTime.set(nm)

    def getConaryVersion(self):
        return self.troveInfo.conaryVersion()

    def setConaryVersion(self, ver):
        return self.troveInfo.conaryVersion.set(ver)

    def setIsCollection(self, b):
        if b:
            return self.troveInfo.flags.isCollection(set = True)
        else:
            return self.troveInfo.flags.isCollection(set = False)

    def isCollection(self):
        return self.troveInfo.flags.isCollection()

    def setLabelPath(self, labelPath):
        self.troveInfo.labelPath = LabelPath()
        for label in labelPath:
            self.troveInfo.labelPath.set(str(label))

    def getLabelPath(self):
        return [ versions.Label(x) for x in self.troveInfo.labelPath ]

    def setBuildRequirements(self, itemList):
        for (name, ver, release) in itemList:
            self.troveInfo.buildReqs.add(name, ver, release)

    def getBuildRequirements(self):
        return [ (x[1].name(), x[1].version(), x[1].flavor()) 
                         for x in self.troveInfo.buildReqs.iterAll() ]

    def setPolicyProviders(self, itemList):
        for (name, ver, release) in itemList:
            self.troveInfo.policyProviders.add(name, ver, release)

    def getPolicyProviders(self):
        return [ (x[1].name(), x[1].version(), x[1].flavor()) 
                         for x in self.troveInfo.policyProviders.iterAll() ]

    def setLoadedTroves(self, itemList):
        for (name, ver, release) in itemList:
            self.troveInfo.loadedTroves.add(name, ver, release)

    def getLoadedTroves(self):
        return [ (x[1].name(), x[1].version(), x[1].flavor()) 
                 for x in self.troveInfo.loadedTroves.iterAll() ]

    def getPathHashes(self):
        return self.troveInfo.pathHashes

    def __init__(self, name, version = None, flavor = None, changeLog = None, 
                 type = TROVE_TYPE_NORMAL, skipIntegrityChecks = False,
                 setVersion = True):
        streams.StreamSet.__init__(self)

        if isinstance(name, AbstractTroveChangeSet):
            trvCs = name
            assert(not trvCs.getOldVersion())
            self.name.set(trvCs.getName())
            self.applyChangeSet(trvCs, skipIntegrityChecks = 
                                            skipIntegrityChecks)
        else:
            if name.count(':') > 1:
                raise TroveError, \
                            'More than one ":" is not allowed in a trove name'
            assert(flavor is not None)
            self.name.set(name)
            self.version.set(version)
            self.flavor.set(flavor)
            if setVersion:
                if type == TROVE_TYPE_REMOVED:
                    self.troveInfo.troveVersion.set(TROVE_VERSION_1_1)
                elif type == TROVE_TYPE_REDIRECT and name.startswith('group-'):
                    self.troveInfo.troveVersion.set(TROVE_VERSION_1_1)
                else:
                    self.troveInfo.troveVersion.set(TROVE_VERSION)
            self.troveInfo.incomplete.set(0)
            if changeLog:
                self.changeLog.thaw(changeLog.freeze())

            self.type.set(type)

class ReferencedTroveSet(dict, streams.InfoStream):

    def freeze(self, skipSet = {}):
	l = []
	for name, troveList in sorted(self.iteritems()):
	    subL = []
	    for (change, version, flavor, byDefault) in sorted(troveList):
		version = version.freeze()
		if flavor is None or flavor == deps.Flavor():
		    flavor = "-"
		else:
		    flavor = flavor.freeze()

		subL.append(change)
		subL.append(version)
		subL.append(flavor)
                if not byDefault:
                    subL.append('0')
                else:
                    subL.append('1')

	    l.append(name)
	    l += subL
	    l.append("")

	return "\0".join(l)

    def thaw(self, data):
	if not data: return
	self.clear()

	l = data.split("\0")
	i = 0

	while i < len(l):
	    name = l[i]
	    self[name] = []

	    i += 1
	    while l[i]:
		change = l[i]
		version = versions.ThawVersion(l[i + 1])
		flavor = l[i + 2]

		if flavor == "-":
		    flavor = deps.Flavor()
		else:
		    flavor = deps.ThawFlavor(flavor)

                if change == '-':
                    byDefault = None
                elif l[i + 3] == '0':
                    byDefault = False
                else:
                    byDefault = True

		self[name].append((change, version, flavor, byDefault))
		i += 4

	    i += 1

    def __init__(self, data = None):
	dict.__init__(self)
	if data is not None:
	    self.thaw(data)

class OldFileStream(list, streams.InfoStream):

    def freeze(self, skipSet = {}):
	return "".join(self)

    def thaw(self, data):
	i = 0
	del self[:]
	while i < len(data):
	    self.append(data[i:i+16])
	    i += 16
	assert(i == len(data))

    def __init__(self, data = None):
	list.__init__(self)
	if data is not None:
	    self.thaw(data)

class ReferencedFileList(list, streams.InfoStream):

    def freeze(self, skipSet = {}):
	l = []

	for (pathId, path, fileId, version) in self:
	    l.append(pathId)
	    if not path:
		path = ""

	    l.append(struct.pack("!H", len(path)))
	    l.append(path)

	    if not fileId:
		fileId = ""

	    l.append(struct.pack("!H", len(fileId)))
	    l.append(fileId)

	    if version:
		version = version.asString()
	    else:
		version = ""

	    l.append(struct.pack("!H", len(version)))
	    l.append(version)

	return "".join(l)

    def thaw(self, data):
	del self[:]

        # this lastVerStr check bypasses the normal version cache whenever
        # there are two sequential versions which are the same; this is a
        # massive speedup for troves with many files (90% or better)
        lastVerStr = None;

        i = 0
        while i < len(data):
            i, (pathId, path, fileId, verStr) = misc.unpack("!S16SHSHSH", i, 
                                                            data)
            if not path: path = None
            if not fileId: fileId = None

            if verStr == lastVerStr:
                version = lastVer
            elif verStr:
                version = versions.VersionFromString(verStr)
                lastVer = version
                lastVerStr = verStr
            else:
                version = None

	    self.append((pathId, path, fileId, version))

    def __init__(self, data = None):
	list.__init__(self)
	if data is not None:
	    self.thaw(data)

_STREAM_TCS_NAME	            =  0
_STREAM_TCS_OLD_VERSION	            =  1
_STREAM_TCS_NEW_VERSION	            =  2
_STREAM_TCS_REQUIRES	            =  3
_STREAM_TCS_PROVIDES	            =  4
_STREAM_TCS_CHANGE_LOG	            =  5
_STREAM_TCS_OLD_FILES	            =  6
_STREAM_TCS_TYPE	            =  7
_STREAM_TCS_STRONG_TROVE_CHANGES    =  8
_STREAM_TCS_NEW_FILES               =  9
_STREAM_TCS_CHG_FILES               = 10
_STREAM_TCS_OLD_FLAVOR              = 11
_STREAM_TCS_NEW_FLAVOR              = 12
_STREAM_TCS_TROVE_TYPE              = 13
_STREAM_TCS_TROVEINFO               = 14
_STREAM_TCS_OLD_SIGS                = 15
_STREAM_TCS_NEW_SIGS                = 16
_STREAM_TCS_WEAK_TROVE_CHANGES      = 17
_STREAM_TCS_REDIRECTS               = 18

_TCS_TYPE_ABSOLUTE = 1
_TCS_TYPE_RELATIVE = 2

class AbstractTroveChangeSet(streams.StreamSet):

    streamDict = { 
	_STREAM_TCS_NAME	: (SMALL, streams.StringStream, "name"       ),
        _STREAM_TCS_OLD_VERSION : (SMALL, FrozenVersionStream,  "oldVersion" ),
        _STREAM_TCS_NEW_VERSION : (SMALL, FrozenVersionStream,  "newVersion" ),
        _STREAM_TCS_REQUIRES    : (LARGE, DependenciesStream,   "requires"   ),
        _STREAM_TCS_PROVIDES    : (LARGE, DependenciesStream,   "provides"   ),
        _STREAM_TCS_CHANGE_LOG  : (LARGE, changelog.ChangeLog,  "changeLog"  ),
        _STREAM_TCS_OLD_FILES   : (LARGE, OldFileStream,        "oldFiles"   ),
        _STREAM_TCS_TYPE        : (SMALL, streams.IntStream,    "tcsType"    ),
        _STREAM_TCS_STRONG_TROVE_CHANGES:
                                  (LARGE, ReferencedTroveSet,   "strongTroves"),
        _STREAM_TCS_WEAK_TROVE_CHANGES:
                                  (LARGE, ReferencedTroveSet,   "weakTroves" ),
        _STREAM_TCS_NEW_FILES   : (LARGE, ReferencedFileList,   "newFiles"   ),
        _STREAM_TCS_CHG_FILES   : (LARGE, ReferencedFileList,   "changedFiles"),
        _STREAM_TCS_OLD_FLAVOR  : (SMALL, FlavorsStream,        "oldFlavor"  ),
        _STREAM_TCS_NEW_FLAVOR  : (SMALL, FlavorsStream,        "newFlavor"  ),
        _STREAM_TCS_TROVE_TYPE  : (SMALL, ByteStream,           "troveType" ),
        _STREAM_TCS_TROVEINFO   : (LARGE, streams.StringStream, "troveInfoDiff"),
        _STREAM_TCS_OLD_SIGS    : (LARGE, TroveSignatures,      "oldSigs"    ),
        _STREAM_TCS_NEW_SIGS    : (LARGE, TroveSignatures,      "newSigs"    ),
        _STREAM_TCS_REDIRECTS   : (LARGE, TroveRedirectList,    "redirects"  ),
    }

    ignoreUnknown = True

    """
    Represents the changes between two troves and forms part of a
    ChangeSet. 
    """

    def isAbsolute(self):
	return self.tcsType() == _TCS_TYPE_ABSOLUTE

    def newFile(self, pathId, path, fileId, version):
	self.newFiles.append((pathId, path, fileId, version))

    def getNewFileList(self):
	return self.newFiles

    def oldFile(self, pathId):
	self.oldFiles.append(pathId)

    def getOldFileList(self):
	return self.oldFiles

    def getName(self):
	return self.name()

    def getTroveInfoDiff(self):
        return self.troveInfoDiff()

    def getChangeLog(self):
	return self.changeLog

    def changeOldVersion(self, version):
	self.oldVersion.set(version)

    def changeChangeLog(self, cl):
        assert(0)
	self.changeLog.thaw(cl.freeze())

    def getOldVersion(self):
	return self.oldVersion()

    def getOldNameVersionFlavor(self):
        return self.name(), self.oldVersion(), self.oldFlavor()

    def getNewVersion(self):
	return self.newVersion()

    def getNewNameVersionFlavor(self):
        return self.name(), self.newVersion(), self.newFlavor()

    def __cmp__(self, other):
        first = self.name()
        second = other.name()

        if first == second:
            first = self.freeze()
            second = other.freeze()

        return cmp(first, second)

    def getOldSigs(self):
        return self.oldSigs

    def getNewSigs(self):
        return self.newSigs

    # path and/or version can be None
    def changedFile(self, pathId, path, fileId, version):
	self.changedFiles.append((pathId, path, fileId, version))

    def getChangedFileList(self):
	return self.changedFiles

    def hasChangedFiles(self):
        return (len(self.newFiles) + len(self.changedFiles) + 
                len(self.oldFiles)) != 0

    def iterChangedTroves(self, strongRefs = True, weakRefs = False):
        if strongRefs:
	    for x in self.strongTroves.iteritems():
                yield x

        if weakRefs:
	    for x in self.weakTroves.iteritems():
                yield x

    def newTroveVersion(self, name, version, flavor, byDefault, 
                        weakRef = False):
	"""
	Adds a version of a troves which appeared in newVersion.

	@param name: name of the trove
	@type name: str
	@param version: new version
	@type version: versions.Version
	@param flavor: new flavor
	@type flavor: deps.deps.Flavor
        @param byDefault: value of byDefault
        @param weakRef: is this a weak references?
        @type weakRef: boolean
        @type byDefault: boolean
	"""

        if weakRef:
            l = self.weakTroves.setdefault(name, [])
        else:
            l = self.strongTroves.setdefault(name, [])

	l.append(('+', version, flavor, byDefault))

    def oldTroveVersion(self, name, version, flavor, weakRef = False):
	"""
	Adds a version of a trove which appeared in oldVersion.

	@param name: name of the trove
	@type name: str
	@param version: old version
	@type version: versions.Version
	@param flavor: old flavor
	@type flavor: deps.deps.Flavor
        @param weakRef: is this a weak reference?
        @type weakRef: boolean
	"""

        if weakRef:
            l = self.weakTroves.setdefault(name, [])
        else:
            l = self.strongTroves.setdefault(name, [])

        l.append(('-', version, flavor, None))

    def changedTrove(self, name, version, flavor, byDefault, weakRef = False):
	"""
	Records the change in the byDefault setting of a referenced trove.

	@param name: name of the trove
	@type name: str
	@param version: version
	@type version: versions.Version
	@param flavor: flavor
	@type flavor: deps.deps.Flavor
        @param byDefault: New value of byDefault
        @type byDefault: boolean
        @param weakRef: is this a weak reference?
        @type weakRef: boolean
	"""
        if weakRef:
            l = self.weakTroves.setdefault(name, [])
        else:
            l = self.strongTroves.setdefault(name, [])

        l.append(('~', version, flavor, byDefault))

    def formatToFile(self, changeSet, f):
	f.write("%s " % self.getName())

	if self.isAbsolute():
	    f.write("absolute ")
	elif self.getOldVersion():
	    f.write("from %s to " % self.getOldVersion().asString())
	else:
	    f.write("new ")

	f.write("%s\n" % self.getNewVersion().asString())

        def depformat(name, dep, f):
            f.write('\t%s: %s\n' %(name,
                                   str(dep).replace('\n', '\n\t%s'
                                                    %(' '* (len(name)+2)))))
        if not self.getRequires().isEmpty():
            depformat('Requires', self.getRequires(), f)
        if not self.getProvides().isEmpty():
            depformat('Provides', self.getProvides(), f)
        if not self.getOldFlavor().isEmpty():
            depformat('Old Flavor', self.getOldFlavor(), f)
        if not self.getNewFlavor().isEmpty():
            depformat('New Flavor', self.getNewFlavor(), f)

	for (pathId, path, fileId, version) in self.newFiles:
	    #f.write("\tadded (%s(.*)%s)\n" % (pathId[:6], pathId[-6:]))
            change = changeSet.getFileChange(None, fileId)
            fileobj = files.ThawFile(change, pathId)
            
	    if isinstance(fileobj, files.SymbolicLink):
		name = "%s -> %s" % (path, fileobj.target())
	    else:
		name = path
	    
            f.write("\t%s    1 %-8s %-8s %s %s %s\n" % 
                    (fileobj.modeString(), fileobj.inode.owner(),
                     fileobj.inode.group(), fileobj.sizeString(),
                     fileobj.timeString(), name))

	for (pathId, path, fileId, version) in self.changedFiles:
	    pathIdStr = sha1helper.md5ToString(pathId)
	    if path:
		f.write("\tchanged %s (%s(.*)%s)\n" % 
			(path, pathIdStr[:6], pathIdStr[-6:]))
	    else:
		f.write("\tchanged %s\n" % pathIdStr)
	    oldFileId, change = changeSet._findFileChange(fileId)
	    f.write("\t\t%s\n" % " ".join(files.fieldsChanged(change)))

	for pathId in self.oldFiles:
	    pathIdStr = sha1helper.md5ToString(pathId)
	    f.write("\tremoved %s(.*)%s\n" % (pathIdStr[:6], pathIdStr[-6:]))

	for name in self.strongTroves.keys():
            l = []
            for x in self.strongTroves[name]:
                l.append(x[0] + x[1].asString())
                if x[3] is None:
                    l[-1] += ' (None)'
                elif x[3]:
                    l[-1] += ' (True)'
                else:
                    l[-1] += ' (False)'
	    f.write("\t" + name + " " + " ".join(l) + "\n")

    def setProvides(self, provides):
	self.provides.set(provides)

    def getType(self):
        return self.troveType()

    def setRedirects(self, redirs):
        self.redirects = redirs.copy()

    def getRedirects(self):
        return self.redirects

    def getProvides(self):
        return self.provides()

    def setRequires(self, requires):
	self.requires.set(requires)

    def getRequires(self):
        return self.requires()

    def getOldFlavor(self):
        return self.oldFlavor()

    def getNewFlavor(self):
        return self.newFlavor()

    def getNewPathHashes(self):
        assert(self.oldVersion() is None)
        return TroveInfo.find(_TROVEINFO_TAG_PATH_HASHES, self.troveInfoDiff())

class TroveChangeSet(AbstractTroveChangeSet):

    def __init__(self, name, changeLog, oldVersion, newVersion, 
		 oldFlavor, newFlavor, oldSigs, newSigs,
                 absolute = 0, type = TROVE_TYPE_NORMAL,
                 troveInfoDiff = None):
	AbstractTroveChangeSet.__init__(self)
	assert(isinstance(newVersion, versions.AbstractVersion))
	assert(isinstance(newFlavor, deps.Flavor))
	assert(oldFlavor is None or isinstance(oldFlavor, deps.Flavor))
	self.name.set(name)
	self.oldVersion.set(oldVersion)
	self.newVersion.set(newVersion)
	if changeLog:
	    self.changeLog = changeLog
	if absolute:
	    self.tcsType.set(_TCS_TYPE_ABSOLUTE)
	else:
	    self.tcsType.set(_TCS_TYPE_RELATIVE)
        if oldVersion is not None:
            self.oldFlavor.set(oldFlavor)
	self.newFlavor.set(newFlavor)
        self.troveType.set(type)
        assert(troveInfoDiff is not None)
        self.troveInfoDiff.set(troveInfoDiff)
        if oldSigs:
            self.oldSigs.thaw(oldSigs.freeze())
        self.newSigs.thaw(newSigs.freeze())

class ThawTroveChangeSet(AbstractTroveChangeSet):

    def __init__(self, buf):
	AbstractTroveChangeSet.__init__(self, buf)


class FlavorScoreCache(object):
    def __init__(self):
        self.cache = {}
        self.NEG_INF = -9999
        self.POS_INF = 9999

    def matches(self, oldFlavor, newFlavor):
        return (self[oldFlavor, newFlavor] > self.NEG_INF)

    def __getitem__(self, (oldFlavor, newFlavor)):
        # check for superset matching and subset
        # matching.  Currently we don't consider 
        # a superset flavor match "better" than 
        # a subset - if we want to change that, 
        # a initial parameter for maxScore that 
        # ordered scores by type would work.
        # If we do that, we should consider adding
        # heuristic to prefer strongly satisfied
        # flavors most of all. 
        if not (oldFlavor, newFlavor) in self.cache:
            if oldFlavor.isEmpty() and newFlavor.isEmpty():
                myMax = self.POS_INF
            else:
                scores = (self.NEG_INF, newFlavor.score(oldFlavor),
                          oldFlavor.score(newFlavor))
                myMax = max(x for x in scores if x is not False)
            self.cache[oldFlavor, newFlavor] = myMax
            self.cache[newFlavor, oldFlavor] = myMax
            return myMax
        else:
            return self.cache[oldFlavor, newFlavor]

class TroveError(errors.ConaryError):

    """
    Ancestor for all exceptions raised by the trove module.
    """

    pass

class ParseError(TroveError):

    """
    Indicates that an error occurred parsing a group file.
    """

    pass

class PatchError(TroveError):

    """
    Indicates that an error occurred parsing a group file.
    """

    pass

class DigitalSignatureVerificationError(TroveError):
    """
    Indicates that a digital signature did not verify.
    """
    def __str__(self):
        return self.message

    def __init__(self, message):
        self.message = message

class TroveIntegrityError(TroveError):
    """
    Indicates that a checksum did not match
    """
    _error = "Trove Integrity Error: %s=%s[%s] checksum does not match precalculated value"
    def __init__(self, name=None, version=None, flavor=None, error=None):
        if name:
            self.nvf = (name, version, flavor)
            if error is None:
                error = self._error % self.nvf
        else:
            self.nvf = None


        TroveError.__init__(self, error)
