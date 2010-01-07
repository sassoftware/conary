#
# Copyright (c) 2004-2009 rPath, Inc.
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
Implements troves (packages, components, etc.) for the repository
"""

import itertools, os
import re
import struct

from conary import changelog
from conary import errors
from conary import files
from conary import rpmhelper
from conary import streams
from conary import versions
from conary.deps import deps
from conary.lib import misc, sha1helper, api
from conary.lib.openpgpfile import KeyNotFound, TRUST_UNTRUSTED, TRUST_TRUSTED
from conary.lib import openpgpkey
from conary.streams import ByteStream
from conary.streams import DependenciesStream, FlavorsStream
from conary.streams import FrozenVersionStream
from conary.streams import SMALL, LARGE, DYNAMIC
from conary.streams import OptionalFlavorStream
from conary.streams import StringVersionStream

TROVE_VERSION=10
# the difference between 10 and 11 is that the REMOVED type appeared, 
# and we allow group redirects; 11 is used *only* for those situations
TROVE_VERSION_1_1=11

# files with this magic pathId are capsules
CAPSULE_PATHID = '\0' * 16

@api.developerApi
def troveIsCollection(troveName):
    return not(":" in troveName or troveName.startswith("fileset-"))

@api.developerApi
def troveIsPackage(troveName):
    return troveIsCollection(troveName) and not troveName.startswith('group-')

@api.developerApi
def troveIsGroup(troveName):
    return troveIsCollection(troveName) and troveName.startswith('group-')

@api.developerApi
def troveIsComponent(troveName):
    return ":" in troveName

@api.developerApi
def troveIsSourceComponent(troveName):
    return troveName.endswith(':source')

@api.developerApi
def troveIsFileSet(troveName):
    return (troveName.startswith('fileset-')
            and not troveName.endswith(':source'))

@api.developerApi
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

    def asTuple(self):
        return self.name(), self.version(), self.flavor()

    def __hash__(self):
        return hash(self.asTuple())

class TroveMtimes(list, streams.InfoStream):

    def thaw(self, frz):
        del self[:]
        count = len(frz) / 4
        self.extend(struct.unpack("!" + ("I" * count), frz))

    def freeze(self, skipSet = None):
        count = len(self)
        return struct.pack("!" + ("I" * count), *self)

    def diff(self, other):
        # absolute diff. gross but easy
        return self.freeze()

    def twm(self, diff, base):
        assert(self == base)
        self.thaw(diff)

    def __eq__(self, other, skipSet = None):
        return list.__eq__(self, other)

    def __init__(self, frz = None):
        if frz:
            self.thaw(frz)

class TroveTupleList(streams.StreamCollection):
    streamDict = { 1 : TroveTuple }
    ignoreSkipSet = True

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

class VersionListStream(streams.OrderedStreamCollection):
    streamDict = { 1 : StringVersionStream }
    ignoreSkipSet = True

    def append(self, ver):
        v = streams.StringVersionStream()
        v.set(ver)
        self.addStream(1, v)

    def __iter__(self):
        return ( x[1]() for x in self.iterAll() )

class SearchPathItem(TroveTuple):
    _SEARCH_PATH_LABEL  = 10
    streamDict = TroveTuple.streamDict.copy()
    streamDict[_SEARCH_PATH_LABEL] =  (SMALL, streams.StringStream, 'label'  )

    def __cmp__(self, other):
        first = self.name()
        second = other.name()

        if first == second:
            first = self.freeze()
            second = other.freeze()

        return cmp(first, second)

    def get(self):
        if self.label():
            return versions.Label(self.label())
        else:
            return (self.name(),
                    self.version(),
                    self.flavor())

    def __hash__(self):
        return hash((self.name(), self.version(), self.flavor(), self.label()))

class SearchPath(TroveTupleList):
    streamDict = { 1 : SearchPathItem }

    def add(self, item):
        dep = SearchPathItem()
        if isinstance(item, versions.Label):
            dep.label.set(str(item))
        else:
            name, version, flavor = item
            dep.name.set(name)
            dep.version.set(version)
            dep.flavor.set(flavor)
        self.addStream(1, dep)

    def iter(self):
        return ( x[1].get() for x in self.iterAll() )

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
    ignoreSkipSet = True

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

class TroveCopiedFrom(TroveTupleList):
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

    def __cmp__(self, other):
        return cmp((self.fingerprint, self.signature, self.timestamp),
                   (other.fingerprint, other.signature, other.timestamp))

    def _mpiToLong(self, data):
        length = ((ord(data[0]) << 8) + ord(data[1]) + 7) / 8
        if len(data) != length + 2:
            raise IndexError

        frontSize = length & ~3

        r = 0L
        ints = struct.unpack("!" + "I" * (frontSize / 4), data[2:2 + frontSize])
        for i in ints:
            r <<= 32;
            r += i

        for i in range(2 + frontSize, 2 + length):
            r <<= 8
            r += ord(data[i])

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
            try:
                lengthMPI = ((ord(data[index]) * 256) +
                             (ord(data[index + 1]) + 7)) / 8 + 2
                mpiList.append(self._mpiToLong(data[index:index + lengthMPI]))
            except IndexError:
                # handle truncated signature data by setting this MPI to 0
                mpiList.append(0L)
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

    def __iter__(self):
        return self.iter()

    def sign(self, digest, keyId):
        keyCache = openpgpkey.getKeyCache()
        key = keyCache.getPrivateKey(keyId)
        sig = key.signString(digest)
        self.add(sig)

    # this function is for convenience. It reduces code duplication in
    # netclient and netauth (since I need to pass the frozen form thru
    # xmlrpc)
    def getSignature(self, keyId):
        for sig in self.iter():
            if keyId in sig[0]:
                return sig
        raise KeyNotFound('Signature by key: %s does not exist' %keyId)

_VERSIONED_DIGITAL_SIGNATURES_VERSION = 0
_VERSIONED_DIGITAL_SIGNATURES_DIGEST   = 1
_VERSIONED_DIGITAL_SIGNATURES_DIGSIGS = 2

class VersionedDigitalSignatures(streams.StreamSet):
    streamDict = {
        _VERSIONED_DIGITAL_SIGNATURES_VERSION:
                (SMALL, streams.ByteStream,    'version'   ),
        _VERSIONED_DIGITAL_SIGNATURES_DIGEST:
                (SMALL, streams.StringStream,  'digest'   ),
        _VERSIONED_DIGITAL_SIGNATURES_DIGSIGS:
                (SMALL, DigitalSignatures,     'signatures'),
    }

    def __cmp__(self, other):
        return cmp((self.version, self.digest, self.signatures),
                   (other.version, other.digest, other.signatures))

    def addPrecomputed(self, sig):
        self.signatures.add(sig)

class VersionedSignaturesSet(streams.StreamCollection):
    streamDict = { 1 : VersionedDigitalSignatures }

    def sign(self, keyId, version=0):
        for vds in self.getStreams(1):
            if version == vds.version():
                digest = vds.digest()
                vds.signatures.sign(digest, keyId)
                return

        raise KeyError(version)

    def getSignatures(self, version = 0):
        for vds in self.getStreams(1):
            if version == vds.version():
                return vds
        return None

    def getDigest(self, version = 0):
        sigs = self.getSignatures(version)
        if sigs:
            return sigs.digest()
        return None

    def addDigest(self, digest, version = 0):
        vds = VersionedDigitalSignatures()
        vds.version.set(version)
        vds.digest.set(digest)
        self.addStream(1, vds)

    def addPrecomputedSignature(self, sig, version = 0):
        for vds in self.getStreams(1):
            if version == vds.version():
                vds.addPrecomputed(sig)
                return

        raise KeyNotFound

    def clear(self):
        self.getStreams(1).clear()

    def __iter__(self):
        for item in self.getStreams(1):
            yield item

_TROVESIG_SHA1   = 0
_TROVESIG_DIGSIG = 1
_TROVESIG_VSIG   = 2

_TROVESIG_VER_CLASSIC = 0
_TROVESIG_VER_NEW     = 1
# NEW2 is exactly the same as NEW -- it is only used if more than one
# trove script compatibility class is in this trove.  The sort order
# was broken in older versions of conary. CNY-2997
_TROVESIG_VER_NEW2    = 2
_TROVESIG_VER_ALL = [ 0, 1, 2 ]

class TroveSignatures(streams.StreamSet):
    """
    sha1 and digitalSigs are "classic" signatures; they include information
    included with conary < 1.1.19. vSigs are versioned digital signatures,
    which allows multiple signing schemes. The "classic" signatures are
    considered version 0, and this object hides the different storage method
    for those signatures.
    """

    ignoreUnknown = True
    streamDict = {
        _TROVESIG_SHA1      : ( SMALL, streams.AbsoluteSha1Stream, 'sha1'   ),
        _TROVESIG_DIGSIG    : ( LARGE, DigitalSignatures,          'digitalSigs' ),
        _TROVESIG_VSIG      : ( LARGE, VersionedSignaturesSet,     'vSigs' ),
    }

    # this code needs to be called any time we're making a derived
    # trove esp. shadows. since some info in the trove gets changed
    # we cannot allow the signatures to persist.
    def reset(self):
        self.digitalSigs = DigitalSignatures()
        self.sha1 = streams.AbsoluteSha1Stream()
        self.vSigs = VersionedSignaturesSet()

    # this parameter order has to match what's used in streamset.c
    def freeze(self, skipSet = {}, freezeKnown = True, freezeUnknown = True):
        return streams.StreamSet.freeze(self, skipSet = skipSet,
                                        freezeKnown = freezeKnown,
                                        freezeUnknown = freezeUnknown)

    def computeDigest(self, sigVersion, message):
        if sigVersion == _TROVESIG_VER_CLASSIC:
            self.sha1.compute(message)
            return

        if sigVersion in (_TROVESIG_VER_NEW, _TROVESIG_VER_NEW2):
            digest = streams.NonStandardSha256Stream()
            digest.compute(message)
        else:
            raise NotImplementedError

        for versionedBlock in self.vSigs:
            if versionedBlock.version() != sigVersion:
                continue

            versionedBlock.digest.set(digest())
            return

        self.vSigs.addDigest(digest(), version = sigVersion)

    def sign(self, keyId):
        """
        Ensure every digest has been signed by the given keyId. If signatures
        are missing, they are generated. Existing signatures are not validated.
        """

        self.digitalSigs.sign(self.sha1(), keyId)

        versionList = set()
        signedVersions = set()
        for versionedBlock in self.vSigs:
            versionList.add(versionedBlock.version())
            for sig in versionedBlock.signatures:
                if sig[0].endswith(keyId):
                    signedVersions.add(versionedBlock.version())
                    break

        for sigVersion in (versionList - signedVersions):
            self.vSigs.sign(keyId, version = sigVersion)

    def addPrecomputedSignature(self, sigVersion, sig):
        if sigVersion == _TROVESIG_VER_CLASSIC:
            self.digitalSigs.add(sig)
            return

        self.vSigs.addPrecomputedSignature(sig, version = sigVersion)

    def __iter__(self):
        """
        Iterates over all signatures in this block. Signatures are returned as
        (version, digest, signature) tuples. Each digest is also returned as
        (digest, None) before any signatures for that digest. The digests
        are returned as digets stream objects.
        """
        if self.sha1():
            yield (_TROVESIG_VER_CLASSIC, self.sha1, None)

        for sig in self.digitalSigs:
            yield (_TROVESIG_VER_CLASSIC, self.sha1, sig)

        for versionedBlock in self.vSigs:
            ver = versionedBlock.version()
            if ver in (_TROVESIG_VER_NEW, _TROVESIG_VER_NEW2):
                digest = streams.NonStandardSha256Stream()
            else:
                # Ignore digest types we don't know about.
                continue

            digest.set(versionedBlock.digest())

            yield (versionedBlock.version(), digest, None)

            for sig in versionedBlock.signatures:
                yield (versionedBlock.version(), digest, sig)

_TROVE_FLAG_ISCOLLECTION = 1 << 0
_TROVE_FLAG_ISDERIVED    = 1 << 1
_TROVE_FLAG_ISMISSING    = 1 << 2

class TroveFlagsStream(streams.ByteStream):

    def isCollection(self, set = None):
	return self._isFlag(_TROVE_FLAG_ISCOLLECTION, set)

    def isDerived(self, set = None):
	return self._isFlag(_TROVE_FLAG_ISDERIVED, set)

    def isMissing(self, set = None):
	return self._isFlag(_TROVE_FLAG_ISMISSING, set)

    def _isFlag(self, flag, set):
	if set != None:
            if self() is None:
                self.set(0x0)
	    if set:
		self.set(self() | flag)
	    else:
		self.set(self() & ~(flag))

	return (self() and self() & flag)


# FIXME: this should be a dynamically extendable stream.  StreamSet is a
# little too rigid.
_METADATA_ITEM_TAG_ID = 0
_METADATA_ITEM_TAG_SHORTDESC = 1
_METADATA_ITEM_TAG_LONGDESC = 2
_METADATA_ITEM_TAG_LICENSES = 3
_METADATA_ITEM_TAG_CRYPTO = 4
_METADATA_ITEM_TAG_URL = 5
_METADATA_ITEM_TAG_CATEGORIES = 6
_METADATA_ITEM_TAG_BIBLIOGRAPHY = 7
_METADATA_ITEM_TAG_OLD_SIGNATURES = 8
_METADATA_ITEM_TAG_NOTES = 9
_METADATA_ITEM_TAG_LANGUAGE = 10
_METADATA_ITEM_ORIG_ITEMS = 10
_METADATA_ITEM_TAG_KEY_VALUE = 11
_METADATA_ITEM_TAG_NEW_SIGNATURES = 12

_METADATA_ITEM_SIG_VER_ALL = [ 0, 1 ]

class KeyValueItemsStream(streams.OrderedStreamCollection):
    __slots__ = [ '_map' ]

    streamDict = {
        1 : streams.StringStream,
        2 : streams.StringStream,
    }

    def __init__(self, data = None):
        self._map = {}
        streams.OrderedStreamCollection.__init__(self, data = data)

    # Dictionary interface
    def __setitem__(self, key, value):
        assert(isinstance(key, str))
        assert(isinstance(value, str))
        self._map[key] = value

    def __getitem__(self, key):
        if self._data is not None:
            self._thaw()
        return self._map[key]

    def keys(self):
        if self._data is not None:
            self._thaw()
        return self._map.keys()

    def items(self):
        if self._data is not None:
            self._thaw()
        return self._map.items()

    def update(self, ddata):
        self._map.update(ddata)

    def iteritems(self):
        if self._data is not None:
            self._thaw()
        return self._map.iteritems()

    def freeze(self, skipSet = None):
        self._reset()

        for key, val in sorted(self._map.iteritems()):
            self.addStream(1, streams.StringStream(key))
            self.addStream(2, streams.StringStream(val))

        self._data = None
        return streams.OrderedStreamCollection.freeze(self, skipSet = skipSet)

    def _thaw(self):
        streams.OrderedStreamCollection._thaw(self)
        keys = self.getStreams(1)
        vals = self.getStreams(2)
        self._map = dict((x(), y()) for (x, y) in zip(keys, vals))
        self._reset()

    def _reset(self):
        for k, v in self.getItems().iteritems():
            del v[:]

OBSS = streams.OrderedBinaryStringsStream
class MetadataItem(streams.StreamSet):
    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _METADATA_ITEM_TAG_ID:
                (DYNAMIC, streams.StringStream,   'id'           ),
        _METADATA_ITEM_TAG_SHORTDESC:
                (DYNAMIC, streams.StringStream,   'shortDesc'    ),
        _METADATA_ITEM_TAG_LONGDESC:
                (DYNAMIC, streams.StringStream,   'longDesc'     ),
        _METADATA_ITEM_TAG_LICENSES:
                (DYNAMIC, OBSS,                   'licenses'     ),
        _METADATA_ITEM_TAG_CRYPTO:
                (DYNAMIC, OBSS,                   'crypto'       ),
        _METADATA_ITEM_TAG_URL:
                (DYNAMIC, streams.StringStream,   'url'          ),
        _METADATA_ITEM_TAG_LANGUAGE:
                (DYNAMIC, streams.StringStream,   'language'     ),
        _METADATA_ITEM_TAG_CATEGORIES:
                (DYNAMIC, OBSS,                   'categories'   ),
        _METADATA_ITEM_TAG_BIBLIOGRAPHY:
                (DYNAMIC, OBSS,                   'bibliography' ),
        _METADATA_ITEM_TAG_OLD_SIGNATURES:
                (DYNAMIC, VersionedSignaturesSet, 'oldSignatures'),
        _METADATA_ITEM_TAG_NOTES:
                (DYNAMIC, OBSS,                   'notes'        ),
        _METADATA_ITEM_TAG_KEY_VALUE:
                (DYNAMIC, KeyValueItemsStream,    'keyValue'     ),
        _METADATA_ITEM_TAG_NEW_SIGNATURES:
                (DYNAMIC, VersionedSignaturesSet, 'signatures'   ),
        }

    _skipSet = { 'id' : True, 'signatures': True, 'oldSignatures' : True }
    _keys = [ x[2] for x in streamDict.itervalues() if x[2] not in _skipSet ]

    def _digest(self, version=0):
        # version 0 is stored in old signatures, and doesn't include any
        # extended metadata
        #
        # version 1 is stored in new signatures, and includes extended
        # metadata
        if version > 1:
            return None

        if version == 0:
            # version 0 of the digest
            skip = self._skipSet.copy()
            skip.update((x[1][2], True) for x in self.streamDict.items() if
                            x[0] > _METADATA_ITEM_ORIG_ITEMS)
            frz = streams.StreamSet.freeze(self, skipSet = skip,
                                           freezeUnknown = False)
        else:
            frz = streams.StreamSet.freeze(self, skipSet = self._skipSet)

        digest = streams.NonStandardSha256Stream()
        digest.compute(frz)
        return digest()

    def _updateId(self):
        frz = streams.StreamSet.freeze(self, self._skipSet)
        self.id.set(sha1helper.sha1String(frz))

    def _updateDigests(self):
        self._updateId()
        skip = dict(
            (x[1][2], True) for x in self.streamDict.items() if
                        x[0] <= _METADATA_ITEM_ORIG_ITEMS)
        newFrz = self.freeze(skipSet = skip)

        for version in _METADATA_ITEM_SIG_VER_ALL:
            if version == 0:
                if not self.oldSignatures.getDigest(version):
                    self.oldSignatures.addDigest(self._digest(version), version)
            else:
                if not self.signatures.getDigest(version) and newFrz:
                    self.signatures.addDigest(self._digest(version), version)

    def computeDigests(self):
        self._updateDigests()

    def addDigitalSignature(self, keyId, version=0):
        self._updateDigests()
        if version == 0:
            self.oldSignatures.sign(keyId, version)
        else:
            self.signatures.sign(keyId, version)

    def clearDigitalSignatures(self):
        self.oldSignatures.clear()
        self.signatures.clear()

    def verifyDigitalSignatures(self, label=None):
        keyCache = openpgpkey.getKeyCache()
        missingKeys = []
        badFingerprints = []
        untrustedKeys = set()
        for signatures in itertools.chain(self.oldSignatures, self.signatures):
            # verify that recomputing the digest for this version
            # of the signature matches the stored version
            digest = self._digest(signatures.version())
            if digest is None:
                # unknown signature version
                continue
            elif digest != signatures.digest():
                raise DigitalSignatureVerificationError(
                    'metadata checksum does not match stored value')

            for signature in signatures.signatures:
                try:
                    key = keyCache.getPublicKey(signature[0],
                                                label,
                                                warn=False)
                except KeyNotFound:
                    missingKeys.append(signature[0])
                    continue
                lev = key.verifyString(digest, signature)
                if lev == -1:
                    badFingerprints.append(key.getFingerprint())
                elif lev < TRUST_TRUSTED:
                    untrustedKeys.add(key.getFingerprint())
        return missingKeys, badFingerprints, untrustedKeys

    def verifyDigests(self):
        for signatures in itertools.chain(self.oldSignatures, self.signatures):
            # verify that recomputing the digest for this version
            # of the signature matches the stored version
            digest = self._digest(signatures.version())
            if digest is None:
                # unknown signature version
                continue
            elif digest != signatures.digest():
                raise DigitalSignatureVerificationError(
                    'metadata checksum does not match stored value')

        return True

    def freeze(self, *args, **kw):
        return streams.StreamSet.freeze(self, *args, **kw)

    def keys(self):
        ret = []
        for x in self._keys:
            attr = getattr(self, x)
            if hasattr(attr, 'keys'):
                if attr.keys():
                    ret.append(x)
            elif hasattr(attr, '__call__') and attr():
                ret.append(x)
        return ret

    def __getitem__(self, key):
        attr = getattr(self, key)
        if hasattr(attr, '__call__'):
            return attr()
        return attr

class Metadata(streams.OrderedStreamCollection):
    streamDict = { 1: MetadataItem }

    def addItem(self, item):
        self.addStream(1, item)

    def addItems(self, items):
        for item in items:
            self.addItem(item)

    def __iter__(self):
        for item in self.getStreams(1):
            yield item

    def computeDigests(self):
        for item in self:
            item.computeDigests()

    def _replaceAll(self, new):
        self._items[1] = new._items[1]

    def get(self, language=None):
        d = dict.fromkeys(MetadataItem._keys)
        for item in self.getStreams(1):
            if not item.language():
                d.update(item)
        if language is not None:
            for item in self.getStreams(1):
                if item.language() == language:
                    d.update(item)
        return d

    def flatten(self, skipSet=None, filteredKeyValues=None):
        if skipSet is None:
            skipSet = []
        if filteredKeyValues is None:
            filteredKeyValues = []
        items = {}
        keys = MetadataItem._keys
        for item in self.getStreams(1):
            language = item.language()
            newItem = items.setdefault(language, MetadataItem())
            for key in item.keys():
                if key in skipSet:
                    continue
                newItemStream = getattr(newItem, key)
                if key == 'keyValue':
                    # Key-value metadata
                    for k, v in item.keyValue.items():
                        if k not in filteredKeyValues:
                            newItemStream[k] = v
                    continue
                values = getattr(item, key)()
                if isinstance(newItemStream, list):
                    # We have to clear the old list before adding new elements
                    # to it, otherwise we end up with a union of the lists,
                    # which is not what we want
                    del newItemStream[:]
                if not isinstance(values, (list, tuple)):
                    values = [values]
                for value in values:
                    newItemStream.set(value)
        return items.values()

    def verifyDigitalSignatures(self, label=None):
        missingKeys = []
        badFingerprints = []
        untrustedKeys = set()
        for item in self:
            rc = item.verifyDigitalSignatures(label=label)
            missingKeys.extend(rc[0])
            badFingerprints.extend(rc[1])
            untrustedKeys.update(rc[2])
        return missingKeys, badFingerprints, untrustedKeys

    def verifyDigests(self):
        for item in self:
            item.verifyDigests()

        return True

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
_TROVEINFO_ORIGINAL_SIG       = _TROVEINFO_TAG_INCOMPLETE
# troveinfo above here is signed in v0 signatures; below here is signed
# in v1 signatures as well
_TROVEINFO_TAG_DIR_HASHES     = 15
_TROVEINFO_TAG_SCRIPTS        = 16
_TROVEINFO_TAG_METADATA       = 17
_TROVEINFO_TAG_COMPLETEFIXUP  = 18  # indicates that this trove went through 
                                    # a fix for incompleteness. only used on
                                    # the client, and left out of frozen forms
                                    # normally (since it should always be None)
_TROVEINFO_TAG_COMPAT_CLASS   = 19
# items added below this point must be DYNAMIC for proper unknown troveinfo
# handling
_TROVEINFO_TAG_BUILD_FLAVOR   = 20
_TROVEINFO_TAG_COPIED_FROM    = 21
_TROVEINFO_TAG_IMAGE_GROUP    = 22
_TROVEINFO_TAG_FACTORY        = 23
_TROVEINFO_TAG_SEARCH_PATH    = 24
_TROVEINFO_TAG_DERIVEDFROM    = 25
_TROVEINFO_TAG_PKGCREATORDATA = 26
_TROVEINFO_TAG_CLONEDFROMLIST = 27
_TROVEINFO_TAG_CAPSULE        = 28
_TROVEINFO_TAG_MTIMES         = 29
_TROVEINFO_TAG_LAST           = 29

_TROVECAPSULE_TYPE            = 0
_TROVECAPSULE_RPM             = 1
_TROVECAPSULE_TYPE_CONARY     = ''
_TROVECAPSULE_TYPE_RPM        = 'rpm'

_TROVECAPSULE_RPM_NAME        = 0
_TROVECAPSULE_RPM_VERSION     = 1
_TROVECAPSULE_RPM_RELEASE     = 2
_TROVECAPSULE_RPM_ARCH        = 3
_TROVECAPSULE_RPM_EPOCH       = 4
_TROVECAPSULE_RPM_OBSOLETES   = 5

_RPM_OBSOLETE_NAME    = 0
_RPM_OBSOLETE_FLAGS   = 1
_RPM_OBSOLETE_VERSION = 2

class SingleRpmObsolete(streams.StreamSet):

    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _RPM_OBSOLETE_NAME    : (DYNAMIC, streams.StringStream, 'name' ),
        _RPM_OBSOLETE_FLAGS   : (DYNAMIC, streams.IntStream,    'flags' ),
        _RPM_OBSOLETE_VERSION : (DYNAMIC, streams.StringStream, 'version' )

    }

    def __cmp__(self, other):
        first = self.name()
        second = other.name()

        if first == second:
            first = self.freeze()
            second = other.freeze()

        return cmp(first, second)

class RpmObsoletes(streams.StreamCollection):
    streamDict = { 1 : SingleRpmObsolete }

    def addFromHeader(self, h):
        if rpmhelper.OBSOLETENAME not in h:
            # really, really, REALLY old RPM packages might not have
            # OBSOLETEFLAGS or OBSOLETEVERSION. I doubt we could find one
            # if we tried.
            return

        for (name, flags, version) in \
                    itertools.izip(h[rpmhelper.OBSOLETENAME],
                                   h[rpmhelper.OBSOLETEFLAGS],
                                   h[rpmhelper.OBSOLETEVERSION]):
            single = SingleRpmObsolete()
            single.name.set(name)
            single.flags.set(flags)
            single.version.set(version)
            self.addStream(1, single)

class TroveCapsule(streams.StreamSet):
    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _TROVECAPSULE_RPM_NAME    : (DYNAMIC, streams.StringStream, 'name' ),
        _TROVECAPSULE_RPM_VERSION : (DYNAMIC, streams.StringStream, 'version' ),
        _TROVECAPSULE_RPM_RELEASE : (DYNAMIC, streams.StringStream, 'release' ),
        _TROVECAPSULE_RPM_ARCH    : (DYNAMIC, streams.StringStream, 'arch' ),
        _TROVECAPSULE_RPM_EPOCH   : (DYNAMIC, streams.IntStream,    'epoch' ),
        _TROVECAPSULE_RPM_OBSOLETES:(DYNAMIC, RpmObsoletes,         'obsoletes' ),
    }

    def reset(self):
        self.name.set(None)
        self.version.set(None)
        self.release.set(None)
        self.arch.set(None)
        self.epoch.set(None)

class TroveCapsule(streams.StreamSet):
    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _TROVECAPSULE_TYPE     : (SMALL, streams.StringStream, 'type'),
        _TROVECAPSULE_RPM      : (SMALL, TroveCapsule,         'rpm'  ),
    }

    def reset(self):
        self.type.set(None)
        self.rpm.reset()

def _getTroveInfoSigExclusions(streamDict):
    return [ streamDef[2] for tag, streamDef in streamDict.items()
             if tag > _TROVEINFO_ORIGINAL_SIG ]

_TROVESCRIPTS_COMPAT_OLD      = 0
_TROVESCRIPTS_COMPAT_NEW      = 1

class TroveScriptCompatibility(streams.StreamSet):
    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _TROVESCRIPTS_COMPAT_OLD : (SMALL, streams.ShortStream, 'old'),
        _TROVESCRIPTS_COMPAT_NEW : (SMALL, streams.ShortStream, 'new'  ),
    }

    def __str__(self):
        return "%s->%s" % (self.old(), self.new())

    def __cmp__(self, other):
        return cmp((self.old, self.new), (other.old, other.new))

class TroveScriptCompatibilityCollection(streams.StreamCollection):

    streamDict = { 1 : TroveScriptCompatibility }

    def addList(self, l):
        for (old, new) in l:
            cvt = TroveScriptCompatibility()
            cvt.old.set(old)
            cvt.new.set(new)
            self.addStream(1, cvt)

    def iter(self):
        return ( x[1] for x in self.iterAll() )

_TROVESCRIPT_SCRIPT        = 0
#_TROVESCRIPT_ROLLBACKFENCE = 1   Supported in 1.1.21; never used
_TROVESCRIPT_CONVERSIONS   = 2

class TroveScript(streams.StreamSet):
    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _TROVESCRIPT_SCRIPT        : (DYNAMIC, streams.StringStream,
                                                        'script' ),
        _TROVESCRIPT_CONVERSIONS   : (DYNAMIC, TroveScriptCompatibilityCollection,
                                                        'conversions' ),
    }

_TROVESCRIPTS_PREUPDATE    = 0
_TROVESCRIPTS_POSTINSTALL  = 1
_TROVESCRIPTS_POSTUPDATE   = 2
_TROVESCRIPTS_POSTROLLBACK = 3
_TROVESCRIPTS_PREINSTALL   = 4
_TROVESCRIPTS_PREERASE     = 5
_TROVESCRIPTS_POSTERASE    = 6
_TROVESCRIPTS_PREROLLBACK  = 7

class TroveScripts(streams.StreamSet):
    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _TROVESCRIPTS_PREUPDATE     : (DYNAMIC, TroveScript, 'preUpdate'  ),
        _TROVESCRIPTS_POSTINSTALL   : (DYNAMIC, TroveScript, 'postInstall' ),
        _TROVESCRIPTS_POSTUPDATE    : (DYNAMIC, TroveScript, 'postUpdate' ),
        _TROVESCRIPTS_PREROLLBACK   : (DYNAMIC, TroveScript, 'preRollback' ),
        _TROVESCRIPTS_POSTROLLBACK  : (DYNAMIC, TroveScript, 'postRollback' ),
        _TROVESCRIPTS_PREINSTALL    : (DYNAMIC, TroveScript, 'preInstall' ),
        _TROVESCRIPTS_PREERASE      : (DYNAMIC, TroveScript, 'preErase' ),
        _TROVESCRIPTS_POSTERASE     : (DYNAMIC, TroveScript, 'postErase' ),
    }

class TroveInfo(streams.StreamSet):
    ignoreUnknown = streams.PRESERVE_UNKNOWN
    streamDict = {
        _TROVEINFO_TAG_SIZE          : (SMALL, streams.LongLongStream,'size'        ),
        _TROVEINFO_TAG_SOURCENAME    : (SMALL, streams.StringStream,  'sourceName'  ),
        _TROVEINFO_TAG_BUILDTIME     : (SMALL, streams.LongLongStream,'buildTime'   ),
        _TROVEINFO_TAG_CONARYVER     : (SMALL, streams.StringStream, 'conaryVersion'),
        _TROVEINFO_TAG_BUILDDEPS     : (LARGE, BuildDependencies,    'buildReqs'    ),
        _TROVEINFO_TAG_LOADEDTROVES  : (LARGE, LoadedTroves,         'loadedTroves' ),
        _TROVEINFO_TAG_FLAGS         : (SMALL, TroveFlagsStream,     'flags'        ),
        _TROVEINFO_TAG_CLONEDFROM    : (SMALL, StringVersionStream,  'clonedFrom'   ),
        _TROVEINFO_TAG_SIGS          : (LARGE, TroveSignatures,      'sigs'         ),
        _TROVEINFO_TAG_PATH_HASHES   : (LARGE, PathHashes,           'pathHashes'   ),
        _TROVEINFO_TAG_LABEL_PATH    : (SMALL, LabelPath,            'labelPath'   ),
        _TROVEINFO_TAG_POLICY_PROV   : (LARGE, PolicyProviders,      'policyProviders'),
        _TROVEINFO_TAG_TROVEVERSION  : (SMALL, streams.IntStream,    'troveVersion'   ),
        _TROVEINFO_TAG_INCOMPLETE    : (SMALL, streams.ByteStream,   'incomplete'   ),
        _TROVEINFO_TAG_DIR_HASHES    : (LARGE, PathHashes,           'dirHashes'    ),
        _TROVEINFO_TAG_SCRIPTS       : (DYNAMIC, TroveScripts,       'scripts'    ),
        _TROVEINFO_TAG_METADATA      : (DYNAMIC, Metadata,           'metadata'    ),
        _TROVEINFO_TAG_COMPLETEFIXUP : (SMALL, streams.ByteStream,   'completeFixup'    ),
        _TROVEINFO_TAG_COMPAT_CLASS  : (SMALL, streams.ShortStream,  'compatibilityClass'    ),
        _TROVEINFO_TAG_BUILD_FLAVOR  : (LARGE, OptionalFlavorStream, 'buildFlavor'    ),
        _TROVEINFO_TAG_COPIED_FROM   : (DYNAMIC, TroveCopiedFrom,    'troveCopiedFrom' ),
        _TROVEINFO_TAG_IMAGE_GROUP   : (DYNAMIC, streams.ByteStream, 'imageGroup' ),
        _TROVEINFO_TAG_FACTORY       : (DYNAMIC, streams.StringStream, 'factory' ),
        _TROVEINFO_TAG_SEARCH_PATH   : (DYNAMIC, SearchPath,          'searchPath'),
        _TROVEINFO_TAG_PKGCREATORDATA: (DYNAMIC, streams.StringStream,'pkgCreatorData'),
        _TROVEINFO_TAG_DERIVEDFROM   : (DYNAMIC, LoadedTroves,        'derivedFrom' ),
        _TROVEINFO_TAG_CLONEDFROMLIST: (DYNAMIC, VersionListStream,   'clonedFromList' ),
        _TROVEINFO_TAG_CAPSULE       : (DYNAMIC, TroveCapsule,        'capsule' ),
        _TROVEINFO_TAG_MTIMES        : (DYNAMIC, TroveMtimes,         'mtimes' ),
    }

    v0SignatureExclusions = _getTroveInfoSigExclusions(streamDict)
    _oldMetadataItems = dict([ (x[1][2], True) for x in
                               MetadataItem.streamDict.items() if
                               x[0] <= _METADATA_ITEM_ORIG_ITEMS ])
    _newMetadataItems = dict([ (x[1][2], True) for x in
                               MetadataItem.streamDict.items() if
                               x[0] > _METADATA_ITEM_ORIG_ITEMS ])

    def diff(self, other):
        return streams.StreamSet.diff(self, other, ignoreUnknown=True)

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
          - total entry size, excluding these two bytes (2 bytes)
          - troveName length (2 bytes)
          - troveName
          - version string length (2 bytes)
          - version string
          - flavor string length (2 bytes)
          - flavor string
          - byDefault value (1 byte, 0 or 1)

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
          - total entry size, excluding these two bytes (2 bytes)
          - pathId (16 bytes)
          - fileId (20 bytes)
          - pathLen (2 bytes)
          - path
          - versionLen (2 bytes)
          - version string

        This whole thing is sorted by the string value of each entry. Sorting
        this way is a bit odd, but it's simple and well-defined.
        """
        l = []
        for (pathId, (dirName, baseName, fileId, version)) in self.iteritems():
            v = version.asString()
            s = misc.pack("!S16S20SHSH", pathId, fileId,
                          os.path.join(dirName, baseName), v);
            l.append((len(s), s))

        l.sort()

        return misc.pack("!" + "SH" * len(l), *( x[1] for x in l))

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
_STREAM_TRV_SIGS            = 10 # unused
_STREAM_TRV_WEAK_TROVES     = 11
_STREAM_TRV_REDIRECTS       = 12

TROVE_TYPE_NORMAL          = 0
TROVE_TYPE_REDIRECT        = 1
TROVE_TYPE_REMOVED         = 2

def _mergeTroveInfoSigExclusions(skipSet, streamDict):
    skipSet.update(dict( [ (x, True) for x in
        streamDict[_STREAM_TRV_TROVEINFO][1].v0SignatureExclusions ] ) )

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

    v0SkipSet = { 'sigs' : True, 'versionStrings' : True, 'incomplete' : True,
                  'pathHashes' : True }
    v1SkipSet = { 'sigs' : True,
                  'versionStrings' : True,
                  'incomplete' : True,
                  'metadata': True,
                  'completeFixup' : True,
                  }

    _mergeTroveInfoSigExclusions(v0SkipSet, streamDict)

    # the memory savings from slots isn't all that interesting here, but it
    # makes sure we don't add data to troves and forget to make it part
    # of the stream
    __slots__ = [ "name", "version", "flavor", "provides", "requires",
                  "changeLog", "troveInfo", "strongTroves", "weakTroves",
                  "idMap", "type", "redirects" ]
    __developer_api__ = True

    def __repr__(self):
        return "trove.Trove(%r, %r, %r)" % (self.name(), self.version(),
                                            self.flavor())

    def _sigString(self, version):
        if version == _TROVESIG_VER_CLASSIC:
            return streams.StreamSet.freeze(self, self.v0SkipSet,
                                            freezeUnknown = False)
        elif version in (_TROVESIG_VER_NEW, _TROVESIG_VER_NEW2):
            return streams.StreamSet.freeze(self, self.v1SkipSet,
                                            freezeUnknown = True)
        raise NotImplementedError

    def addDigitalSignature(self, keyId, skipIntegrityChecks = False):
        """
        Signs all of the available digests for this trove and stores those
        signatures.

        @param keyId: ID of the key use for signing
        @type keyId: str
        """
        sha1_orig = self.troveInfo.sigs.sha1()
        self.computeDigests()
        assert(skipIntegrityChecks or not sha1_orig or 
               sha1_orig == self.troveInfo.sigs.sha1())

        self.troveInfo.sigs.sign(keyId)

    def addPrecomputedDigitalSignature(self, newSigs):
        """
        Adds a previously computed signature, allowing signatures to be
        added to troves. All digests must have already been computed.

        @param newSigs: Signature to add
        @type newSigs: VersionedDigitalSignatureSet
        """
        assert(self.verifyDigests())

        providedSigs = set()
        versionDigests = {}
        for sigBlock in newSigs:
            for sig in sigBlock.signatures:
                providedSigs.add((sigBlock.version(), sigBlock.digest(), sig))

        for version, digest, sig in self.troveInfo.sigs:
            versionDigests[version] = digest
            providedSigs.discard((version, digest, sig))

        for version, digest, sig in providedSigs:
            if version in versionDigests:
                if digest != versionDigests[version]():
                    # XXX
                    raise RuntimeError('inconsistant digital signature digest')
            else:
                raise RuntimeError('missing digest for version %d' % version)

            self.troveInfo.sigs.addPrecomputedSignature(version, sig)

    def getDigitalSignature(self, keyId):
        """
        Returns the signature created by the key whose keyId is passed. The
        signature is returned as a VersionedSignaturesSet object
        containing only the signatures for the specified keyId.

        @param keyId: Id for the key whose signature is returns
        @type keyId: str
        @rtype: DigitalSignature
        """
        sigs = VersionedSignaturesSet()
        found = False
        for version, digest, sig in self.troveInfo.sigs:
            if sig and keyId == sig[0]:
                sigs.addDigest(digest(), version = version)
                sigs.addPrecomputedSignature(sig, version = version)
                found = True

        if not found:
            raise KeyNotFound('Signature by key: %s does not exist' %keyId)

        return sigs

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
        @raise DigitalSignatureVerificationError: if an invalid signature is
        found, or the keys are not trusted
        """
        version = self.getVersion()
        vlabel = None
        if isinstance(version, versions.Label):
            vlabel = version
        elif isinstance(version, versions.Version):
            vlabel = version.trailingLabel()
        elif isinstance(version, versions.VersionSequence):
            allLabels = list(version.iterLabels())
            if allLabels:
                vlabel = allLabels[-1]
        missingKeys = []
        badFingerprints = []
        untrustedKeys = set()
        maxTrust = TRUST_UNTRUSTED
        assert(self.verifyDigests())

        if keyCache is None:
            keyCache = openpgpkey.getKeyCache()

        for version, digest, signature in self.troveInfo.sigs:
            if not signature:
                # this doesn't validate the digests
                continue

            try:
                key = keyCache.getPublicKey(signature[0],
                                            label = vlabel,
                                            # don't warn about missing gpg
                                            # if the threshold is <= 0
                                            warn=(threshold > 0))
            except KeyNotFound:
                missingKeys.append(signature[0])
                continue
            lev = key.verifyString(digest(), signature)
            if lev == -1:
                badFingerprints.append(key.getFingerprint())
            elif lev < TRUST_TRUSTED:
                untrustedKeys.add(key.getFingerprint())
            maxTrust = max(lev,maxTrust)

        # verify metadata.  Pass in the label so it can
        # find additional fingerprints
        rc = self.troveInfo.metadata.verifyDigitalSignatures(label=vlabel)
        metaMissingKeys, metaBadSigs, metaUntrustedKeys = rc
        missingKeys.extend(metaMissingKeys)
        badFingerprints.extend(metaBadSigs)
        untrustedKeys.update(metaUntrustedKeys)

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
            if untrustedKeys:
                from conary.lib import log
                log.warning('The trove %s has signatures generated with '
                    'untrusted keys. You can either resign the trove with a '
                    'key that you trust, or add one of the keys to the list '
                    'of trusted keys (the trustedKeys configuration option). '
                    'The keys that were not trusted are: %s' %
                        (self.getName(), ', '.join(
                            "%s" % x[-8:] for x in sorted(untrustedKeys))))
            raise DigitalSignatureVerificationError(
                    "Trove does not meet minimum trust level: %s" 
                            % self.getName())
        return maxTrust, missingKeys, untrustedKeys

    def invalidateDigests(self):
        self.troveInfo.sigs.reset()

    def _use_TROVESIG_VER_NEW2(self):
        # go through all the trove scripts for this trove.  If any
        # of them have more than one conversion, we need to use
        # the TROVESIG_VER_NEW2
        for script in [ x[2] for x in TroveScripts.streamDict.values() ]:
            conversions = getattr(self.troveInfo.scripts, script).conversions
            if len(conversions.getStreams(1)) > 1:
                return True
        return False

    def computeDigests(self, store = True):
        """
        Recomputes the sha1 signature of this trove.

        @param store: The newly computed sha1 is stored as the sha1 for this 
        trove.
        @type store: boolean
        @rtype: string
        """
        if self._use_TROVESIG_VER_NEW2():
            sigVersions = [ _TROVESIG_VER_CLASSIC, _TROVESIG_VER_NEW2 ]
        else:
            sigVersions = [ _TROVESIG_VER_CLASSIC, _TROVESIG_VER_NEW ]

        self.troveInfo.metadata.computeDigests()

        for sigVersion in sigVersions:
            s = self._sigString(version = sigVersion)
            self.troveInfo.sigs.computeDigest(sigVersion, s)

    def verifyDigests(self):
        """
        Verifies the trove's digests

        @rtype: boolean
        """
        lastSigVersion = None
        lastDigest = None
        for (sigVersion, sigDigest, signature) in self.troveInfo.sigs:
            if sigVersion == _TROVESIG_VER_NEW and self._use_TROVESIG_VER_NEW2():
                from conary.lib import log
                log.warning('Ignoring version 1 signature on %s=%s[%s] - '
                            'it has multiple conversion entries for '
                            'a trove script' %(self.getName(),
                                               self.getVersion(),
                                               self.getFlavor()))
                continue
            if lastSigVersion == sigVersion:
                if sigDigest() != lastDigest:
                    return False
            else:
                lastDigest = sigDigest()
                lastSigVersion = sigVersion
                s = self._sigString(sigVersion)
                if not sigDigest.verify(s):
                    return False

        self.troveInfo.metadata.verifyDigests()

        return True

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

    @api.publicApi
    def getName(self):
        return self.name()
    
    @api.publicApi
    def getVersion(self):
        return self.version()

    @api.publicApi
    def getNameVersionFlavor(self):
        return self.name(), self.version(), self.flavor()

    @api.publicApi
    def getMetadata(self, language=None):
        return self.troveInfo.metadata.get(language)

    def getAllMetadataItems(self):
        return self.troveInfo.metadata.flatten()

    def copyMetadata(self, trv, skipSet=None, filteredKeyValues=None):
        """
        Copy metadata from a different trove
        @param trv: Trove object from which metadata items will be copied
        @type trv: Trove
        @param skipSet: Items that will not be copied
        @type skipSet: iterable
        @param filteredKeyValues: Keys in the key-value metadata that will
        not be copied.
        @type filteredKeyValues: iterable
        """
        return self.copyMetadataFromMetadata(trv.troveInfo.metadata,
                                             skipSet=skipSet,
                                             filteredKeyValues=filteredKeyValues)

    def copyMetadataFromMetadata(self, metadata, skipSet=None, filteredKeyValues=None):
        """
        Copy metadata from a different metadata object
        @param metadata: Metadata object from which metadata items will be
        copied
        @type metadata: Metadata
        @param skipSet: Items that will not be copied
        @type skipSet: iterable
        @param filteredKeyValues: Keys in the key-value metadata that will
        not be copied.
        @type filteredKeyValues: iterable
        """
        items = metadata.flatten(skipSet=skipSet,
                                 filteredKeyValues=filteredKeyValues)
        self.troveInfo.metadata = Metadata()
        self.troveInfo.metadata.addItems(items)

    def getFactory(self):
        return self.troveInfo.factory()

    def setFactory(self, val):
        return self.troveInfo.factory.set(val)

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
        dirName, baseName = os.path.split(path)
        self.addRawFile(pathId, dirName, baseName, version, fileId)

    def addRawFile(self, pathId, dirName, baseName, version, fileId):
        assert(len(pathId) == 16)
        assert(fileId is None or len(fileId) == 20)
        assert(not self.type())
        self.idMap[pathId] = (dirName, baseName, fileId, version)

    def addRpmCapsule(self, path, version, fileId, hdr):
        assert(len(fileId) == 20)
        dir, base = os.path.split(path)
        self.idMap[CAPSULE_PATHID] = (dir, base, fileId, version)
        self.troveInfo.capsule.type.set('rpm')
        self.troveInfo.capsule.rpm.name.set(hdr[rpmhelper.NAME])
        self.troveInfo.capsule.rpm.version.set(hdr[rpmhelper.VERSION])
        self.troveInfo.capsule.rpm.release.set(hdr[rpmhelper.RELEASE])
        self.troveInfo.capsule.rpm.arch.set(hdr[rpmhelper.ARCH])
        epoch = hdr.get(rpmhelper.EPOCH, [None])[0]
        if epoch:
            self.troveInfo.capsule.rpm.epoch.set(epoch)

        self.troveInfo.capsule.rpm.obsoletes.addFromHeader(hdr)

    def computePathHashes(self):
        self.troveInfo.pathHashes.clear()
        self.troveInfo.dirHashes.clear()
        for dirName, base, fileId, version in self.idMap.itervalues():
            self.troveInfo.dirHashes.addPath(dirName)
            self.troveInfo.pathHashes.addPath(os.path.join(dirName, base))

    # pathId is the only thing that must be here; the other fields could
    # be None
    def updateFile(self, pathId, path, version, fileId):
        if not path:
            dirName = None
            baseName = None
        else:
            dirName, baseName = os.path.split(path)
            dirName = dirName
            baseName = baseName

        self.updateRawFile(pathId, dirName, baseName, version, fileId)

    def updateRawFile(self, pathId, dirName, baseName, version, fileId):
	(origDir, origBase, origFileId, origVersion) = self.idMap[pathId]

	if baseName is None:
            dirName, baseName = origDir, origBase

	if not version:
	    version = origVersion
	    
	if not fileId:
	    fileId = origFileId
	    
	self.idMap[pathId] = (dirName, baseName, fileId, version)

    @api.developerApi
    def removeFile(self, pathId):   
	del self.idMap[pathId]

	#return self.idMap.iteritems()

    def removeAllFiles(self):
        self.idMap = TroveRefsFilesStream()

    def iterFileList(self, members = None, capsules = False):
        if members is None:
            members = (not capsules)

        if capsules and not self.troveInfo.capsule.type():
            # We were asked for only the capsules, but this is a
            # conary format trove. That means everything is its own
            # capsule, so return everything
            members = True

        for (theId, (path, base, fileId, version)) in self.idMap.iteritems():
            if ( (theId != CAPSULE_PATHID and members) or
                 (theId == CAPSULE_PATHID and capsules) ):
                yield (theId, os.path.join(path, base), fileId, version)

    def emptyFileList(self):
        return len(self.idMap) == 0

    def getFile(self, pathId):
        x = self.idMap[pathId]
	return (os.path.join(x[0], x[1]), x[2], x[3])

    def hasFile(self, pathId):
	return self.idMap.has_key(pathId)

    def hasFiles(self):
        return len(self.idMap) != 0

    def fileCount(self):
        return len(self.idMap)

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

    @api.publicApi
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
                       allowIncomplete = False, skipFiles = False,
                       needNewFileMap = False):
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
            for (pathId, dirName, baseName, fileId, fileVersion) in \
                            trvCs.getNewFileList(raw = True):
                self.addRawFile(pathId, dirName, baseName, fileVersion, fileId)
                if needNewFileMap:
                    fileMap[pathId] = self.idMap[pathId] + \
                                        (self.name(), None, None, None)

            for (pathId, dirName, baseName, fileId, fileVersion) in \
                                        trvCs.getChangedFileList(raw = True):
                (oldDir, oldBase, oldFileId, oldVersion) = self.idMap[pathId]
                self.updateRawFile(pathId, dirName, baseName, fileVersion,
                                   fileId)
                # look up the path/version in self.idMap as the ones here
                # could be None
                if baseName is not None:
                    path = os.path.join(dirName, baseName)
                else:
                    path = None

                if needNewFileMap:
                    fileMap[pathId] = (None, fileVersion, fileId, self.name(),
                                       os.path.join(oldDir, oldBase),
                                       oldFileId, oldVersion)

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

        incomplete = (self.troveInfo.incomplete() and 1) or 0

        if trvCs.getFrozenTroveInfo():
            # We cannot trust the absolute trove info representation since the
            # incomplete flag is dynamic (the old trove's flag can be set now
            # even if it wasn't originally). Use the relative data (which may
            # set it or not).
            troveInfoClass = self.streamDict[_STREAM_TRV_TROVEINFO][1]
            self.troveInfo = trvCs.getTroveInfo(klass = troveInfoClass)
            if not self.troveInfo.incomplete():
                self.troveInfo.incomplete.set(incomplete)
        elif not trvCs.getOldVersion():
            self.troveInfo = TroveInfo(trvCs.getTroveInfoDiff())
        else:
            self.troveInfo.twm(trvCs.getTroveInfoDiff(), self.troveInfo)

        if self.troveInfo.completeFixup():
            self.troveInfo.incomplete.set(0)
            self.troveInfo.completeFixup.set(None)

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
                if not self.verifyDigests():
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

        @param strongChangeList: A list or generator specifying a set of trove
        changes; this is the same as returned by
        TroveChangeSet.iterChangedTroves(strongRefs=True, weakRefs=False)
        @type strongChangeList: (name, list) tuple
        @param weakChangeList: A list or generator specifying a set of trove
        changes; this is the same as returned by
        TroveChangeSet.iterChangedTroves(strongRefs=False, weakRefs=True)
        @type weakChangeList: (name, list) tuple
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

    @api.publicApi
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

        chgSet.setTroveInfo(self.troveInfo)

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
                (selfDir, selfBase, selfFileId, selfVersion) = \
                            self.idMap[pathId]
                filesNeeded.append((pathId, None, None, selfFileId,
                                    selfVersion))
                chgSet.newFile(pathId, os.path.join(selfDir, selfBase),
                               selfFileId, selfVersion)

            for pathId in sameIds.keys():
                (selfDir, selfBase, selfFileId,
                                    selfVersion) = self.idMap[pathId]
                (themDir, themBase, themFileId,
                                    themVersion) = themMap[pathId]
                newPath = None
                newVersion = None

                if selfDir != themDir or selfBase != themBase:
                    newPath = os.path.join(selfDir, selfBase)

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
            then by heuristics based on labels, flavors, and path hashes.
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
            # different labels and the timestamps for the same label. If the
            # same version exists twice here, it means the flavors are
            # incompatible or tied; in either case the flavor won't help us
            # much.
            matches = []
            byLabel = {}
            # we need copies we can update
            oldInfoSet = set(oldInfoSet)
            newInfoSet = set(newInfoSet)

            for newInfo in newInfoSet:
                for oldInfo in oldInfoSet:
                    if newInfo[0].trailingLabel() == oldInfo[0].trailingLabel():
                        l = byLabel.setdefault(newInfo, [])
                        l.append(((oldInfo[0].trailingRevision(), oldInfo)))

            # pass 1, find things on the same branch
            for newInfo, oldInfoList in sorted(byLabel.items(), reverse=True):
                # take the newest (by timestamp) item from oldInfoList which
                # hasn't been matched to anything else 
                oldInfoList.sort(reverse=True)

                for revision, oldInfo in oldInfoList:
                    if oldInfo not in oldInfoSet: continue
                    matches.append((oldInfo, newInfo))
                    oldInfoSet.remove(oldInfo)
                    newInfoSet.remove(newInfo)
                    break

            del byLabel

            # pass 2, match across labels -- we know there is nothing left
            # on the same label anymore
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
            elif (len(addedDict[name]) == 1 and len(removedDict[name]) == 1):
                # there's only one thing to match it to. some match is better
                # than no match. the complicated matching logic later on
                # would give the same result at the end of it all, but just
                # doing it here avoids fetching the path hashes, which is
                # relatively expensive if those hashes are in the local
                # database
                trvList.append( (name, list(removedDict[name])[0],
                                 list(addedDict[name])[0], False) )
                del addedDict[name]
                del removedDict[name]

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


            addedByLabel = {}
            removedByLabel = {}
            for version, flavor in addedDict[name]:
                addedByLabel.setdefault(version.trailingLabel(), []).append(
                                                            (version, flavor))
            for version, flavor in removedDict[name]:
                removedByLabel.setdefault(version.trailingLabel(), []).append(
                                                            (version, flavor))
            for label, labelAdded in addedByLabel.iteritems():
                labelRemoved = removedByLabel.get(label, [])
                if not labelRemoved:
                    continue
                # 1. match troves on the same label with compatible flavors.
                _matchList(name, labelAdded, labelRemoved, trvList,
                           overlaps, scoreCache, requireCompatible=True)

            # 2. match troves with compatible flavors on different
            #    labels
            _matchList(name, addedDict[name], removedDict[name], trvList,
                       overlaps, scoreCache, requireCompatible=True)

            addedByLabel = {}
            removedByLabel = {}
            for version, flavor in addedDict[name]:
                addedByLabel.setdefault(version.trailingLabel(), []).append(
                                                            (version, flavor))
            for version, flavor in removedDict[name]:
                removedByLabel.setdefault(version.trailingLabel(), []).append(
                                               (version, flavor))

            for label, labelAdded in addedByLabel.iteritems():
                labelRemoved = removedByLabel.get(label, [])
                if not labelRemoved:
                    continue
                # 3. match troves on the same label without compatible flavors.
                _matchList(name, labelAdded, labelRemoved, trvList,
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

    def setCompatibilityClass(self, theClass):
        self.troveInfo.compatibilityClass.set(theClass)
 
    @api.publicApi
    def getCompatibilityClass(self):
        c = self.troveInfo.compatibilityClass()
        if c is None:
            return 0

        return c

    def setBuildFlavor(self, flavor):
        return self.troveInfo.buildFlavor.set(flavor)

    def getBuildFlavor(self):
        return self.troveInfo.buildFlavor()

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

    def setIsDerived(self, b):
        if b:
            return self.troveInfo.flags.isDerived(set = True)
        else:
            return self.troveInfo.flags.isDerived(set = False)

    def setIsMissing(self, b):
        if b:
            return self.troveInfo.flags.isMissing(set = True)
        else:
            return self.troveInfo.flags.isMissing(set = False)

    def isCollection(self):
        return self.troveInfo.flags.isCollection()

    def isDerived(self):
        return self.troveInfo.flags.isDerived()

    def isMissing(self):
        return self.troveInfo.flags.isMissing()

    def setLabelPath(self, labelPath):
        self.troveInfo.labelPath = LabelPath()
        for label in labelPath:
            self.troveInfo.labelPath.set(str(label))

    def getLabelPath(self):
        return [ versions.Label(x) for x in self.troveInfo.labelPath ]

    def setSearchPath(self, searchPath):
        for item in searchPath:
            self.troveInfo.searchPath.add(item)

    def getSearchPath(self):
        return list(self.troveInfo.searchPath.iter())

    def setBuildRequirements(self, itemList):
        for (name, ver, flavor) in itemList:
            self.troveInfo.buildReqs.add(name, ver, flavor)

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

    def setDerivedFrom(self, itemList):
        for (name, ver, release) in itemList:
            self.troveInfo.derivedFrom.add(name, ver, release)

    def getDerivedFrom(self):
        return [ (x[1].name(), x[1].version(), x[1].flavor())
                 for x in self.troveInfo.derivedFrom.iterAll() ]

    def getPathHashes(self):
        return self.troveInfo.pathHashes

    def setTroveCopiedFrom(self, itemList):
        for (name, ver, flavor) in itemList:
            self.troveInfo.troveCopiedFrom.add(name, ver, flavor)

    def getTroveCopiedFrom(self):
        """For groups, return the list of troves that were used when
        a statement like addAll or addCopy was used.

        @rtype: list
        @return: list of (name, version, flavor) tuples.
        """
        return [ (x[1].name(), x[1].version(), x[1].flavor())
                 for x in self.troveInfo.troveCopiedFrom.iterAll() ]

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

            if not re.match('^[_A-Za-z0-9+\.\-:@]+$', name):
                raise TroveError, \
                            "Illegal characters in trove name '%s'" % name

            if 0 in [ len(x) for x in name.split(":") ]:
                raise TroveError, \
                            'Trove and component names cannot be empty'

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

class TroveWithFileObjects(Trove):

    def addFileObject(self, fileId, obj):
        self.fileObjs[fileId] = obj

    def getFileObject(self, fileId):
        return self.fileObjs[fileId]

    def __init__(self, *args, **kwargs):
        # indexed by fileId
        self.fileObjs = {}
        Trove.__init__(self, *args, **kwargs)

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
                change, version, flavor, byDefFlag = l[i:i+4]
                version = versions.ThawVersion(version)

		if flavor == "-":
		    flavor = deps.Flavor()
		else:
		    flavor = deps.ThawFlavor(flavor)

                if change == '-':
                    byDefault = None
                elif byDefFlag == '0':
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

	for (pathId, dirName, baseName, fileId, version) in self:
	    l.append(pathId)
            if baseName is None:
                path = ""
            else:
                path = os.path.join(dirName, baseName)

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
            if not path:
                dirName = None
                baseName = None
            else:
                dirName, baseName = os.path.split(path)
                dirName = intern(dirName)
                baseName = intern(baseName)

            if not fileId:
                fileId = None
            else:
                fileId = intern(fileId)

            if verStr == lastVerStr:
                version = lastVer
            elif verStr:
                version = versions.VersionFromString(verStr)
                lastVer = version
                lastVerStr = verStr
            else:
                version = None

            self.append((pathId, dirName, baseName, fileId,
                         version))

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
_STREAM_TCS_ABSOLUTE_TROVEINFO      = 19
_STREAM_TCS_EXTENDED_METADATA       = 20

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
        _STREAM_TCS_ABSOLUTE_TROVEINFO
                                : (LARGE, streams.StringStream,
                                                        "absoluteTroveInfo"  ),
        _STREAM_TCS_EXTENDED_METADATA
                                : (LARGE, streams.StringStream,
                                                        "extendedMetadata"   ),
    }

    ignoreUnknown = True

    """
    Represents the changes between two troves and forms part of a
    ChangeSet. 
    """

    @api.publicApi
    def isAbsolute(self):
	return self.tcsType() == _TCS_TYPE_ABSOLUTE

    def newFile(self, pathId, path, fileId, version):
        dirName, baseName = os.path.split(path)
        self.newFiles.append((pathId, dirName, baseName, fileId, version))

    # raw means separate dirName/baseName in the tuple
    def getNewFileList(self, raw = False):
        if raw:
            return self.newFiles

        return [ (x[0], os.path.join(x[1], x[2]), x[3], x[4])
                            for x in self.newFiles]

    def oldFile(self, pathId):
	self.oldFiles.append(pathId)

    def getOldFileList(self):
	return self.oldFiles

    @api.publicApi
    def getName(self):
        """
        Get the name of the trove.
        @return: name of the trove.
        @rtype: string
        """
	return self.name()

    def getTroveInfoDiff(self):
        return self.troveInfoDiff()

    def getFrozenTroveInfo(self):
        return self.absoluteTroveInfo()

    def getFrozenExtendedMetadata(self):
        return self.extendedMetadata()

    def _getScriptObj(self, kind):
        troveInfo = self.absoluteTroveInfo()
        scriptStream = TroveInfo.find(_TROVEINFO_TAG_SCRIPTS,
                                      troveInfo)

        if not troveInfo and scriptStream is None:
            # fall back to the trove info diff - _only_ if there is no
            # absolute trove info.  this is deprecated
            scriptStream = TroveInfo.find(_TROVEINFO_TAG_SCRIPTS,
                                          self.troveInfoDiff())
            if scriptStream:
                import warnings
                warnings.warn("Obtaining unsigned script information from "
                              "an old changeset.  In the future, unsigned "
                              "script information will not be retreived. "
                              "Use a new version of Conary to generate "
                              "compatible changesets.", FutureWarning)

        if scriptStream is None:
            return None

        # this is horrid, but it's just looking up the script stream we're
        # looking for
        script = scriptStream.__getattribute__(scriptStream.streamDict[kind][2])
        return script

    def _getScript(self, kind):
        scriptObj = self._getScriptObj(kind)
        if scriptObj:
            return scriptObj.script()
        return None

    def getPostInstallScript(self):
        return self._getScript(_TROVESCRIPTS_POSTINSTALL)

    def getPostUpdateScript(self):
        return self._getScript(_TROVESCRIPTS_POSTUPDATE)

    def getPreUpdateScript(self):
        return self._getScript(_TROVESCRIPTS_PREUPDATE)

    def getPreRollbackScript(self):
        return self._getScript(_TROVESCRIPTS_PREROLLBACK)

    def getPostRollbackScript(self):
        return self._getScript(_TROVESCRIPTS_POSTROLLBACK)

    # Not intended for general use
    def _getPreInstallScript(self):
        return self._getScript(_TROVESCRIPTS_PREINSTALL)

    def _getPreEraseScript(self):
        return self._getScript(_TROVESCRIPTS_PREERASE)

    def _getPostEraseScript(self):
        return self._getScript(_TROVESCRIPTS_POSTERASE)

    def getNewCompatibilityClass(self):
        troveInfo = self.absoluteTroveInfo()
        c = TroveInfo.find(_TROVEINFO_TAG_COMPAT_CLASS, troveInfo)
        if not troveInfo and c is None:
            # fall back to the trove info diff - this is deprecated
            c = TroveInfo.find(_TROVEINFO_TAG_COMPAT_CLASS,
                               self.troveInfoDiff())
            if c:
                import warnings
                warnings.warn("Obtaining unsigned script information from "
                              "an old changeset.  In the future, unsigned "
                              "script information will not be retreived. "
                              "Use a new version of Conary to generate "
                              "compatible changesets.", FutureWarning)


        if c is None or c() is None:
            # no compatibility class has been set for this trove; treat that
            # as compatibility class 0
            c = 0
        else:
            c = c()

        return c

    @api.publicApi
    def isRollbackFence(self, oldCompatibilityClass = None, update = False):
        """
        Determine whether an update from the given oldCompatibilityClass to the
        version represented by this changeset would cross a rollback fence.  If
        an update crosses a rollback fence, then it is not allowed to be rolled
        back.
        @param oldCompatibilityClass: the old compatibility class.  If this is
        None, then compatibility class checks isn't used to restrict rollbacks,
        so this will return False.
        @type oldCompatibilityClass: integer or None
        @param update: unused
        @type update: any
        @return: whether applying this changeset would cross a rollback fence.
        @rtype: boolean
        @raises AssertionError: if the input oldCompatibilityClass is neither
        an integer nor None.
        """
        # FIXME: why is the update parameter unused?  Is this for
        # backwards-compatibility?
        if oldCompatibilityClass is None:
            return False
        assert isinstance(oldCompatibilityClass, (int, long))

        thisCompatClass = self.getNewCompatibilityClass()

        # if both old a new have the same compatibility class, there is no
        # fence
        if oldCompatibilityClass == thisCompatClass:
            return False

        # FIXME: the rollbackScript variable below is never used.
        rollbackScript = self.getPostRollbackScript()
        postRollback = self._getScriptObj(_TROVESCRIPTS_POSTROLLBACK)

        if postRollback is None or not postRollback.script():
            # there is no rollback script; use a strict compatibility class
            # check
            return oldCompatibilityClass != thisCompatClass

        # otherwise see if the rollback script is valid for this case
        for cvt in list(postRollback.conversions.iter()):
            # this may look backwards, but it's a rollback script
            if (cvt.new() == oldCompatibilityClass and
                                cvt.old() == thisCompatClass):
                return False

        return True

    def setTroveInfo(self, ti):
        self.absoluteTroveInfo.set((ti.freeze(skipSet = ti._newMetadataItems)))
        self.extendedMetadata.set((ti.metadata.freeze(skipSet =
                                                        ti._oldMetadataItems)))

    def getTroveInfo(self, klass = TroveInfo):
        if self.absoluteTroveInfo():
            trvInfo = klass(self.absoluteTroveInfo())
            if self.extendedMetadata():
                extMetadata = Metadata(self.extendedMetadata())
                for old, ext in itertools.izip(trvInfo.metadata, extMetadata):
                    for attrName in trvInfo._oldMetadataItems:
                        setattr(ext, attrName, getattr(old, attrName))
                trvInfo.metadata._replaceAll(extMetadata)

            return trvInfo
        else:
            return None

    def getChangeLog(self):
	return self.changeLog

    def changeOldVersion(self, version):
	self.oldVersion.set(version)

    def changeChangeLog(self, cl):
        assert(0)
	self.changeLog.thaw(cl.freeze())

    @api.publicApi
    def getOldVersion(self):
        """
        Get the old version of the trove this changeset applies to.  For an
        absolute changeset, this is None
        @return: old version
        @rtype: conary.versions.Version object or None
        """
	return self.oldVersion()

    @api.publicApi
    def getOldNameVersionFlavor(self):
        return self.name(), self.oldVersion(), self.oldFlavor()

    @api.publicApi
    def getNewVersion(self):
        """
        Get the new version of the trove that'd be installed after applying
        this changeset.
        @return: new version
        @rtype: conary.versions.Version object
        """
	return self.newVersion()

    @api.publicApi
    def getNewNameVersionFlavor(self):
        return self.name(), self.newVersion(), self.newFlavor()

    def getJob(self):
        return (self.name(), (self.oldVersion(), self.oldFlavor()),
                             (self.newVersion(), self.newFlavor()),
                self.isAbsolute())

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
        if path:
            dirName, baseName = os.path.split(path)
        else:
            dirName = None
            baseName = None

        self.changedFiles.append((pathId, dirName, baseName, fileId, version))

    # raw means separate dirName/baseName in the tuple
    def getChangedFileList(self, raw = False):
        if raw:
            return self.changedFiles

        l = []
        for t in self.changedFiles:
            if t[1] is not None or t[2] is not None:
                t = (t[0], os.path.join(t[1], t[2]), t[3], t[4])
            else:
                t = (t[0], None, t[3], t[4])
            l.append(t)

        return l

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

        if self.troveType() == TROVE_TYPE_REDIRECT:
            if not [ x for x in self.redirects.iter() ]:
                f.write("remove redirect ")
            else:
                f.write("redirect ")

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

        for redirect in self.redirects.iter():
            print '\t-> %s=%s' % (redirect.name(), redirect.branch())

	for (pathId, path, fileId, version) in self.getNewFileList():
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

	for (pathId, path, fileId, version) in self.getChangedFileList():
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

    @api.publicApi
    def getOldFlavor(self):
        return self.oldFlavor()

    @api.publicApi
    def getNewFlavor(self):
        return self.newFlavor()

    def getNewPathHashes(self):
        absInfo = self.absoluteTroveInfo()
        if absInfo:
            return TroveInfo.find(_TROVEINFO_TAG_PATH_HASHES, absInfo)
        elif self.oldVersion() is None:
            return TroveInfo.find(_TROVEINFO_TAG_PATH_HASHES,
                                  self.troveInfoDiff())

        return None

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

    def __repr__(self):
        return "conary.trove.TroveChangeSet('%s')" % self.name()

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
    # Mark the error as uncatchable, we want the callback wrapper to re-raise
    # it instead of burying it
    errorIsUncatchable = True
    def __str__(self):
        return self.digest

    def __init__(self, digest):
        self.digest = digest

class TroveIntegrityError(TroveError):
    """
    Indicates that a checksum did not match
    """
    _error = "Trove Integrity Error: %s=%s[%s] checksum does not match precalculated value"

    def marshall(self, marshaller):
        return (str(self), marshaller.fromTroveTup(self.nvf)), {}

    @staticmethod
    def demarshall(marshaller, tup):
        return marshaller.toTroveTup(tup[1]), {}

    def __init__(self, name=None, version=None, flavor=None, error=None):
        if name:
            self.nvf = (name, version, flavor)
            if error is None:
                error = self._error % self.nvf
        else:
            self.nvf = None


        TroveError.__init__(self, error)
