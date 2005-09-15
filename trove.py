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
Implements troves (packages, components, etc.) for the repository
"""

import changelog
import copy
import files
from lib import sha1helper
from lib.openpgpkey import getKeyCache
from lib.openpgpfile import KeyNotFound
import streams
import struct
import versions
from deps import deps
from changelog import ChangeLog
from streams import FrozenVersionStream
from streams import StringVersionStream
from streams import DependenciesStream
from streams import ByteStream

class TroveTuple(streams.StreamSet):
    _SINGLE_TROVE_TUP_NAME    = 0
    _SINGLE_TROVE_TUP_VERSION = 1
    _SINGLE_TROVE_TUP_FLAVOR  = 2

    ignoreUnknown = True
    streamDict = {
        _SINGLE_TROVE_TUP_NAME    : (streams.StringStream,        'name'    ),
        _SINGLE_TROVE_TUP_VERSION : (streams.StringVersionStream, 'version' ),
        _SINGLE_TROVE_TUP_FLAVOR  : (streams.DependenciesStream,  'flavor'  )
    }
    _streamDict = streams.StreamSetDef(streamDict)

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

    def iter(self):
        return self.iterAll()

class BuildDependencies(TroveTupleList):
    pass

class LoadedTroves(TroveTupleList):
    pass

class InstallBucketItem(streams.StreamSet):
    _INSTALL_BIN_KEY  = 0
    _INSTALL_BIN_VALUE = 1

    ignoreUnknown = True
    streamDict = {
        _INSTALL_BIN_KEY    : (streams.StringStream,        'key'    ),
        _INSTALL_BIN_VALUE  : (streams.StringStream,        'value'  ),
    }
    _streamDict = streams.StreamSetDef(streamDict)


class InstallBucket(streams.StreamCollection):
    streamDict = { 1 : InstallBucketItem }

    # XXX these aren't particularly efficient; they imitate a dict with a list

    def add(self, key, value):
        item = InstallBucketItem()
        item.key.set(key)
        item.value.set(value)
        self.addStream(1, item)

    def keys(self):
        return [ x for x in self.iterkeys() ]

    def __iter__(self):
        return self.iterkeys()

    def iterkeys(self):
        for bucket in self.getStreams(1):
            yield bucket.key()

    def values(self):
        return [ x for x in self.itervalues() ]

    def itervalues(self):
        for bucket in self.getStreams(1):
            yield bucket.value()

    def items(self):
        return [ x for x in self.iteritems() ]

    def iteritems(self):
        for bucket in self.getStreams(1):
            yield bucket.key(), bucket.value()

    def __getitem__(self, key):
        for bucket in self.getStreams(1):
            if bucket.key() == key:
                return bucket.value()

        raise KeyError

    def __setitem__(self, key, value):
        for bucket in self.getStreams(1):
            if bucket.key() == key:
                bucket.value.set(value)
                return

        self.add(key, value)

    def compatibleWith(self, other):
        if not other.keys() or not self.keys():
            return False

        otherDict = dict(other)

        for key, value in self.iteritems():
            if otherDict.get(key, value) == value:
                return False
            del otherDict[key]

        if otherDict:
            return False

        return True

_DIGSIG_FINGERPRINT   = 0
_DIGSIG_SIGNATURE     = 1
_DIGSIG_TIMESTAMP     = 2

class DigitalSignature(streams.StreamSet):
    streamDict = {
        _DIGSIG_FINGERPRINT     : ( streams.StringStream,    'fingerprint' ),
        _DIGSIG_SIGNATURE       : ( streams.StringStream,    'signature'   ),
        _DIGSIG_TIMESTAMP       : ( streams.IntStream,       'timestamp'   ),
    }
    _streamDict = streams.StreamSetDef(streamDict)

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
        _TROVESIG_SHA1            : ( streams.Sha1Stream,    'sha1'        ),
        _TROVESIG_DIGSIG          : ( DigitalSignatures,     'digitalSigs' ),
    }
    _streamDict = streams.StreamSetDef(streamDict)

    # this code needs to be called any time we're making a derived
    # trove esp. shadows. since some info in the trove gets changed
    # we cannot allow the signatures to persist.
    def reset(self):
        self.digitalSigs = DigitalSignatures()
        self.sha1 = streams.Sha1Stream()

    def freeze(self, skipSet = {}):
        if not self.sha1():
            return ""

        return streams.StreamSet.freeze(self, skipSet = skipSet)

_TROVEINFO_TAG_SIZE           = 0
_TROVEINFO_TAG_SOURCENAME     = 1
_TROVEINFO_TAG_BUILDTIME      = 2
_TROVEINFO_TAG_CONARYVER      = 3
_TROVEINFO_TAG_BUILDDEPS      = 4
_TROVEINFO_TAG_LOADEDTROVES   = 5
_TROVEINFO_TAG_INSTALLBUCKET  = 6
_TROVEINFO_TAG_ISCOLLECTION   = 7
_TROVEINFO_TAG_CLONEDFROM     = 8
_TROVEINFO_TAG_SIGS           = 9

class TroveInfo(streams.StreamSet):
    ignoreUnknown = True
    streamDict = {
        _TROVEINFO_TAG_SIZE          : ( streams.LongLongStream,'size'        ),
        _TROVEINFO_TAG_SOURCENAME    : ( streams.StringStream,  'sourceName'  ),
        _TROVEINFO_TAG_BUILDTIME     : ( streams.LongLongStream,'buildTime'   ),
        _TROVEINFO_TAG_CONARYVER     : ( streams.StringStream, 'conaryVersion'),
        _TROVEINFO_TAG_BUILDDEPS     : ( BuildDependencies,    'buildReqs'    ),
        _TROVEINFO_TAG_LOADEDTROVES  : ( LoadedTroves,         'loadedTroves' ),
        _TROVEINFO_TAG_INSTALLBUCKET : ( InstallBucket,        'installBucket'),
        _TROVEINFO_TAG_ISCOLLECTION  : ( streams.ShortStream,  'isCollection' ),
        _TROVEINFO_TAG_CLONEDFROM    : ( StringVersionStream,  'clonedFrom'   ),
        _TROVEINFO_TAG_SIGS          : ( TroveSignatures,      'sigs'         ),
    }
    _streamDict = streams.StreamSetDef(streamDict)



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

_STREAM_TRV_NAME      = 0
_STREAM_TRV_VERSION   = 1
_STREAM_TRV_FLAVOR    = 2
_STREAM_TRV_CHANGELOG = 3
_STREAM_TRV_TROVEINFO = 4
_STREAM_TRV_PROVIDES  = 5
_STREAM_TRV_REQUIRES  = 6
_STREAM_TRV_TROVES    = 7
_STREAM_TRV_FILES     = 8
_STREAM_TRV_REDIRECT  = 9
_STREAM_TRV_SIGS      = 10

class Trove(streams.LargeStreamSet):
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
        _STREAM_TRV_NAME      : (streams.StringStream,        "name"      ),
        _STREAM_TRV_VERSION   : (streams.FrozenVersionStream, "version"   ), 
        _STREAM_TRV_FLAVOR    : (streams.DependenciesStream,  "flavor"    ), 
        _STREAM_TRV_PROVIDES  : (streams.DependenciesStream,  "provides"  ), 
        _STREAM_TRV_REQUIRES  : (streams.DependenciesStream,  "requires"  ), 
        _STREAM_TRV_CHANGELOG : (changelog.ChangeLog,         "changeLog" ), 
        _STREAM_TRV_TROVEINFO : (TroveInfo,                   "troveInfo" ), 
        _STREAM_TRV_TROVES    : (TroveRefsTrovesStream,       "troves"    ), 
        _STREAM_TRV_FILES     : (TroveRefsFilesStream,        "idMap"     ), 
        _STREAM_TRV_REDIRECT  : (ByteStream,                  "redirect"  ),
    }
    _streamDict = streams.StreamSetDef(streamDict)
    ignoreUnknown = False

    # the memory savings from slots isn't all that interesting here, but it
    # makes sure we don't add data to troves and forget to make it part
    # of the stream
    __slots__ = [ "name", "version", "flavor", "provides", "requires",
                  "changeLog", "troveInfo", "troves", "idMap", "redirect",
                  "immutable" ]

    def __repr__(self):
        return "trove.Trove('%s', %s)" % (self.name(), repr(self.version()))

    def _sigString(self):
        return streams.LargeStreamSet.freeze(self, 
                                             skipSet = { 'sigs' : True,
                                                      'versionStrings' : True })
    def addDigitalSignature(self, keyId, skipIntegrityChecks = False):
        if skipIntegrityChecks:
            self.computeSignatures()
        else:
            sha1_orig = self.troveInfo.sigs.sha1()
            sha1_new = self.computeSignatures()
            if sha1_orig:
                assert(sha1_orig == sha1_new)
        keyCache = getKeyCache()
        key = keyCache.getPrivateKey(keyId)
        sig = key.signString(self.troveInfo.sigs.sha1())
        self.troveInfo.sigs.digitalSigs.add(sig)

    #these functions reduce code duplication in netauth and netserver
    #since we're going to need to pass the frozen form over the net
    def addPrecomputedDigitalSignature(self, sig):
        sha1_orig = self.troveInfo.sigs.sha1()
        sha1_new = self.computeSignatures()
        if sha1_orig:
            assert(sha1_orig == sha1_new)
        signature = DigitalSignature()
        signature.set(sig)
        self.troveInfo.sigs.digitalSigs.addStream(_DIGSIGS_DIGSIGNATURE, signature)

    def getDigitalSignature(self, keyId):
        return self.troveInfo.sigs.digitalSigs.getSignature(keyId)

    # return codes:
    # 0 completely unknown voracity:
    #    either we didn't have the key or we have no reason to trust the signing key
    # positive values indicate a successful verification
    #   the higher the value, the more we trust the signing key
    # missingKeys is a list of missing fingerprints. pass a (blank?) list if you care to collect them...
    #raise an exception if there were any bad signatures
    #raise an exception if the trust level is beneath threshold
    def verifyDigitalSignatures(self, threshold = 0):
        missingKeys = []
        badFingerprints = []
        maxTrust = 0
        sha1_orig = self.troveInfo.sigs.sha1()
        sha1_new = self.computeSignatures(store=False)
        if sha1_orig:
            assert(sha1_orig == sha1_new)

        keyCache = getKeyCache()
        for signature in self.troveInfo.sigs.digitalSigs.iter():
            try:
                key = keyCache.getPublicKey(signature[0])
            except KeyNotFound:
                missingKeys.append(signature[0])
                continue
            lev = key.verifyString(self.troveInfo.sigs.sha1(), signature)
            if lev == -1:
                badFingerprints.append(key.getFingerprint())
            maxTrust = max(lev,maxTrust)

        if len(badFingerprints):
            raise DigitalSignatureVerificationError("The following keys have bad digital signatures: %s" % (' '.join(badFingerprints)))

        if maxTrust < threshold:
            raise DigitalSignatureVerificationError("This trove does not meet minimum trust level")
        return maxTrust, missingKeys

    def computeSignatures(self, store = True):
        s = self._sigString()
        sha1 = sha1helper.sha1String(s)
        if store:
            self.troveInfo.sigs.sha1.set(sha1)
        return sha1

    def verifySignatures(self):
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
                            isRedirect = self.isRedirect())
        new.idMap = self.idMap.copy()
        new.troves = self.troves.copy()
        new.provides.thaw(self.provides.freeze())
        new.requires.thaw(self.requires.freeze())
        new.changeLog = changelog.ChangeLog(self.changeLog.freeze())
        new.troveInfo.thaw(self.troveInfo.freeze())
        return new

    def getName(self):
        return self.name()
    
    def getVersion(self):
        return self.version()
    
    def changeVersion(self, version):
        self.version.set(version)

    def changeChangeLog(self, cl):
	self.changeLog.thaw(cl.freeze())

    def changeFlavor(self, flavor):
        self.flavor.set(flavor)

    def getSigs(self):
        return self.troveInfo.sigs

    def setSigs(self, sigs):
        # make sure the signature block being applied to this trove is
        # correct for this trove
        check = self.computeSignatures(store=False)
        assert(check == sigs.sha1())
        self.troveInfo.sigs = sigs

    def isRedirect(self):
        return self.redirect()

    def addFile(self, pathId, path, version, fileId):
	assert(len(pathId) == 16)
	assert(fileId is None or len(fileId) == 20)
        assert(not self.redirect())
	self.idMap[pathId] = (path, fileId, version)

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
                 byDefault = True):
	"""
	Adds a single version of a trove.

	@param name: name of the trove
	@type name: str
	@param version: version of the trove
	@type version: versions.Version
	@param flavor: flavor of the trove to include
	@type flavor: deps.deps.DependencySet
	@param presentOkay: replace if this is a duplicate, don't complain
	@type presentOkay: boolean
	"""
	if not presentOkay and self.troves.has_key((name, version, flavor)):
	    raise TroveError, "duplicate trove included in %s" % self.name()
	self.troves[(name, version, flavor)] = byDefault

    def delTrove(self, name, version, flavor, missingOkay):
	"""
	Removes a single version of a trove.

	@param name: name of the trove
	@type name: str
	@param version: version of the trove
	@type version: versions.Version
	@param flavor: flavor of the trove to include
	@type flavor: deps.deps.DependencySet
	@param missingOkay: should we raise an error if the version isn't
	part of this trove?
	@type missingOkay: boolean
	"""
	if self.troves.has_key((name, version, flavor)):
	    del self.troves[(name, version, flavor)]
	elif missingOkay:
	    pass
	else:
	    # FIXME, we should have better text here
	    raise TroveError

    def iterTroveList(self):
	"""
	Returns a generator for (name, version, flavor) ordered pairs, 
	listing all of the trove in the group, along with their versions. 

	@rtype: list
	"""
	return self.troves.iterkeys()

    def includeTroveByDefault(self, name, version, flavor):
        return self.troves[(name, version, flavor)]

    def hasTrove(self, name, version, flavor):
	return self.troves.has_key((name, version, flavor))

    def mergeCollections(self, primaryDeriv, secondaryDeriv, excludeTroveList):

        def diffJob(derivativeTrove):
            trvCs, files, troves = derivativeTrove.diff(self)
            assert(not files)

            job = []
            replaces = {}
            replaced = {}

            for (name, oldVersion, newVersion, oldFlavor, newFlavor) in troves:
                if newVersion:
                    byDef = derivativeTrove.includeTroveByDefault(
                                                name, newVersion, newFlavor)
                else:
                    # for erase byDef is meaningless
                    byDef = False

                job.append((name, (oldVersion, oldFlavor),
                                  (newVersion, newFlavor), byDef))

            for (name, changeList) in trvCs.iterChangedTroves():
                for (changeType, version, flavor, byDef) in changeList:
                    if changeType == '~':
                        job.append((name, (version, flavor),
                                          (version, flavor), byDef))
            return set(job)

        def applyJob(jobSet, skipNotByDefault = False, excludeTroves = None):
            for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), 
                 byDef) in jobSet:
                if oldVersion is not None:
                    self.delTrove(name, oldVersion, oldFlavor, byDef)
                if newVersion is not None and \
                        (oldVersion or not skipNotByDefault or byDef):
                    if not excludeTroves or not excludeTroves.match(name):
                        self.addTrove(name, newVersion, newFlavor, 
                                      byDefault = byDef)

        # Trove changesets suck. They don't store the A->B for a trove
        # inclusion, they store -A and +B. The A->B relationship can
        # be inferred, but having to do so is dumb. This operation really
        # ought to work on two trove changesets. It works on the two other
        # troves (and calculates the rest) becaues of this flaw.
        #import lib
        #lib.epdb.st()

        primaryJob = diffJob(primaryDeriv)
        secondaryJob = diffJob(secondaryDeriv)

        # things that we have in common are easy
        common = primaryJob & secondaryJob
        primaryJob = primaryJob - common
        secondaryJob = secondaryJob - common

        applyJob(common)

        # build an index to the secondary job set
        secondaryIndex = {}
        for job in secondaryJob:
            (name, oldInfo, newInfo, byDefault) = job
            assert((name, (oldInfo[0], oldInfo[1])) not in secondaryIndex)
            assert((name, (newInfo[0], newInfo[1])) not in secondaryIndex)
            if oldInfo[0]:
                secondaryIndex[(name, oldInfo)] = job
            if newInfo[0]:
                secondaryIndex[(name, newInfo)] = job

        primaryRemoveList = []
        for job in primaryJob:
            (name, oldInfo, newInfo, byDefault) = job

            oldOverlap = secondaryIndex.get((name, oldInfo), None)
            newOverlap = secondaryIndex.get((name, newInfo), None)

            if oldOverlap is None and newOverlap is None:
                # it doesn't overlap with the secondary job set
                continue

            keepSecondary = True
            keepPrimary = True

            if oldOverlap == newOverlap:
                # both primary and secondary jobs are updating from the same 
                # trove and updating to the same trove.  They also both 
                # started out with the same byDefault value.
                # The primary and secondary job must differ by their 
                # byDefault value.  Keep whichever update twiddles the by
                # default bit.
                origByDefault = self.includeTroveByDefault(name, 
                                                     oldInfo[0], oldInfo[1])
                if byDefault == origByDefault:
                    keepPrimary = False
                else:
                    keepSecondary = False
            elif oldOverlap is None:
                # They diverge from different places and end up at the same
                # place. This can't happen since both diffs are from the
                # same trove (self).
                assert(0)
            else:
                # Both troves started from the same place, and diverged to
                # different troves. Here are the rules:
                #   1. If the trove was removed from the primary, leave
                #      it removed.
                #   2. If the trove was removed from the secondary, it needs
                #      to be removed
                #   3. If the new troves are on the same branch, take the
                #      newer one.
                #   4. Otherwise, take the trove from the primary (it's
                #      not clear this case is possible given how linkages
                #      are maintained)
                assert(newOverlap is None)
                if job[2] == (None, None):
                    keepSecondary = False
                elif oldOverlap[2][0] is None:
                    keepPrimary = False
                elif job[2][0].branch() == oldOverlap[2][0].branch():
                    if job[2][0].isAfter(oldOverlap[2][0]):
                        keepSecondary = False
                    else:
                        keepPrimary = False
                else:
                    keepSecondary = False

            assert(not keepPrimary or not keepSecondary)

            if not keepPrimary:
                primaryRemoveList.append(job)

            if not keepSecondary:
                if oldOverlap is not None:
                    if oldOverlap[1][0] is not None:
                        del secondaryIndex[(oldOverlap[0], oldOverlap[1])]
                    if oldOverlap[2][0] is not None and \
                                oldOverlap[1][0] != oldOverlap[2][0]:
                        # the overlaps are the same if only the byDefault
                        # flag changed
                        del secondaryIndex[(oldOverlap[0], oldOverlap[2])]
                    secondaryJob.remove(oldOverlap)

        for job in primaryRemoveList:
            primaryJob.remove(job)

        applyJob(primaryJob)
        applyJob(secondaryJob, skipNotByDefault = True,
                 excludeTroves = excludeTroveList)

    # returns a dictionary mapping a pathId to a (path, version, trvName) tuple
    def applyChangeSet(self, trvCs, skipIntegrityChecks = False):
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

        assert(not self.immutable)

	self.redirect.set(trvCs.getIsRedirect())
        if self.redirect():
            # we don't explicitly remove files for redirects
            self.idMap = TroveRefsFilesStream()

	fileMap = {}

	for (pathId, path, fileId, fileVersion) in trvCs.getNewFileList():
	    self.addFile(pathId, path, fileVersion, fileId)
	    fileMap[pathId] = self.idMap[pathId] + \
                                (self.name(), None, None, None)

	for (pathId, path, fileId, fileVersion) in trvCs.getChangedFileList():
	    (oldPath, oldFileId, oldVersion) = self.idMap[pathId]
	    self.updateFile(pathId, path, fileVersion, fileId)
	    # look up the path/version in self.idMap as the ones here
	    # could be None
	    fileMap[pathId] = self.idMap[pathId] + \
                                (self.name(), oldPath, oldFileId, oldVersion)

	for pathId in trvCs.getOldFileList():
	    self.removeFile(pathId)

	self.mergeTroveListChanges(trvCs.iterChangedTroves())
	self.flavor.set(trvCs.getNewFlavor())
	self.changeLog = trvCs.getChangeLog()
	self.setProvides(trvCs.getProvides())
	self.setRequires(trvCs.getRequires())
	self.changeVersion(trvCs.getNewVersion())
	self.changeFlavor(trvCs.getNewFlavor())

        if not trvCs.getOldVersion():
            self.troveInfo = TroveInfo(trvCs.getTroveInfoDiff())
        else:
            self.troveInfo.twm(trvCs.getTroveInfoDiff(), self.troveInfo)

        if not skipIntegrityChecks:
            # if we have a sha1 in our troveinfo, verify it
            if self.troveInfo.sigs.sha1():
                if not self.verifySignatures():
                    raise TroveIntegrityError
            else:
                #log.warning('changeset does not contain a sha1 checksum')
                pass

        assert((not self.idMap) or (not self.troves))

	return fileMap

    def mergeTroveListChanges(self, changeList, redundantOkay = False):
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

        #import lib
        #lib.epdb.st('f')

        for (name, list) in changeList:
            for (oper, version, flavor, byDefault) in list:
                if oper == '+':
                    self.addTrove(name, version, flavor,
                                           presentOkay = redundantOkay,
                                           byDefault = byDefault)

                elif oper == "-":
                    self.delTrove(name, version, flavor,
                                           missingOkay = redundantOkay)
                elif oper == "~":
                    self.troves[(name, version, flavor)] = byDefault
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
	if self.isRedirect() != them.isRedirect():
	    return False

	(csg, pcl, fcl) = self.diff(them)
	return (not pcl) and (not fcl) and (not csg.getOldFileList()) \
            and self.getRequires() == them.getRequires() \
            and self.getProvides() == them.getProvides() \
            and self.getTroveInfo() == them.getTroveInfo() \
            and not([x for x in csg.iterChangedTroves()])


    def __ne__(self, them):
	return not self == them

    def diff(self, them, absolute = 0):
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

	assert(not them or self.name() == them.name())
        assert((not self.idMap) or (not self.troves))
        assert((not them) or (not them.idMap) or (not them.troves))

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
                                    isRedirect = self.redirect(),
                                    troveInfoDiff = troveInfoDiff)
	else:
	    themMap = {}
	    chgSet = TroveChangeSet(self.name(), self.changeLog,
				      None, self.getVersion(),
				      None, self.getFlavor(),
                                      None, self.getSigs(),
				      absolute = absolute,
                                      isRedirect = self.redirect(),
                                      troveInfoDiff = self.troveInfo.freeze())

	# dependency and flavor information is always included in total;
	# this lets us do dependency checking w/o having to load troves
	# on the client
        chgSet.setRequires(self.requires())
        chgSet.setProvides(self.provides())

	removedIds = []
	addedIds = []
	sameIds = {}
	filesNeeded = []

        if not self.redirect():
            # we just ignore file information for redirects
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
	added = {}
	removed = {}

	for key in self.troves.iterkeys():
	    if them and them.troves.has_key(key): 
                if self.troves[key] != them.troves[key]:
                    chgSet.changedTrove(key[0], key[1], key[2],
                                        self.troves[key])
                continue

	    (name, version, flavor) = key
	    chgSet.newTroveVersion(name, version, flavor, self.troves[key])

            d = added.setdefault(name, {})
            l = d.setdefault(flavor, [])
            l.append(version)

	if them:
	    for key in them.troves.iterkeys():
		if self.troves.has_key(key): continue

		(name, version, flavor) = key
		chgSet.oldTroveVersion(name, version, flavor)
                d = removed.setdefault(name, {})
                l = d.setdefault(flavor, [])
                l.append(version)

	trvList = []

	if absolute:
	    for name in added.keys():
		for flavor in added[name]:
		    for version in added[name][flavor]:
			trvList.append((name, None, version, None, flavor))
	    return (chgSet, filesNeeded, trvList)

	# use added and removed to assemble a list of trove diffs which need
	# to go along with this change set

	for name in added.keys(): 
	    if not removed.has_key(name):
		# there isn't anything which disappeared that has the same
		# name; this must be a new addition
		for newFlavor in added[name]:
		    for version in added[name][newFlavor]:
			trvList.append((name, None, version, None, newFlavor))
		del added[name]

	changePair = []
	# for things that are left, see if we can match flavors 
	for name in added.keys():
            if not removed.has_key(name):
                # nothing to match up against
                continue

            # try to match flavors by branch first - take bad matches
            # that are on the same branch over perfect matches that are
            # not.

            # To that end, sort flavors by branch.  Search over
            # the flavors on a particular branch first.  If those flavors
            # match, remove from the main added/removed list
            addedByBranch = {}
            removedByBranch = {}
            for flavor, versionList in added[name].iteritems():
                for version in versionList:
                    d = addedByBranch.setdefault(version.branch(), {})
                    d.setdefault(flavor, []).append(version)

            for flavor, versionList in removed[name].iteritems():
                for version in versionList:
                    d = removedByBranch.setdefault(version.branch(), {})
                    d.setdefault(flavor, []).append(version)

            searchOrder = []
            for branch, addedFlavors in addedByBranch.iteritems():
                removedFlavors = removedByBranch.get(branch, False)
                if removedFlavors:
                    searchOrder.append((addedFlavors, removedFlavors))

            # after we've found all the matches we could while staying
            # with branch affinity, try to match up all added/removed
            # troves with this name that remain
            searchOrder.append((added[name], removed[name]))

            scoreCache = {}
            NEG_INF = -99999

            for addedSource, removedSource in searchOrder:
                found = True
                used = set()

                while found:
                    found = False
                    maxScore = None
                    # score every new flavor against every old flavor 
                    # to find the best match.  Doing anything less
                    # may result in incorrect flavor lineups
                    for newFlavor in addedSource.keys():
                        if newFlavor in used:
                            continue
                        if not newFlavor:
                            if newFlavor in removedSource:
                                # match up empty flavors immediately
                                maxScore = (9999, newFlavor, newFlavor)
                                break
                            else:
                                continue
                        for oldFlavor in removedSource.keys():
                            if not oldFlavor or oldFlavor in used:
                                continue
                            myMax = scoreCache.get((newFlavor, 
                                                     oldFlavor), None)
                            if myMax is None:
                                # check for superset matching and subset
                                # matching.  Currently we don't consider 
                                # a superset flavor match "better" than 
                                # a subset - if we want to change that, 
                                # a initial parameter for maxScore that 
                                # ordered scores by type would work.
                                # If we do that, we should consider adding
                                # heuristic to prefer strongly satisfied
                                # flavors most of all. 
                                scores = (newFlavor.score(oldFlavor),
                                          oldFlavor.score(newFlavor))
                                scores = [x for x in scores if x is not False]
                                if scores:
                                    myMax = max(scores)
                                else:
                                    myMax = NEG_INF
                                scoreCache[newFlavor, oldFlavor] = myMax
                            if not maxScore or myMax > maxScore[0]:
                                maxScore = (myMax, newFlavor, oldFlavor)
                    if maxScore and maxScore[0] > NEG_INF:
                        found = True
                        newFlavor, oldFlavor = maxScore[1:]
                        used.update((newFlavor, oldFlavor))
                    if found:
                        changePair.append((name, addedSource[newFlavor][:], 
                                           newFlavor, 
                                           removedSource[oldFlavor][:],
                                           oldFlavor))
                        lst = added[name][newFlavor]
                        # remove these versions from the final "added"
                        # list.
                        for version in addedSource[newFlavor][:]:
                            lst.remove(version)
                        if not lst:
                            del added[name][newFlavor]

                        lst = removed[name][oldFlavor]
                        for version in removedSource[oldFlavor][:]:
                            lst.remove(version)
                        if not lst:
                            del removed[name][oldFlavor]

	    if not added[name]:
		del added[name]
	    if not removed[name]:
		del removed[name]
	    
	for name in added.keys():
	    if len(added[name]) == 1 and removed.has_key(name) and \
                        len(removed[name]) == 1:
		# one of each? they *must* be a good match...
		newFlavor = added[name].keys()[0]
		oldFlavor = removed[name].keys()[0]
		changePair.append((name, added[name][newFlavor], newFlavor, 
				   removed[name][oldFlavor], oldFlavor))
		del added[name]
		del removed[name]
		continue

	for name in added.keys():
	    for newFlavor in added[name].keys():
		# no good match. that's too bad
		changePair.append((name, added[name][newFlavor], newFlavor, 
				   None, None))

	for (name, newVersionList, newFlavor, oldVersionList, oldFlavor) \
		    in changePair:
	    assert(newVersionList)

	    if not oldVersionList:
		for newVersion in newVersionList:
		    trvList.append((name, None, newVersion, 
					  None, newFlavor))
                continue

	    # for each new version of a trove, try and generate the diff
	    # between that trove and the version of the trove which was
	    # removed which was on the same branch. if that's not possible,
	    # see if the parent of the trove was removed, and use that as
	    # the diff. if we can't do that and only one version of this
	    # trove is being obsoleted, use that for the diff. if we
	    # can't do that either, throw up our hands in a fit of pique

	    for version in newVersionList:


		if not oldVersionList:
		    # no nice match, that's too bad
		    trvList.append((name, None, version, None, newFlavor))
                    continue
		elif len(oldVersionList) == 1:
		    trvList.append((name, oldVersionList[0], version, 
				    oldFlavor, newFlavor))
		    del oldVersionList[0]
                    continue

		branch = version.branch()

		if branch.hasParentBranch():
		    parentBranch = branch.parentBranch()
		else:
		    parentBranch = None

                if version.hasParentVersion():
                    parentVersion = version.parentVersion()
		else:
		    parentVersion = None



                sameBranches = []
                parentBranches = []
                childBranches = []
                parentNodes = []
                childNodes = []

                for other in oldVersionList:
                    if other.branch() == branch:
                        sameBranches.append(other)
                    if parentVersion and other == parentVersion:
                        parentNodes.append(other)
                    if parentBranch and other.branch() == parentBranch:
                        parentBranches.append(other)
                    if other.hasParentVersion():
                        if other.parentVersion() == version:
                            childNodes.append(other)
                    if other.branch().hasParentBranch():
                        if other.branch().parentBranch() == branch:
                            childBranches.append(other)

                # none is a sentinel
                priority = [ sameBranches, parentNodes, childNodes, 
                             parentBranches, childBranches, None ]

                for match in priority:
                    if match:
                        break

                if match and len(match) == 1:
                    match = match[0]
                    oldVersionList.remove(match)
                    trvList.append((name, match, version, 
                                    oldFlavor, newFlavor))
                else:
                    # Here's the fit of pique. This shouldn't happen
                    # except for the most ill-formed of groups.
                    raise TroveDiffError, "Cannot determine what trove is " \
                        "being replaced for %s=%s[%s]" %  \
                            (name, version.asString(), 
                             deps.formatFlavor(newFlavor))

	    # remove old versions which didn't get matches
	    for oldVersion in oldVersionList:
		trvList.append((name, oldVersion, None, oldFlavor, None))

        for name, flavorList in removed.iteritems():
            for flavor, versionList in flavorList.iteritems():
                for version in versionList:
                    trvList.append((name, version, None, flavor, None))

	return (chgSet, filesNeeded, trvList)

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
            return self.troveInfo.isCollection.set(1)
        else:
            return self.troveInfo.isCollection.set(0)

    def isCollection(self):
        return self.troveInfo.isCollection()

    def setBuildRequirements(self, itemList):
        for (name, ver, release) in itemList:
            self.troveInfo.buildReqs.add(name, ver, release)

    def getBuildRequirements(self):
        return [ (x[1].name(), x[1].version(), x[1].flavor()) 
                         for x in self.troveInfo.buildReqs.iterAll() ]

    def setLoadedTroves(self, itemList):
        for (name, ver, release) in itemList:
            self.troveInfo.loadedTroves.add(name, ver, release)

    def getLoadedTroves(self):
        return [ (x[1].name(), x[1].version(), x[1].flavor()) 
                 for x in self.troveInfo.loadedTroves.iterAll() ]

    def setInstallBucket(self, installBucket):
        for (key, value) in sorted(installBucket.iteritems()):
            self.troveInfo.installBucket.add(key, value)

    def getInstallBucket(self):
        return self.troveInfo.installBucket

    def __init__(self, name, version, flavor, changeLog, isRedirect = False):
        streams.LargeStreamSet.__init__(self)
        assert(flavor is not None)
	self.name.set(name)
	self.version.set(version)
	self.flavor.set(flavor)
        if changeLog:
            self.changeLog.thaw(changeLog.freeze())
        self.redirect.set(isRedirect)
        self.immutable = False

class ReferencedTroveSet(dict, streams.InfoStream):

    def freeze(self, skipSet = {}):
	l = []
	for name, troveList in self.iteritems():
	    subL = []
	    for (change, version, flavor, byDefault) in troveList:
		version = version.freeze()
		if flavor:
		    flavor = flavor.freeze()
		else:
		    flavor = "-"

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
		    flavor = deps.DependencySet()
		else:
		    flavor = deps.ThawDependencySet(flavor)

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
	if not data:
	    return

	i = 0
	while i < len(data):
	    pathId = data[i:i+16]
	    i += 16

	    pathLen = struct.unpack("!H", data[i:i+2])[0]
	    i += 2
	    if pathLen:
		path = data[i:i + pathLen]
		i += pathLen
	    else:
		path = None

	    fileIdLen = struct.unpack("!H", data[i:i+2])[0]
	    i += 2
	    if fileIdLen:
                assert(fileIdLen == 20)
		fileId = data[i:i+20]
		i += fileIdLen
	    else:
		fileIdLen = None

	    versionLen = struct.unpack("!H", data[i:i+2])[0]
	    i += 2
	    if versionLen:
		version = versions.VersionFromString(data[i:i + versionLen])
		i += versionLen
	    else:
		version = None

	    self.append((pathId, path, fileId, version))

    def __init__(self, data = None):
	list.__init__(self)
	if data is not None:
	    self.thaw(data)

_STREAM_TCS_NAME	    =  0
_STREAM_TCS_OLD_VERSION	    =  1
_STREAM_TCS_NEW_VERSION	    =  2
_STREAM_TCS_REQUIRES	    =  3
_STREAM_TCS_PROVIDES	    =  4
_STREAM_TCS_CHANGE_LOG	    =  5
_STREAM_TCS_OLD_FILES	    =  6
_STREAM_TCS_TYPE	    =  7
_STREAM_TCS_TROVE_CHANGES   =  8
_STREAM_TCS_NEW_FILES       =  9
_STREAM_TCS_CHG_FILES       = 10
_STREAM_TCS_OLD_FLAVOR      = 11
_STREAM_TCS_NEW_FLAVOR      = 12
_STREAM_TCS_IS_REDIRECT     = 13
_STREAM_TCS_TROVEINFO       = 14
_STREAM_TCS_OLD_SIGS        = 15
_STREAM_TCS_NEW_SIGS        = 16

_TCS_TYPE_ABSOLUTE = 1
_TCS_TYPE_RELATIVE = 2

class AbstractTroveChangeSet(streams.LargeStreamSet):

    streamDict = { 
	_STREAM_TCS_NAME	: (streams.StringStream, "name"          ),
        _STREAM_TCS_OLD_VERSION : (FrozenVersionStream,  "oldVersion"    ),
        _STREAM_TCS_NEW_VERSION : (FrozenVersionStream,  "newVersion"    ),
        _STREAM_TCS_REQUIRES    : (DependenciesStream,   "requires"      ),
        _STREAM_TCS_PROVIDES    : (DependenciesStream,   "provides"      ),
        _STREAM_TCS_CHANGE_LOG  : (ChangeLog,            "changeLog"     ),
        _STREAM_TCS_OLD_FILES   : (OldFileStream,	 "oldFiles"      ),
        _STREAM_TCS_TYPE        : (streams.IntStream,    "tcsType"       ),
        _STREAM_TCS_TROVE_CHANGES:(ReferencedTroveSet,   "troves"        ),
        _STREAM_TCS_NEW_FILES   : (ReferencedFileList,   "newFiles"      ),
        _STREAM_TCS_CHG_FILES   : (ReferencedFileList,   "changedFiles"  ),
        _STREAM_TCS_OLD_FLAVOR  : (DependenciesStream,   "oldFlavor"     ),
        _STREAM_TCS_NEW_FLAVOR  : (DependenciesStream,   "newFlavor"     ),
        _STREAM_TCS_IS_REDIRECT : (ByteStream,           "isRedirect"    ),
        _STREAM_TCS_TROVEINFO   : (streams.StringStream, "troveInfoDiff" ),
        _STREAM_TCS_OLD_SIGS    : (TroveSignatures,      "oldSigs"       ),
        _STREAM_TCS_NEW_SIGS    : (TroveSignatures,      "newSigs"       ),
    }
    _streamDict = streams.StreamSetDef(streamDict)

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

    def resetNewFileList(self):
        self.newFiles = []

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

    def changeNewVersion(self, version):
	self.newVersion.set(version)

    def changeChangeLog(self, cl):
        assert(0)
	self.changeLog.thaw(cl.freeze())

    def getOldVersion(self):
	return self.oldVersion()

    def getNewVersion(self):
	return self.newVersion()

    def getOldSigs(self):
        return self.oldSigs

    def getNewSigs(self):
        return self.newSigs

    # path and/or version can be None
    def changedFile(self, pathId, path, fileId, version):
	self.changedFiles.append((pathId, path, fileId, version))

    def resetChangedFileList(self):
        self.changedFiles = []

    def getChangedFileList(self):
	return self.changedFiles

    def hasChangedFiles(self):
        return (len(self.newFiles) + len(self.changedFiles) + 
                len(self.oldFiles)) != 0

    def iterChangedTroves(self):
	return self.troves.iteritems()

    def newTroveVersion(self, name, version, flavor, byDefault):
	"""
	Adds a version of a troves which appeared in newVersion.

	@param name: name of the trove
	@type name: str
	@param version: new version
	@type version: versions.Version
	@param flavor: new flavor
	@type flavor: deps.deps.DependencySet
        @param byDefault: value of byDefault
        @type byDefault: boolean
	"""

        l = self.troves.setdefault(name, [])
	l.append(('+', version, flavor, byDefault))

    def updateChangedTrove(self, name, flavor, old, new):
	"""
	Removes trove (name, flavor, old version) from the changed list and
	adds trove (name, flavor, version) new to the list (with the same 
	change type).

	@param name: name of the trove
	@type name: str
	@param flavor: flavor of the trove
	@type flavor: deps.deps.DependencySet
	@param old: version to remove from the changed list
	@type old: versions.VersionString
	@param new: version to add to the changed list
	@type new: versions.VersionString
	"""
	for (theName, l) in self.troves.iteritems():
	    if theName != name: continue
	    for (i, (change, ver, flavor, byDefault)) in enumerate(l):
		if ver == old:
		    l[i] = (change, new, flavor, byDefault)
		    return

        raise TroveError, "trove not found to update"

    def oldTroveVersion(self, name, version, flavor):
	"""
	Adds a version of a trove which appeared in oldVersion.

	@param name: name of the trove
	@type name: str
	@param version: old version
	@type version: versions.Version
	@param flavor: old flavor
	@type flavor: deps.deps.DependencySet
	"""
        l = self.troves.setdefault(name, [])
        l.append(('-', version, flavor, None))

    def changedTrove(self, name, version, flavor, byDefault):
	"""
	Records the change in the byDefault setting of a referenced trove.

	@param name: name of the trove
	@type name: str
	@param version: version
	@type version: versions.Version
	@param flavor: flavor
	@type flavor: deps.deps.DependencySet
        @param byDefault: New value of byDefault
        @type byDefault: boolean
	"""
        l = self.troves.setdefault(name, [])
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
        if self.getRequires():
            depformat('Requires', self.getRequires(), f)
        if self.getProvides():
            depformat('Provides', self.getProvides(), f)
        if self.getOldFlavor():
            depformat('Old Flavor', self.getOldFlavor(), f)
        if self.getNewFlavor():
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

	for name in self.troves.keys():
            l = []
            for x in self.troves[name]:
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

    def setIsRedirect(self, val):
        assert(type(val) == bool)
        self.isRedirect.set(val)

    def getIsRedirect(self):
        return self.isRedirect()

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

class TroveChangeSet(AbstractTroveChangeSet):

    _streamDict = AbstractTroveChangeSet._streamDict

    def __init__(self, name, changeLog, oldVersion, newVersion, 
		 oldFlavor, newFlavor, oldSigs, newSigs,
                 absolute = 0, isRedirect = False,
                 troveInfoDiff = None):
	AbstractTroveChangeSet.__init__(self)
	assert(isinstance(newVersion, versions.AbstractVersion))
	assert(isinstance(newFlavor, deps.DependencySet))
	assert(oldFlavor is None or isinstance(oldFlavor, deps.DependencySet))
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
        self.isRedirect.set(isRedirect)
        assert(troveInfoDiff is not None)
        self.troveInfoDiff.set(troveInfoDiff)
        if oldSigs:
            self.oldSigs.thaw(oldSigs.freeze())
        self.newSigs.thaw(newSigs.freeze())

class ThawTroveChangeSet(AbstractTroveChangeSet):

    _streamDict = AbstractTroveChangeSet._streamDict

    def __init__(self, buf):
	AbstractTroveChangeSet.__init__(self, buf)

class TroveError(Exception):

    """
    Ancestor for all exceptions raised by the trove module.
    """

    pass

class ParseError(TroveError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

class PatchError(TroveError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

class TroveDiffError(TroveError):
    """
    Indicates that an error occurred calculating differences between troves.
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
    pass
