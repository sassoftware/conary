#
# Copyright (c) 2005-2008 rPath, Inc.
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

import os
import sys
import getpass
import tempfile
import subprocess
from time import time

from conary import callbacks
from conary.lib.util import log
from conary.lib import graph

from Crypto.PublicKey import DSA
from openpgpfile import BadPassPhrase
from openpgpfile import PGP_Signature
from openpgpfile import getKeyTrust
from openpgpfile import KeyNotFound
from openpgpfile import num_getRelPrime
from openpgpfile import seekKeyById
from openpgpfile import SEEK_SET, SEEK_END

#-----#
#OpenPGPKey structure:
#-----#

class OpenPGPKey(object):
    __slots__ = ['fingerprint', 'cryptoKey', 'revoked', 'timestamp',
                 'trustLevel', 'signatures', 'id']
    def __init__(self, key, cryptoKey, trustLevel=255):
        """
        Instantiates a OpenPGPKey object

        @param key: A PGP key
        @type key: instance of openpgpfile.PGP_Key
        @param cryptoKey: DSA or RSA key object
        @type cryptoKey: instance
        @param trustLevel: the trust level of this key, as stored locally
        @type trustLevel: int
        """

        self.id = key.getKeyId()
        self.fingerprint = key.getKeyFingerprint()
        self.cryptoKey = cryptoKey
        self.revoked, self.timestamp = key.getEndOfLife()
        self.trustLevel = trustLevel
        self.signatures = []
        self._initSignatures(key)

    def _initSignatures(self, key):
        # Iterate over this packet's signatures
        sigs = {}
        keyId = key.getKeyId()
        for sig in key.iterCertifications():
            # Ignore self signatures
            sigKeyId = sig.getSignerKeyId()
            if sigKeyId == keyId[-16:]:
                continue
            # XXX We should deal with conflict here
            if sigKeyId in sigs:
                continue
            trustLevel, trustAmount, trustRegex = sig.getTrust()
            sigs[sigKeyId] = OpenPGPKeySignature(
                    sigId = sig.getSignatureHash(),
                    signer = sigKeyId,
                    signature = sig.parseMPIs(),
                    pubKeyAlg = sig.pubKeyAlg,
                    hashAlg = sig.hashAlg,
                    creation = sig.getCreation(),
                    expiration = sig.getExpiration(),
                    trustLevel = trustLevel,
                    trustAmount = trustAmount,
                    trustRegex = trustRegex)
        self.signatures = sorted(sigs.values(), key = lambda x: x.signer)

    def getTrustLevel(self):
        return self.trustLevel

    def isRevoked(self):
        return self.revoked

    def getKeyId(self):
        return self.id

    def getFingerprint(self):
        return self.fingerprint

    def getTimestamp(self):
        return self.timestamp

    def signString(self, data):
        if isinstance(self.cryptoKey,(DSA.DSAobj_c, DSA.DSAobj)):
            K = self.cryptoKey.q + 1
            while K > self.cryptoKey.q:
                K = num_getRelPrime(self.cryptoKey.q)
        else:
            K = 0
        timeStamp = int(time())
        return (self.fingerprint, timeStamp,
                self.cryptoKey.sign(data+str(timeStamp), K))

    def verifyString(self, data, sig):
        """
        verifies a digital signature

        returns -1 if the signature does not verify.  Otherwise it returns
        the trust value of the public key that corresponds to the private
        key that signed the data.

        @param data: the data that has been signed
	@type data: strint
	@param sig: the digital signature to verify
	@type sig: 4-tuple (fingerprint, timestamp, signature, K)
        @rtype int
        """
        # this function was not designed to throw an exception at this level
        # because in some cases the calling function wants to aggregate a list
        # of failed/passed signatures all at once.

        if (self.fingerprint == sig[0]
            and self.cryptoKey.verify(data+str(sig[1]), sig[2])):
            return self.trustLevel
        else:
            return -1

class OpenPGPKeySignature(object):
    __slots__ = ['sigId', 'signer', 'creation', 'expiration', 'revocation',
                 'trustLevel', 'trustAmount', 'pubKeyAlg', 'hashAlg',
                 'signature', '_verifies']
    """A key signature on a key"""
    def __init__(self, **kwargs):
        self.sigId = kwargs.pop('sigId')
        self.signer = kwargs.pop('signer')
        self.creation = kwargs.pop('creation')
        self.pubKeyAlg = kwargs.pop('pubKeyAlg')
        self.hashAlg = kwargs.pop('hashAlg')
        self.signature = kwargs.pop('signature')
        self.expiration = kwargs.pop('expiration', None)
        self.revocation = kwargs.pop('revocation', None)
        self.trustLevel = kwargs.pop('trustLevel', None)
        self.trustAmount = kwargs.pop('trustAmount', None)
        self._verifies = None

    def getSignerKeyId(self):
        return self.signer

    def verifies(self, keyRetrievalCallback):
        if self._verifies is not None:
            return self._verifies
        # We need to get the signer's crypto alg
        sigKey = keyRetrievalCallback(self.signer)
        self._verifies = PGP_Signature.verifySignature(self.sigId,
                sigKey.cryptoKey, self.signature, self.pubKeyAlg, self.hashAlg)
        return self._verifies

class _KeyNotFound(KeyNotFound):
    errorIsUncatchable = True

class OpenPGPKeyCache:
    """
    Base class for a key cache
    """
    def __init__(self):
        self.publicDict = {}
        self.privateDict = {}

    def getPublicKey(self, keyId):
        raise NotImplementedError

    def getPrivateKey(self, keyId, passphrase=None):
        raise NotImplementedError

    def reset(self):
        self.publicDict = {}
        self.privateDict = {}

class OpenPGPKeyFileCache(OpenPGPKeyCache):
    """
    OpenPGPKeyCache based object that reads keys from public and private
    keyrings
    """
    def __init__(self, callback = None):
        if callback is None:
            callback = callbacks.KeyCacheCallback()
        OpenPGPKeyCache.__init__(self)
        self.callback = callback
        if 'HOME' not in os.environ:
            self.publicPaths  = [ '/etc/conary/pubring.gpg' ]
            self.trustDbPaths = [ '/etc/conary/trustdb.gpg' ]
            self.privatePath  = None
        else:
            self.publicPaths  = [ os.environ['HOME'] + '/.gnupg/pubring.gpg',
                                  '/etc/conary/pubring.gpg' ]
            self.trustDbPaths = [ os.environ['HOME'] + '/.gnupg/trustdb.gpg',
                                  '/etc/conary/trustdb.gpg' ]
            self.privatePath  = os.environ['HOME'] + '/.gnupg/secring.gpg'

    def setPublicPath(self, path):
        if isinstance(path, list):
            self.publicPaths = path
        else:
            self.publicPaths = [ path ]

    def setTrustDbPath(self, path):
        self.trustDbPaths = [ path ]

    def addPublicPath(self, path):
        if isinstance(path, list):
            self.publicPaths.extend(path)
        else:
            self.publicPaths.append(path)

    def setPrivatePath(self, path):
        self.privatePath = path

    def setCallback(self, callback):
        self.callback = callback
        pubRing = callback.pubRing
        if pubRing not in self.publicPaths:
            self.addPublicPath(pubRing)
            trustDbPath = '/'.join(pubRing.split('/')[:-1]) + '/trustdb.gpg'
            self.trustDbPaths.append(trustDbPath)

    def getPublicKey(self, keyId, serverName = None, warn=True):
        # if we have this key cached, return it immediately
        if keyId in self.publicDict:
            return self.publicDict[keyId]

        # otherwise search for it
        for i in range(len(self.publicPaths)):
            try:
                publicPath = self.publicPaths[i]
                try:
                    trustDbPath = self.trustDbPaths[i]
                except:
                    pass

                # translate the keyId to a full fingerprint for consistency
                key = seekKeyById(keyId, publicPath)
                if not key:
                    continue
                trustLevel = getKeyTrust(trustDbPath, key.getKeyFingerprint())
                self.publicDict[keyId] = OpenPGPKey(key, key.getCryptoKey(),
                                                    trustLevel)
                return self.publicDict[keyId]
            except (KeyNotFound, IOError):
                pass
        # callback should only return True if it found the key.
        if serverName and self.callback.getPublicKey(keyId, serverName,
                                                     warn=warn):
            return self.getPublicKey(keyId, warn=warn)
        raise KeyNotFound(keyId)

    def getPrivateKey(self, keyId, passphrase=None):
        if keyId in self.privateDict:
            return self.privateDict[keyId]

        # translate the keyId to a full fingerprint for consistency
        key = seekKeyById(keyId, self.privatePath)

        # if we were supplied a password, use it.  The caller will need
        # to deal with handling BadPassPhrase exceptions
        if passphrase is not None:
            cryptoKey = key.getCryptoKey(passphrase)
            self.privateDict[keyId] = OpenPGPKey(key, cryptoKey)
            return self.privateDict[keyId]

        # next, see if the key has no passphrase (WHY???)
        # if it's readable, there's no need to prompt the user
        try:
            cryptoKey = key.getCryptoKey('')
            self.privateDict[keyId] = OpenPGPKey(key, cryptoKey)
            return self.privateDict[keyId]
        except BadPassPhrase:
            pass

        # FIXME: make this a callback
        print "\nsignature key is: %s"% keyId

        tries = 0
        while tries < 3:
            # FIXME: make this a callback
            passPhrase = getpass.getpass("Passphrase: ")
            try:
                cryptoKey = key.getCryptoKey(passPhrase)
                self.privateDict[keyId] = OpenPGPKey(key, cryptoKey)
                return self.privateDict[keyId]
            except BadPassPhrase:
                print "Bad passphrase. Please try again."
            tries += 1

        raise BadPassPhrase

#-----#
#OpenPGPKeyFinder: download missing keys from conary servers.
#-----#
class KeyCacheCallback(callbacks.KeyCacheCallback):
    gpgBin = 'gpg'
    def _getGPGCommonArgs(self, homeDir):
        return [self.gpgBin, '-q', '--no-tty',
                '--homedir', homeDir,
                '--no-greeting', '--no-secmem-warning',
                '--no-verbose', '--no-mdc-warning',
                '--no-default-keyring',
                '--keyring', os.path.basename(self.pubRing),
                '--batch', '--no-permission-warning',
                ]
    def _getGPGExtraArgs(self, source, keyId, warn=True):
        """Returns extra arguments to pass to GPG, and an optional stream to
        be used as standard input"""
        return [
                '--keyserver', '%sgetOpenPGPKey?search=%s' %(source, keyId),
                '--keyserver-options', 'timeout=3',
                '--recv-key', keyId,
        ], None

    def _normalizeKeySource(self, source):
        """Munge the source before passing it to GPG"""
        server = source
        # don't depend on repoMap entries ending with /
        if server[-1] != '/':
            server += '/'
        # rewrite (and hope) that a URL that uses https:// can
        # use http:// just as well.  GPG doesn't ship with a key getter
        # that can access https:// servers.
        if server.startswith('https://'):
            server = server.replace('https://', 'http://')
        return server

    def findOpenPGPKey(self, source, keyId, warn=True):
        # if we can't exec gpg, go ahead and bail
        if not self.hasGPG:
            return

        pubRingPath = os.path.dirname(self.pubRing)

        source = self._normalizeKeySource(source)

        # check to see if there's an existing secret key.  If it was
        # already there, don't remove it.  Otherwise we should clean
        # it up.
        secringExists = os.path.exists(os.path.join(pubRingPath, 'secring.gpg'))

        # we don't care about any of the possible output from this process.
        # gpg is pretty cavalier about dumping random garbage to stdout/err
        # regardless of the command line options admonishing it not to.
        devnull = open(os.devnull, "w")
        gpgArgs = self._getGPGCommonArgs(pubRingPath)
        extraArgs, stdin = self._getGPGExtraArgs(source, keyId, warn=warn)
        gpgArgs.extend(extraArgs)
        try:
            p = subprocess.Popen(gpgArgs,
                                 stdin=stdin, stdout=devnull, stderr=devnull)
            p.communicate()
            # One should check p.returncode here
        except OSError, e:
            if e.errno == 2: # No such file or directory
                self.hasGPG = False
                if warn:
                    log.warning('gpg does not appear to be installed.  gpg '
                                'is required to import keys into the '
                                'conary public keyring.  Use "conary '
                                'update gnupg" to install gpg.')
            else:
                # Raise everything else
                raise

        if not secringExists:
            try:
                os.remove(pubRingPath + '/secring.gpg')
            except:
                pass

    def _formatSource(self, serverName):
        """Network-aware source formatter"""
        server = None
        if serverName not in (self.repositoryMap or []):
            server = "http://%s/conary/" % serverName
        else:
            server = self.repositoryMap[serverName]
        return server

    def getPublicKey(self, keyId, serverName, warn=True):
        keySource = self._formatSource(serverName)
        if keySource == None:
            return False

        # findOpenPGPKey can be smart enough to raise exceptions if the key
        # cannot be found
        try:
            self.findOpenPGPKey(keySource, keyId, warn=warn)
        except _KeyNotFound:
            return False

        # decide if we found the key or not.
        pkt = seekKeyById(keyId, self.pubRing)
        return (pkt is not None)

    def __init__(self, *args, **kw):
        callbacks.KeyCacheCallback.__init__(self, *args, **kw)
        self.hasGPG = True

class DiskKeyCacheCallback(KeyCacheCallback):
    """Retrieve keys from a directory - keys are saved as <keyid>.asc"""
    def _formatSource(self, source):
        """For the disk case, this is a no-op"""
        return source

    def _normalizeKeySource(self, source):
        """For the disk case, this is a no-op"""
        return source

    def _getGPGExtraArgs(self, source, keyId, warn=True):
        keyFile = os.path.join(self.dirSource, "%s.asc" % keyId.lower())
        if not os.access(keyFile, os.R_OK):
            raise _KeyNotFound(keyId)
        return ["--import", keyFile], None

    def __init__(self, dirSource, pubRing=''):
        KeyCacheCallback.__init__(self, pubRing=pubRing)
        self.dirSource = dirSource

class KeyringCacheCallback(KeyCacheCallback):
    """Retrieve keys from a keyring"""
    def _formatSource(self, source):
        """For the keyring case, this is a no-op"""
        return source

    def _normalizeKeySource(self, source):
        """For the keyring case, this is a no-op"""
        return source

    def _getGPGExtraArgs(self, source, keyId, warn=True):
        # Use gpg to fetch the key from a keyring

        if not os.access(self.srcKeyring, os.R_OK):
            # Keyring doesn't exist
            raise _KeyNotFound(keyId)

        cmd = [self.gpgBin,
               "--no-default-keyring",
               "--keyring", self.srcKeyring,
               "--armor",
               "--export", keyId]
        # Redirect stderr to /dev/null
        devnull = open(os.devnull, "w")
        try:
            p = subprocess.Popen(cmd, stdout = subprocess.PIPE, 
                                 stderr = devnull)
        except OSError, e:
            if e.errno == 2: # No such file or directory
                if warn:
                    log.warning('gpg does not appear to be installed.  gpg '
                                'is required to import keys into the '
                                'conary public keyring.  Use "conary '
                                'update gnupg" to install gpg.')
            raise _KeyNotFound(keyId)
        except Exception, e:
            if warn:
                log.warning('Error while executing gpg: %s' % e)
            raise _KeyNotFound(keyId)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise _KeyNotFound(keyId)

        if not stdout.startswith('-' * 5 + "BEGIN"):
            raise _KeyNotFound(keyId)

        # Redirect stdout to a temporary file
        fd, tempf = tempfile.mkstemp()
        os.unlink(tempf)
        sio = os.fdopen(fd, "w")
        sio.write(stdout)
        sio.seek(0)

        return [ "--import" ], sio

    def __init__(self, keyring, pubRing=''):
        KeyCacheCallback.__init__(self, pubRing=pubRing)
        self.srcKeyring = keyring

_keyCache = OpenPGPKeyFileCache()

def getKeyCache():
    """
    @return: the OpenPGP key cache
    @rtype: L{lib.openpgpkey.OpenPGPKeyFileCache}
    """
    global _keyCache
    return _keyCache

def setKeyCache(keyCache):
    global _keyCache
    _keyCache = keyCache

class Trust(object):
    depthLimit = 10
    marginals = 3

    def __init__(self, topLevelKeys, keyCache):
        self.topLevelKeys = topLevelKeys
        self.keyCache = keyCache
        self._graph = None
        # self._trust is a dictionary, keyed on the node index, and with 3
        # values: (node trust level, node trust amount, actual trust)
        # The first two are just caches of the values in a trust signature,
        # and only affect the values for this node's children (i.e. they
        # determine the amount of trust this node transmits to the nodes it
        # signed). If the cumulated actual trust for this node does not exceed
        # 120, this node is considered untrusted, and it will be ignored
        # completely in determining trust for other keys.
        self._trust = {}
        self._depth = {}
        # Requesting keys by the short name will populate _idMap
        self._idMap = {}

    def _getKey(self, keyId):
        try:
            key = self.keyCache.getPublicKey(keyId)
            realKeyId = key.getKeyId()
            if key != realKeyId:
                self._idMap[keyId] = realKeyId
            return key
        except KeyNotFound:
            return None

    def computeTrust(self, keyId):
        self._graph = graph.DirectedGraph()
        g = self._graph
        self._trust.clear()
        self._depth.clear()

        # Normalize the key
        key = self._getKey(keyId)
        if key is None:
            return {}, {}
        keyId = key.getKeyId()
        g.addNode(keyId)
        starts, finishes, trees, pred, depth = g.doBFS(
            start = keyId,
            getChildrenCallback = self.getChildrenCallback,
            depthLimit = self.depthLimit)

        # Start walking the tree in reverse order, validating the trust of
        # each signature
        gt = g.transpose()
        self._graph = gt

        topLevelKeyIds = [ self._getKey(x) for x in self.topLevelKeys ]
        topLevelKeyIds = [ x.getKeyId() for x in topLevelKeyIds if x is not None ]
        # Top-level keys are fully trusted
        topLevelKeyIds = [ x for x in topLevelKeyIds if x in g ]
        self._trust = dict((x, (self.depthLimit, 120, 120))
                            for x in topLevelKeyIds)

        tstart, tfinishes, ttrees, tpred, tdepth = gt.doBFS(
            start = topLevelKeyIds,
            getChildrenCallback = self.trustComputationCallback)
        self._depth = dict((g.get(x), y) for x, y in tdepth.items())
        return self._trust, self._depth

    def getTrust(self, keyId):
        keyId = self._idMap.get(keyId, keyId)
        return self._trust.get(keyId, None)

    def getDepth(self, keyId):
        keyId = self._idMap.get(keyId, keyId)
        return self._depth.get(keyId, None)

    def getChildrenCallback(self, nodeIdx):
        nodeId = self._graph.get(nodeIdx)
        try:
            node = self.keyCache.getPublicKey(nodeId)
        except KeyNotFound:
            return []

        for sig in node.signatures:
            self._graph.addEdge(nodeId, sig.getSignerKeyId())
        return self._graph.edges[nodeIdx]

    def trustComputationCallback(self, nodeIdx):
        gt = self._graph
        trust = self._trust
        classicTrust = int(120 / self.marginals)
        if 120 % self.marginals:
            classicTrust += 1

        nodeId = gt.get(nodeIdx)
        if nodeId not in trust:
            return []
        nodeSigLevel, nodeSigTrust, nodeTrust = trust[nodeId]
        if nodeSigLevel == 0 or nodeTrust < 120:
            # This node is not trusted, don't propagate its trust to children
            return []

        for snIdx in gt.edges[nodeIdx]:
            node = gt.get(snIdx)
            ntlev, ntamt, tramt = trust.setdefault(node,
                    (nodeSigLevel - 1, 120, 0))
            # Get the signature
            n = self.keyCache.getPublicKey(node)
            sig = [ x for x in n.signatures if x.getSignerKeyId() == nodeId ]
            assert(sig)
            sig = sig[0]

            if not sig.verifies(self.keyCache.getPublicKey):
                continue

            if sig.trustLevel is not None:
                ntlev = min(ntlev, sig.trustLevel)
            # If no trust amount is present, use the standard trust model
            # (self.marginals keys needed to introduce a trusted key.
            # Note this is limited to one level of intermediate trusted keys
            # only)
            amt = ((sig.trustAmount is None) and classicTrust) or sig.trustAmount
            ntamt = min(ntamt, amt)
            # Child node trust cannot exceed the parent's trust
            tramt = min(tramt + nodeSigTrust, nodeTrust)

            trust[node] = (ntlev, ntamt, tramt)

        return gt.edges[nodeIdx]

