#
# Copyright (c) 2005 rPath, Inc.
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

import os
import sys
from getpass import getpass
from time import time
from conary import callbacks

from Crypto.PublicKey import DSA
from Crypto.PublicKey import RSA
from Crypto.Util.number import getPrime
from openpgpfile import getPrivateKey
from openpgpfile import getPublicKey
from openpgpfile import getPublicKeyFromString
from openpgpfile import getFingerprint
from openpgpfile import getKeyEndOfLife
from openpgpfile import getKeyTrust
from openpgpfile import seekNextKey
from openpgpfile import seekKeyById
from openpgpfile import IncompatibleKey
from openpgpfile import BadPassPhrase
from openpgpfile import KeyNotFound
from openpgpfile import SEEK_SET, SEEK_CUR, SEEK_END

#-----#
#OpenPGPKey structure:
#-----#

class OpenPGPKey:
    def __init__(self, fingerprint, cryptoKey, revoked, timestamp, trustLevel=255):
        """
        instantiates a OpenPGPKey object

        @param fingerprint: string key fingerprint of this key
        @type fingerprint: str
        @param cyptoKey: DSA or RSA key object
        @type cryptoKey: instance
        @param revoked: is this key revoked
        @type revoked: bool
        @param trustLevel: the trust level of this key, as stored locally
        @type trustLevel: int
        """

        self.fingerprint = fingerprint
        self.cryptoKey = cryptoKey
        self.revoked = revoked
        self.timestamp = timestamp
        self.trustLevel = trustLevel

    def getTrustLevel(self):
        return self.trustLevel

    def isRevoked(self):
        return self.revoked

    def getFingerprint(self):
        return self.fingerprint

    def getTimestamp(self):
        return self.timestamp

    def _gcf(self, a, b):
        while b:
            a, b = b, a % b
        return a

    def _bitLen(self, a):
        r=0
        while a:
            a, r = a/2, r+1
        return r

    def _getRelPrime(self, q):
        # We /dev/random instead of /dev/urandom. This was not a mistake;
        # we want the most random data available
        rand=open('/dev/random','r')
        b = self._bitLen(q)/8 + 1
        r = 0L
        while r < 2:
            for i in range(b):
                r = r*256 + ord(rand.read(1))
                r %= q
            while self._gcf(r, q-1) != 1:
                r = (r+1) % q
        rand.close()
        return r

    def signString(self, data):
        if isinstance(self.cryptoKey,(DSA.DSAobj_c, DSA.DSAobj)):
            K = self.cryptoKey.q + 1
            while K > self.cryptoKey.q:
                K = self._getRelPrime(self.cryptoKey.q)
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
	@type name: str
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
    def __init__(self, callback = callbacks.KeyCacheCallback()):
        OpenPGPKeyCache.__init__(self)
        self.callback = callback
        if 'HOME' not in os.environ:
            self.publicPaths = [ '/etc/conary/pubring.gpg' ]
            self.privatePath = None
        else:
            self.publicPaths = [ os.environ['HOME'] + '/.gnupg/pubring.gpg',
                                 '/etc/conary/pubring.gpg' ]
            self.trustDbPaths = [ os.environ['HOME'] + '/.gnupg/trustdb.gpg',
                                 '/etc/conary/trustdb.gpg' ]
            self.privatePath = os.environ['HOME'] + '/.gnupg/secring.gpg'

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
        trustDbPath = '/'.join(pubRing.split('/')[:-1]) + 'trustdb.gpg'
        self.trustDbPaths.append(trustDbPath)

    def getPublicKey(self, keyId):
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
                fingerprint = getFingerprint(keyId, publicPath)
                revoked, timestamp = getKeyEndOfLife(keyId, publicPath)
                cryptoKey = getPublicKey(keyId, publicPath)
                trustLevel = getKeyTrust(trustDbPath, fingerprint)
                self.publicDict[keyId] = OpenPGPKey(fingerprint, cryptoKey,
                                                    revoked, timestamp,
                                                    trustLevel)
                return self.publicDict[keyId]
            except KeyNotFound:
                pass
        # callback should only return True if it found the key.
        if self.callback.getPublicKey(keyId):
            return self.getPublicKey(keyId)
        raise KeyNotFound(keyId)

    def getPrivateKey(self, keyId, passphrase=None):
        if keyId in self.privateDict:
            return self.privateDict[keyId]

        # translate the keyId to a full fingerprint for consistency
        fingerprint = getFingerprint(keyId, self.privatePath)
        revoked, timestamp = getKeyEndOfLife(keyId, self.privatePath)

        # if we were supplied a password, use it.  The caller will need
        # to deal with handling BadPassPhrase exceptions
        if passphrase is not None:
            cryptoKey = getPrivateKey(keyId, passphrase, self.privatePath)
            self.privateDict[keyId] = OpenPGPKey(fingerprint, cryptoKey,
                                                 revoked, timestamp)
            return self.privateDict[keyId]

        # next, see if the key has no passphrase (WHY???)
        # if it's readable, there's no need to prompt the user
        try:
            cryptoKey = getPrivateKey(keyId, '', self.privatePath)
            self.privateDict[keyId] = OpenPGPKey(fingerprint, cryptoKey, revoked, timestamp)
            return self.privateDict[keyId]
        except BadPassPhrase:
            pass

        # FIXME: make this a callback
        print "\nsignature key is: %s"% keyId

        tries = 0
        while tries < 3:
            # FIXME: make this a callback
            passPhrase = getpass("Passphrase: ")
            try:
                cryptoKey = getPrivateKey(keyId, passPhrase, self.privatePath)
                self.privateDict[keyId] = OpenPGPKey(fingerprint, cryptoKey, revoked, timestamp)
                return self.privateDict[keyId]
            except BadPassPhrase:
                print "Bad passphrase. Please try again."
            tries += 1

        raise BadPassPhrase

#-----#
#OpenPGPKeyFinder: download missing keys from conary servers.
#-----#
class KeyCacheCallback(callbacks.KeyCacheCallback):
    def getPublicKey(self, keyId):
        for server in self.repositoryMap.values():
            findOpenPGPKey(server, keyId, self.pubRing)
            # decide if we found the key or not.
            keyRing = open(self.pubRing)
            keyRing.seek(0, SEEK_END)
            limit = keyRing.tell()
            keyRing.seek(0, SEEK_SET)
            seekKeyById(keyId, keyRing)
            found = keyRing.tell() != limit
            keyRing.close()
            if found:
                return True
        return False

def findOpenPGPKey(server, keyId, pubRing):
    pubRingPath = '/'.join(pubRing.split('/')[:-1])

    # don't depend on repoMap entries ending with /
    if server[-1] != '/':
        server += '/'

    secringExists = False
    if 'secring.gpg' in os.listdir(pubRingPath):
        secringExists = True

    pid = os.fork()
    if pid == 0:
        # we don't care about any of the possible output from this process.
        # gpg is pretty cavalier about dumping random garbage to stdout/err
        # regardless of the command line options admonishing it not to.
        fd = os.open(os.devnull, os.W_OK)
        os.dup2(fd, sys.stdout.fileno())
        os.dup2(fd, sys.stderr.fileno())
        os.close(fd)
        os.execlp('gpg', 'gpg', '-q', '--no-tty', '--homedir', pubRingPath,
                  '--no-greeting', '--no-secmem-warning', '--no-verbose',
                  '--no-mdc-warning', '--no-default-keyring', '--keyring',
                  pubRing.split('/')[-1], '--batch',
                  '--no-permission-warning', '--keyserver',
                  '%sgetOpenPGPKey?search=%s' % (server, keyId),
                  '--keyserver-options', 'timeout=3',
                  '--recv-key', keyId)
    os.wait()

    if not secringExists:
        try:
            os.remove(pubRingPath + '/secring.gpg')
        except:
            pass

_keyCache = OpenPGPKeyFileCache()

def getKeyCache():
    global _keyCache
    return _keyCache

def setKeyCache(keyCache):
    global _keyCache
    _keyCache = keyCache

