#!/usr/bin/python2.4
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

from Crypto.PublicKey import DSA
from Crypto.PublicKey import RSA
from Crypto.Util.number import getPrime
from openpgpfile import getPrivateKey
from openpgpfile import getPublicKey
from openpgpfile import getFingerprint

#-----#
#OpenPGPKey structure:
#     cyptoKey:         class  (either DSA or RSA object)
#     fingerprint:      string key fingerprint of this key
#     revoked:          bool   indicates if key is revoked
#     trustLevel:       int    Higher is more trusted
#                              0 is untrusted
#-----#

class OpenPGPKey:
    def __init__(self, keyId, private, passPhrase = '', keyFile = ''):
        ###change when implement revocation functionality
        self.revoked=0
        ###change when implement trust levels
        self.trustLevel = 255
        #translate the keyId to the fingerprint for consistency
        self.fingerprint = getFingerprint (keyId)
        if private:
            self.cryptoKey = getPrivateKey(keyId, passPhrase, keyFile)
        else:
            self.cryptoKey = getPublicKey(keyId, keyFile)

    def getTrustLevel(self):
        return self.trustLevel

    def isRevoked(self):
        return self.revoked

    def getFingerprint(self):
        return self.fingerprint

    def _gcf(self, a, b):
        while b:
            a, b = b, a%b
        return a

    def _bitLen(self, a):
        r=0
        while a:
            a, r = a/2, r+1
        return r

    def _getRelPrime(self, q):
        rand=open('/dev/random','r')
        b = self._bitLen(q)/8 + 1
        r= 0L
        for i in range(b):
            r = r*256 + ord(rand.read(1))
        rand.close()
        r %= q
        while self._gcf(r, q-1) != 1:
            r = (r+1) % q
        return r

    def signString(self, data):
        if 'DSAobj_c' in self.cryptoKey.__class__.__name__:
            K = self.cryptoKey.q + 1
            while K > self.cryptoKey.q:
                K = self._getRelPrime(self.cryptoKey.q)
        else:
            K=0
        return (self.fingerprint, self.cryptoKey.sign( data, K ))

    #the result of this verification process:
    # -1 indicates FAILURE. the string has been modified since it was signed,
    # or you gave this key a signature it didn't make.
    # any other value will be the trust level of the key itself
    # (which is always 0 or greater)
    def verifyString(self, data, sig):
        if self.fingerprint == sig[0] and self.cryptoKey.verify( data, sig[1] ):
            return self.trustLevel
        else:
            return -1

class OpenPGPPublicKey(OpenPGPKey):
    def __init__ (self, keyId, keyFile = ''):
        OpenPGPKey.__init__(self, keyId, 0, '', keyFile)

class OpenPGPPrivateKey(OpenPGPKey):
    def __init__ (self, keyId, passPhrase = '', keyFile = ''):
        OpenPGPKey.__init__(self, keyId, 1, passPhrase, keyFile)


class OpenPGPKeyCache:
    def __init__(self, keyFile = ''):
        self.publicDict = {}
        self.privateDict = {}
        self.keyFile = keyFile

    def getPublicKey(self, keyId):
        if keyId not in self.publicDict:
            self.publicDict[keyId] = OpenPGPPublicKey(keyId, self.keyFile)
        return self.publicDict[keyId]

    def getPrivateKey(self, keyId, passPhrase = ''):
        if keyId not in self.privateDict:
            self.privateDict[keyId] = OpenPGPPrivateKey(keyId, passPhrase, self.keyFile)
        return self.privateDict[keyId]

global keyCache
keyCache = OpenPGPKeyCache()
