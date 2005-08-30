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

import os
from getpass import getpass
from time import time

from Crypto.PublicKey import DSA
from Crypto.PublicKey import RSA
from Crypto.Util.number import getPrime
from openpgpfile import getPrivateKey
from openpgpfile import getPublicKey
from openpgpfile import getFingerprint
from openpgpfile import getBlockType
from openpgpfile import seekNextKey
from openpgpfile import getDBKey
from openpgpfile import IncompatibleKey
from openpgpfile import BadPassPhrase
from openpgpfile import KeyNotFound

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
        self.fingerprint = getFingerprint (keyId, keyFile)
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

    # We /dev/random instead of /dev/urandom. This was not a mistake;
    # we want the most random data available
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
        timeStamp = int(time())
        return (self.fingerprint, timeStamp, self.cryptoKey.sign( data+str(timeStamp), K))

    # the result of this verification process:
    # -1 indicates FAILURE. the string has been modified since it was signed,
    # or you gave this key a signature it didn't make.
    # any other value will be the trust level of the key itself
    # (which is always 0 or greater)
    # this function was not designed to throw an exception at this level
    # because in some cases the calling function wants to aggregate a list
    # of failed/passed signatures all at once.
    def verifyString(self, data, sig):
        if self.fingerprint == sig[0] and self.cryptoKey.verify( data+str(sig[1]), sig[2] ):
            return self.trustLevel
        else:
            return -1

class OpenPGPDBKey(OpenPGPKey):
    def __init__(self, keyId, keyTable):
        ###change when implement revocation functionality
        self.revoked=0
        ###change when implement trust levels
        self.trustLevel = 255
        #set fingerprint for consistency
        self.fingerprint = keyTable.getFingerprint(keyId)
        self.cryptoKey = getDBKey(keyId, keyTable)

class OpenPGPPublicKey(OpenPGPKey):
    def __init__ (self, keyId, keyFile = ''):
        OpenPGPKey.__init__(self, keyId, 0, '', keyFile)

class OpenPGPPrivateKey(OpenPGPKey):
    def __init__ (self, keyId, passPhrase = '', keyFile = ''):
        OpenPGPKey.__init__(self, keyId, 1, passPhrase, keyFile)

_KC_SRC_PUBLIC  = 1
_KC_SRC_DB      = 2

class OpenPGPKeyCache:
    def __init__(self):
        self.publicDict = {}
        self.privateDict = {}

        if 'HOME' not in os.environ:
            self.publicPaths = [ '/etc/conary/pubring.gpg' ]
            self.privatePath = None
        else:
            self.publicPaths = [ os.environ['HOME']+'/.gnupg/pubring.gpg', '/etc/conary/pubring.gpg' ]
            self.privatePath = os.environ['HOME']+'/.gnupg/secring.gpg'
        self.source = _KC_SRC_PUBLIC
        self.keyTable = None

    def getPublicKey(self, keyId):
        if keyId not in self.publicDict:
            if self.source == _KC_SRC_PUBLIC:
                failures = 0
                for publicPath in self.publicPaths:
                    try:
                        self.publicDict[keyId] = OpenPGPPublicKey(keyId, publicPath)
                        break
                    except KeyNotFound:
                        failures += 1
                if failures == len(self.publicPaths):
                    raise KeyNotFound(keyId)
            elif self.source == _KC_SRC_DB:
                if self.keyTable is not None:
                    self.publicDict[keyId] = OpenPGPDBKey(keyId, self.keyTable)
                else:
                    raise KeyNotFound("Can't open database")
            else:
                assert(0)
        return self.publicDict[keyId]

    def getPrivateKey(self, keyId):
        if keyId not in self.privateDict:
            #first see if the key has no passphrase (WHY???)
            #if it's readable, there's no need to prompt the user
            try:
                self.privateDict[keyId] = OpenPGPPrivateKey(keyId, '', self.privatePath)
                badPass = 0
            except BadPassPhrase:
                badPass = 1
            print "\nsignature key is: %s"% keyId
            while badPass:
                passPhrase=getpass("Passphrase: ")
                try:
                    self.privateDict[keyId] = OpenPGPPrivateKey(keyId, passPhrase, self.privatePath)
                    badPass = 0
                except BadPassPhrase:
                    print "Bad passphrase. please try again."
        return self.privateDict[keyId]

    def setKeyTable(self, keyTable):
        self.keyTable = keyTable

    def setPublicPath(self, path):
        self.publicPaths = [ path ]

    def addPublicPath(self, path):
        self.publicPaths.append(path)

    def setPrivatePath(self, path):
        self.privatePath = path

    #this is used to distinguish source of public keys only
    #private keys will always come from a file
    def setSource(self, type):
        self.source = type

global keyCache
keyCache = OpenPGPKeyCache()
