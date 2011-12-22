#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


from testrunner import testhelp
import sys

import base64
import fcntl
import os
import time

SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2


from conary_test import rephelp
from conary_test import resources
from conary import conaryclient
from conary.lib import openpgpfile, openpgpkey, util
from conary.lib.openpgpfile import PGP_Message
from conary.lib.openpgpfile import BadSelfSignature
from conary.lib.openpgpfile import KeyNotFound
from conary.lib.openpgpfile import getFingerprint
from conary.lib.openpgpfile import seekKeyById
from conary.lib.openpgpfile import TRUST_UNTRUSTED, TRUST_MARGINAL, \
     TRUST_FULL, TRUST_ULTIMATE
from random import randrange


class BaseTestHelper(rephelp.RepositoryHelper):
    def getKeyring(self, keyring):
        return os.path.join(resources.get_archive(), 'pgp-keys', keyring)

    def getPrivateFile(self):
        return resources.get_archive()+'/secring.gpg'

    def getPublicFile(self):
        return resources.get_archive()+'/pubring.gpg'

    def getTrustDbFile(self):
        return resources.get_archive()+'/trustdb.gpg'

    def getRandomString(self, numOctets):
        r=''
        for i in range(0,numOctets):
            r+=chr(randrange(0,256))
        return r

class OpenPGPTest(BaseTestHelper):
    # deliberately seek for keys known to be in the test rings.
    # ensures keyId translation is correct--or they won't be found
    # as a side note, the same code used to make a fingerprint
    # is used to verify self signatures, so it tests the accuracy of both
    def testFindKeys(self):
        # test specific keys in both the private and public rings
        # we don't care what the fingerprints are, we care that the keys
        # can be found in the first place
        for keyFile in (self.getPublicFile(), self.getPrivateFile()):
            # test a main key
            getFingerprint('C7C32FC1F94E405E', keyFile)
            # test a subkey
            getFingerprint('017111F7', keyFile)

    def testSeekMissingKey(self):
        keyRing = open(self.getPublicFile())
        keyRing.seek(0, SEEK_END)
        limit = keyRing.tell()
        keyRing.seek(0, SEEK_SET)
        try:
            ret = openpgpfile.seekKeyById('NOTAVALIDFINGERPRINT', keyRing)
            if ret:
                self.fail('seekKeyById wandered outside keyring')
        finally:
            keyRing.close()

    def testKeyEndOfLife(self):
        #grab a couple of keys and check that their timestamps work correctly
        keyRing = open(self.getPublicFile())
        keyCache = openpgpkey.getKeyCache()
        keyCache.setPublicPath(self.getPublicFile())
        # test a key expiration
        pubKey = keyCache.getPublicKey('DA44E4BD')
        assert pubKey.getTimestamp() == 1142264067
        #test a sub key
        assert keyCache.getPublicKey('017111F7').getTimestamp() == 0

    #for every key in the test rings, verify the public Id matches the
    #private Id. This test is valuable because we have to translate the
    #private key to a public key before we can hash it for the fingerprint.
    def testCompareKeys(self):
        pubmsg = openpgpfile.PGP_Message(self.getPublicFile())
        privmsg = openpgpfile.PGP_Message(self.getPublicFile())
        for pubkey, privkey in zip(pubmsg.iterMainKeys(), privmsg.iterMainKeys()):
            self.failUnlessEqual(pubkey.getKeyId(), privkey.getKeyId())
            self.failUnlessEqual(pubkey.getKeyFingerprint(), privkey.getKeyFingerprint())
            self.failUnlessEqual(len(pubkey.getKeyId()), 16)
            self.failUnlessEqual(len(pubkey.getKeyFingerprint()), 40)
            self.failUnless(pubkey.getKeyFingerprint().endswith(pubkey.getKeyId()))
            for pubsubkey, privsubkey in zip(
                                pubkey.iterSubKeys(), privkey.iterSubKeys()):
                self.failUnlessEqual(pubsubkey.getKeyId(), privsubkey.getKeyId())
                self.failUnlessEqual(pubsubkey.getKeyFingerprint(), privsubkey.getKeyFingerprint())
                self.failUnlessEqual(len(pubsubkey.getKeyId()), 16)
                self.failUnlessEqual(len(pubsubkey.getKeyFingerprint()), 40)
                self.failUnless(pubsubkey.getKeyFingerprint().endswith(pubsubkey.getKeyId()))

    def testTrustLevels(self):
        raise testhelp.SkipTestException("Need to fix the trust")
        trustDbFile = self.getTrustDbFile()
        trustKeys = {
            '95B457D16843B21EA3FC73BBC7C32FC1F94E405E' : TRUST_ULTIMATE,
            'F7440D78FE813C882212C2BF8AC2828190B1E477' : TRUST_FULL,
            'AC880ED21C690941484D8FF5DF7E85BCF0297F56' : TRUST_FULL,
            '69E3912FA8DDD94EDF172D4BF7C542A9DA44E4BD' : TRUST_MARGINAL,
            'A5894452EAA09DA7180ECCCE9A111B2F688CF0B8' : TRUST_UNTRUSTED,
            'E0DE948B813E14A1F14D40DEABA356F97D527792' : TRUST_UNTRUSTED,
            '03B9CDDB42E9764275181784910E85FD7FA9DDBC' : TRUST_UNTRUSTED,
            }
        for fingerprint, trust in trustKeys.iteritems():
            pass
            #if trust != getKeyTrust(trustDbFile, fingerprint):
                #self.fail("Trust of %d returned when expecting %d for key: %s" %(getKeyTrust(trustDbFile, fingerprint), trust, fingerprint))

    def testSetPaths(self):
        keyCache = openpgpkey.getKeyCache()
        publicPaths = keyCache.publicPaths
        try:
            keyCache.setPublicPath('test')
            self.failUnlessEqual(keyCache.publicPaths, ['test'],
                   "Public path failed to translate from string")
            keyCache.addPublicPath('foo')
            self.failUnlessEqual(keyCache.publicPaths, ['test', 'foo'],
                   "Public path failed to translate when adding string")
            keyCache.setPublicPath(['test', 'foo'])
            self.failUnlessEqual(keyCache.publicPaths, ['test', 'foo'],
                   "Public path failed to translate from list")
            keyCache.addPublicPath(['bar', 'baz'])
            self.failUnlessEqual(keyCache.publicPaths, ['test', 'foo',
                                            'bar', 'baz'],
                   "Public path failed to translate when adding list")        
        finally:
            keyCache.publicPaths = publicPaths

    def testFingerprintToIntKeyId(self):
        # the zero octet is 8th from the end. this function used to fail
        # that scenario by returning a 7 octet string.
        fingerprint = "112233445566778899AABBDD00EEFF1122334455"
        intKeyId = openpgpfile.fingerprintToInternalKeyId(fingerprint)
        if len(intKeyId) != 8:
            self.fail("fingerprintToInternalKeyId didn't return 8 octets")
        if intKeyId != '\x00\xee\xff\x11"3DU':
            self.fail('fingerprint lost in translation')
        self.failUnlessEqual(openpgpfile.fingerprintToInternalKeyId(''), '')

    def testS2K(self):
        data = [
                  ('', 'DA39A3EE5E6B4B0D3255BFEF95601890',
                          '89D14893B12A0116A88A1AEB5530FDE8',
                          '006A5A5D2422D19051A4ABC141C1F510'),
                  ('a', '86F7E437FAA5A7FCE15D1DDCB9EAEAEA',
                          '529B38BD89A90F873D1D51E30B57C96B',
                          '09A377456C27F8D8A6D527D0CF4C419D'),
                  ('aa', 'E0C9035898DD52FC65C41454CEC9C4D2',
                          '89CDA9A5D0393DFF564BB2A1D769F36E',
                          'E18D17331021E559DAE2EFD280931B5C'),
                  ('abcde', '03DE6C570BFE24BFC328CCD7CA46B76E',
                          'C62BE1A3D54633B3D2A00806DC51CB74',
                          'B2FD7438E58039F8ADD455527421BD8B'),
               ]
        keySize = 128
        func1 = openpgpfile.simpleS2K
        func2 = openpgpfile.saltedS2K
        func3 = openpgpfile.iteratedS2K
        salt = 'some-salt'
        hashAlg = openpgpfile.digestlib.sha1
        s2a = openpgpfile.stringToAscii
        count = 2
        for s, expected1, expected2, expected3 in data:
            ret1 = func1(s, hashAlg, keySize)
            self.failUnlessEqual(s2a(ret1), expected1)

            ret2 = func2(s, hashAlg, keySize, salt)
            self.failUnlessEqual(s2a(ret2), expected2)

            ret3 = func3(s, hashAlg, keySize, salt, count)
            self.failUnlessEqual(s2a(ret3), expected3)

    def test_hashSet(self):
        sioc = util.ExtendedStringIO
        items = [ sioc(''), sioc('a'), sioc('aa'), sioc('abcde') ]
        expItems = [ '03DE6C570BFE24BFC328CCD7CA46B76EADAF4334',
                     'DA39A3EE5E6B4B0D3255BFEF95601890AFD80709',
                     'E0C9035898DD52FC65C41454CEC9C4D2611BFB37',
                     '86F7E437FAA5A7FCE15D1DDCB9EAEAEA377667B8',
                    ]
        func = openpgpfile.PGP_Packet._hashSet
        s2a = openpgpfile.stringToAscii
        for ret, exp in zip(func(items), expItems):
            self.failUnlessEqual(s2a(ret), exp)

    def testGetSigId(self):
        sigTest = util.ExtendedStringIO("GARBAGE")
        sig = PGP_Message.newPacket(openpgpfile.PKT_SIG, sigTest)
        self.failUnlessRaises(openpgpfile.InvalidBodyError, sig.getSigId)

        stream = util.ExtendedStringIO('')
        mkey = PGP_Message.newPacket(openpgpfile.PKT_PUBLIC_KEY,
                util.SeekableNestedFile(stream, len(stream.getvalue())))

        # V3 sigs...
        sigTest = util.ExtendedStringIO()
        # Signature version
        sigTest.write(chr(3))
        # Length of hashed material
        sigTest.write(chr(5))
        # Sig type, creation
        sigTest.write(chr(openpgpfile.SIG_TYPE_SUBKEY_REVOC) + '1234' )
        # Signer key ID
        sigTest.write('8bytegbg')
        # PK alg, hash alg, sig0, sig1
        sigTest.write(chr(0) * 4)
        sigTest.seek(0)

        sig = PGP_Message.newPacket(openpgpfile.PKT_SIG, sigTest)
        sig.setParentPacket(mkey)
        # We know how to read even V3 sigs
        self.failUnlessEqual(sig.getSigId(), '8bytegbg')
        self.failUnlessEqual(sig.getSignerKeyId(),
                             openpgpfile.stringToAscii('8bytegbg'))
        # (but we won't verify them)
        self.failUnlessRaises(openpgpfile.UnsupportedHashAlgorithm,
            sig.getSignatureHash)
        try:
            sig.getSignatureHash()
        except openpgpfile.UnsupportedHashAlgorithm, e:
            self.failUnlessEqual(str(e), 'Unsupported hash algorithm code 0')

        # Use md5
        sig.hashAlg = 1
        sig.setShortSigHash('\x08,')
        sig._sigDigest = None
        self.failUnlessEqual(sig.getSignatureHash(),
            '\x08,!\xb10\x07\xf5C\xcb\xe1\x0e\x0cOZx\xf7')
        # Use sha1
        sig.hashAlg = 2
        sig.setShortSigHash('\x95z')
        sig._sigDigest = None
        self.failUnlessEqual(sig.getSignatureHash(),
            '\x95z\x07-3~\xdb\x1c\x8e?\xa7\x86-.\xdd\xf1\xc3\xabe;')

        sigTest = util.ExtendedStringIO()
        # Signature version
        sigTest.write(chr(3))
        # Length of hashed material (bogus)
        sigTest.write(chr(4))
        # Signature type
        sigTest.write(chr(1))

        sigTest.seek(0)
        sig = PGP_Message.newPacket(openpgpfile.PKT_SIG, sigTest)
        sig.setParentPacket(mkey)
        try:
            sig.parse()
        except openpgpfile.PGPError, e:
            self.failUnlessEqual(str(e), "Expected 5 octets of length of "
                                 "hashed material, got 4")
        else:
            self.fail("Should have failed")

        # bogus V4 sig packet forces test of all remaining code paths
        # KeyID longer than 8 bytes
        spkt = chr(11) + chr(openpgpfile.SIG_SUBPKT_ISSUER_KEYID) + \
               chr(0xBE) * 10
        sigTest = util.ExtendedStringIO(chr(4) + chr(0) * 3
                                       # hashed data length, hashed data
                                       + chr(0) * 2
                                       # unhashed data length, unhashed data
                                       + chr(0) + chr(len(spkt))
                                       + spkt
                                       # 2-octet hash sig
                                       + '  '
                                       )

        sig = PGP_Message.newPacket(openpgpfile.PKT_SIG, sigTest)
        try:
            sig.getSigId()
        except openpgpfile.InvalidPacketError, e:
            self.failUnlessEqual(str(e), "Expected 8 bytes, got 10 instead")
        else:
            self.failIf(True, "Should have raised InvalidPacketError")

    def testVerifyBindingSig(self):
        fingerprint = "03B9CDDB42E9764275181784910E85FD7FA9DDBC"
        subkeyFingerprint = "1C9CF8632DDA8DD5F1FC7CC76AC04A81017111F7"

        # read the key data for the main key and the subkey
        keyRing = open(self.getPublicFile())
        pkt = seekKeyById(fingerprint, keyRing)
        subpkt = pkt.iterSubKeys().next()

        # prove that the signature is good.
        pkt.verifySelfSignatures()
        subpkt.verifySelfSignatures()

        sio = util.ExtendedStringIO()
        pkt.writeAll(sio)

        # surgically replace a single octet in the hashed key data and repeat
        # only bytes 515-519, 521 and 522 can be changed for proper effect.
        sio.seek(516)
        sio.write('Q')
        sio.seek(0)

        pkt = seekKeyById(fingerprint, sio)
        subpkt = pkt.iterSubKeys().next()

        pkt.verifySelfSignatures()
        # Subpacket signature should fail
        self.failUnlessRaises(BadSelfSignature, subpkt.verifySelfSignatures)

    def testVerifyDirectKeySig(self):
        fingerprint = "03B9CDDB42E9764275181784910E85FD7FA9DDBC"
        subkeyFingerprint = "1C9CF8632DDA8DD5F1FC7CC76AC04A81017111F7"

        # read the key data for the main key and the subkey
        pubkey = openpgpfile.seekKeyById(fingerprint, self.getPublicFile())
        subkey = pubkey.iterSubKeys().next()
        self.failUnlessEqual(subkey.getKeyFingerprint(), subkeyFingerprint)

        # Add binding sig as a revocation in the main key
        bindingSig = subkey.bindingSig.clone()
        bindingSig.setParentPacket(pubkey)
        pubkey.revsigs.append(bindingSig)

        self.failUnlessRaises(BadSelfSignature, pubkey.verifySelfSignatures)

    def testUserIdBindingSig(self):
        fingerprint = "03B9CDDB42E9764275181784910E85FD7FA9DDBC"
        subkeyFingerprint = "1C9CF8632DDA8DD5F1FC7CC76AC04A81017111F7"

        # read the key data for the main key and the subkey
        pubkey = openpgpfile.seekKeyById(fingerprint, self.getPublicFile())
        subkey = pubkey.iterSubKeys().next()
        self.failUnlessEqual(subkey.getKeyFingerprint(), subkeyFingerprint)

        pubkey.verifySelfSignatures()
        subkey.verifySelfSignatures()

        sio = util.ExtendedStringIO()
        pubkey.writeAll(sio)

        # user Id packet starts at offset 141, so just whack a random octet
        sio.seek(145)
        sio.write('Q')
        sio.seek(0)

        pkt = seekKeyById(fingerprint, sio)
        subKey = pkt.iterSubKeys().next()

        # The userId is part of the main key, but as part of verifying a
        # subkey we verify the main key too
        self.failUnlessRaises(BadSelfSignature, subKey.verifySelfSignatures)

    def testDiskKeyCache(self):
        keyDir = os.path.join(self.workDir, "public-key-dir")
        util.mkdirChain(keyDir)

        # Well-known key
        keyFile = resources.get_archive('key.asc')
        keyId = "95B457D16843B21EA3FC73BBC7C32FC1F94E405E"
        pubKeyRing = os.path.join(self.workDir, "test-pubring.gpg")
        callback = openpgpkey.DiskKeyCacheCallback(keyDir)
        newKeyCache = openpgpkey.OpenPGPKeyFileCache(callback=callback)
        newKeyCache.setPublicPath(pubKeyRing)

        self.failUnlessRaises(KeyNotFound, newKeyCache.getPublicKey, keyId)

        # this worked until we started keeping negative cache entries
        # Copy the key file
        #kpath = os.path.join(keyDir, "%s.asc" % keyId.lower())
        #open(kpath, "w+").write(open(keyFile).read())
        #try:
            #os.chmod(kpath, 0)
            #self.failUnlessRaises(KeyNotFound, newKeyCache.getPublicKey, keyId)
        #finally:
            #os.chmod(kpath, 0644)

        #newKeyCache.getPublicKey(keyId, 'dummy')

    def testKeyCacheKeyRetrieval(self):
        kc = openpgpkey.OpenPGPKeyFileCache()
        kc.setPublicPath(self.getKeyring('pubringrev.gpg'))
        kc.setPrivatePath(self.getKeyring('secringrev.gpg'))

        pk = kc.getPublicKey('91E3E6C5')
        self.failUnlessRaises(openpgpfile.BadPassPhrase,
            kc.getPrivateKey, '91E3E6C5', 'bad-passphrase')

        tries = []

        def mockGetpass(msg):
            tries.append('')
            return 'bad-passphrase'

        import getpass
        self.mock(getpass, 'getpass', mockGetpass)
        try:
            import keyutils
            self.mock(keyutils, 'request_key', lambda slf, x: None)
        except ImportError:
            pass
        self.failUnlessRaises(openpgpfile.BadPassPhrase,
            self.discardOutput, kc.getPrivateKey, '91E3E6C5')
        self.failUnlessEqual(len(tries), 5)
        self.unmock()

        def mockGetpass2(msg):
            return 'key32'
        self.mock(getpass, 'getpass', mockGetpass2)
        self.discardOutput(kc.getPrivateKey, '91E3E6C5')

    def testKeyringKeyCache(self):
        keyring = os.path.join(self.workDir, "temp-keyring")
        keyringf = open(keyring, "w+")

        # Well-known key
        testDir = os.path.realpath(os.path.dirname(
            sys.modules['rephelp'].__file__))

        keyFile = os.path.join(testDir, "archive", "key.asc")
        for armkey in [pubkey1, pubkey2, pubkey3]:
            # Un-armor
            key = readKey(armkey.split('\n'))
            keyringf.write(key)
        keyringf.close()

        keyId = "95B457D16843B21EA3FC73BBC7C32FC1F94E405E"
        pubKeyRing = os.path.join(self.workDir, "test-pubring.gpg")
        callback = openpgpkey.KeyringCacheCallback(keyring)
        newKeyCache = openpgpkey.OpenPGPKeyFileCache(callback=callback)
        newKeyCache.setPublicPath(pubKeyRing)

        self.logFilter.add()
        self.failUnlessRaises(KeyNotFound, newKeyCache.getPublicKey, keyId,
            'dummy')
        self.logFilter.remove()

        keyringf = open(keyring, "a")
        # Add the key
        for armkey in [open(keyFile).read()]:
            # Un-armor
            key = readKey(armkey.split('\n'))
            keyringf.write(key)
        keyringf.close()

        # this stoppd working when we added negative openpgp cache entries
        #k = newKeyCache.getPublicKey(keyId, 'dummy')
        #self.failUnless(k)

        #newKeyCache.privateDict[keyId] = "Some value"

        #newKeyCache.remove(keyId)
        #self.failIf(keyId in newKeyCache.publicDict)
        #self.failIf(keyId in newKeyCache.privateDict)



def readKey(rowList):
    """Reads an armored key from a list of rows and return the non-armored
    version"""
    rlIter = iter(rowList)
    for row in rlIter:
        if row.strip() == '':
            break
    keyLines = []
    for row in rlIter:
        if row[0] == '=':
            break
        keyLines.append(row.strip())
    return base64.decodestring(''.join(keyLines))


pubkey1 = """\
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.1 (GNU/Linux)

mI0ERfN+dwEEAOT+tXGfM+sNV1ZUrHVpOxcw57cg6fdS2bSThA7AqBNEnbMkatgX
I4S71s1NTGpKpSIoT4Twk9tJrI3rsy/2QaSZ9J34+1WhQ8oD+tn8KDxg5HP02eAe
9u8sYuOEURQ4w8VVOiwMn1B789Lf76qGiU8HEvhQRAibbpigX5P2/S5tABEBAAG0
ME1paGFpIEliYW5lc2N1IChUZXN0IGtleSkgPG1pc2ErdGVzdDRAcnBhdGguY29t
Poi2BBMBAgAgBQJF8353AhsvBgsJCAcDAgQVAggDBBYCAwECHgECF4AACgkQ76OS
Ta4H43jRwwQAsdTqyCYpB8Btov9zrEF2qdSx5m5AwoBUyEQYuNN/XNGza1/y0tJr
BLbSYXe82Eal6+emiMkhdCGDA/BSPYd6UA284uJm+fhuJF+Sw5BvPBolMTx8IZoi
7U+kLJEXtyyzDaMjSM0WJctyvJGjQWyh0ukGn/jAv4ASX1yoKy7j4bm5AQ0ERfN+
eRAEAJwVsQ+3bvNj4e2GkakcnsfYnKMlAZTa7uZUG+g3NDNcNOsK1NMSE/r/oFhP
/yB3LbaWAT0xaCp7nd1DSCh0AmyHE7H9LNVf2EleRXmEtLt6E3o9TtfZNUWkJ6M9
hX6NXF83AFE9JRuLqe72LmNivwIrVctNV77uOiSLiT6PqqtXAAMFA/9BZ0YHcvnv
xfOX19rrDynvT01+vxlAJN07Rd6aldSGnPUTD6MUNknWavMdyzlfwFebwYxYlIYp
/2dMy2yXzOfNPzjktEmUTcAjiqJUXfQVzvVaK4ugzfdS7M552CbsTFPwdmu67Q4c
9kfrFLAGTupH6ryBK3aH63M/pFZQRwihhIifBBgBAgAJBQJF8355AhsMAAoJEO+j
kk2uB+N4zmUEAIHBgkhg2S5YI247umj/pwnfC5B1MW9wA4vy6puKMnY3rQMX7TUI
71JObz6iZDrXZKyYMNEyXBTtDgflzcTLBVZrLO6E897PBi9bBGajN6ZA6YlBeM5f
DXbmN/21Bb1iPCtWwfLfiTSgp9yuNFg2WdiA6LIFYkT4F/mMFyawKuGU
=R/ZN
-----END PGP PUBLIC KEY BLOCK-----
"""

pubkey2 = """\
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.1 (GNU/Linux)

mI0ERfN+QwEEANKRvx9zNOHFADHWtVCMA23mApOGEO2O3sLwPizCLmHdvapT1Mk3
u0s9cwJ6Gv4bYpI9qFhWkRr0KW6BWM4UeDL3CMpDm2Jb18Q1My4aufO0vinIUZ8W
WCMUttOCv24FDbx7Ql44s84qtu0Sobr0q6F7DadrHNXdI3V/um0Mien5ABEBAAG0
ME1paGFpIEliYW5lc2N1IChUZXN0IGtleSkgPG1pc2ErdGVzdDNAcnBhdGguY29t
Poi2BBMBAgAgBQJF835DAhsvBgsJCAcDAgQVAggDBBYCAwECHgECF4AACgkQNjFr
jwjf7oaUlQP+JzjYqrB8PnUKyvHhUvYOqkGkGLRWOd/Ps6fUXuAq+jbDmgk8IdXe
pzpXMRUnff0jM9I3YIcJlzQgToQtpqm/+AyZOou7X61tD7SC4R9o6kQ6Pzwe1axP
tngtuEo0LJpkES/QeTs6XKCeH+wl2RKZiAA8jhANYlwCzQaujttzsxW5AQ0ERfN+
RBAEAMByh8ROZ20ez7rz6tk+bkrVrHBX2r982a7sYgPWpeWV5SRXI57JgM77Mzyt
+t7dRBi04O+GMCqNIhba2FN3VnkKyNODsNT+wO+UcnPcr3e0nuKL4ye/R0J9fhTS
sd0qdeYqXCsjCXiCzH0xNh4z8NLA4miimZ62K6EDulwJoHCTAAMGA/9D3iUelqHR
Gad2Ac5joa9UYkX6euRvf82TG1PDQPZJ+9cyctkVGXo0QRKB5yPd7XsqzHwd/ylA
FHA123xr4NrqTgIt2JVOl7/+uBIyB/HCxRCW8rwpILyGzs7I0NkQ6lN0sP4boC4f
8xzrwEPGaLxfnvroH4Lks4U0XzBJ5nSpCoifBBgBAgAJBQJF835EAhsMAAoJEDYx
a48I3+6GTKQD/jBWKZ9VMhVmWP35CeuyaOKas1XQUaWGcifWwDMshbawX3A5hyMq
E+AQ+W5zFBFMnyB9LTshADEMIl05844b0C9viq586kArChSOBBSKRnLb5OegRAaH
t35vONSTlAlGMnpD75nNtSKF2TFv3IIko+wXfjMMZLoSAAgJuJFLAs1T
=hlvV
-----END PGP PUBLIC KEY BLOCK-----
"""

pubkey3 = """\
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.1 (GNU/Linux)

mI0ERfN+IwEEAK1OLzJFuLLTnkD3yfjT9Cr+D7Uc7xKDcQSCQW7N/Ssqrobo4NP3
dBH5MUg4T6PaWmPdXxUOmxczyXwQnGBmzEnJ9qRrhI/teyg6Qll6qy3Ostp+36/R
nnHbE61X3nQD38vBKpRnMMaNWCsGg2uBVk456WDYwmRyUl/S7BymBj4hABEBAAG0
ME1paGFpIEliYW5lc2N1IChUZXN0IGtleSkgPG1pc2ErdGVzdDJAcnBhdGguY29t
Poi2BBMBAgAgBQJF834jAhsvBgsJCAcDAgQVAggDBBYCAwECHgECF4AACgkQGn8l
Thp+MOxbIQP+J3niPCC3qFF81tPx7o/9CK9dYE2HhPPFmTk854VRzoGjsyBx1u7b
7/Kzkkcoo1o753cJ99eZz4QH09fmykECR96GITjaKQOV15hPAgSLymy1Ajn36fG1
XXud4Mxqzf7egHTJHSgr3RgOxhQKQPW4p7kTR0kU+8BnkuK7mCmBSvS5AQ0ERfN+
JRAEAJYvU0RhYSf0zL/k2rvw1Pb/oCeueWFJbkpZIFKveC4e/KBGNWmuuiN+WDMy
Tr0Nb4Ob43cOT5pXc0AN/53+hZhXQ235NHdmrFdAWK5bNzIcAzjM/sRgyo/nWAMV
/J3jp06eo7sRph1Z9PdCNieFJHYAGSOPSxUo9ziK0UaSW9BrAAMFBACRZih+C+rI
k6P4L78rIWH7jMoT5PHsSlFlud1sMeI3qg/drak9rDD27NjSnfnp96UVy472yVMX
/lpzwLqPfwvHQI/OVbiwTE3GgHyXiTA051/kQxLK0u/7NGzSZlgkKug3LsjhTpLc
cHzbw2a5Q4uAE0RDJlaJ9A39XxyA14UrBoifBBgBAgAJBQJF834lAhsMAAoJEBp/
JU4afjDs8gYD/2c3ICiOuhlHTzPKmpdtmrQvzVHFwL7A9h5WBHSujcyFIxXDpuE3
GRPTI7sFILo9vX8XUUaO3EZ5jan5nDsZuGSNvkDxlAQjp+7wfIyPVVaA4aSgx1S+
fbH4kC2IUkRcZsGlxXTU5neClrO5MB82nno3yjeaOaIFFZNBA4/VOxPm
=I/wl
-----END PGP PUBLIC KEY BLOCK-----
"""

class OpenPGPMessageTest(BaseTestHelper):
    def testNewPacket(self):
        sio = util.ExtendedStringIO("foo")

        sioDest = util.ExtendedStringIO()
        pkt = PGP_Message.newPacket(14, sio)

        pkt.write(sioDest)
        data = sioDest.getvalue()
        self.failUnlessEqual(data, '\xb8\x03foo')

        # Forcing 2-byte length body
        sioDest = util.ExtendedStringIO()
        pkt = PGP_Message.newPacket(11, sio, minHeaderLen = 3)
        pkt.write(sioDest)
        data = sioDest.getvalue()
        self.failUnlessEqual(data, '\xad\x00\x03foo')

        # Forcing 4-byte length
        sioDest = util.ExtendedStringIO()
        pkt = PGP_Message.newPacket(11, sio, minHeaderLen = 4)
        pkt.write(sioDest)
        data = sioDest.getvalue()
        self.failUnlessEqual(data, '\xae\x00\x00\x00\x03foo')

        # Create message, verify everything is readable
        sioDest.seek(0)
        pkt = PGP_Message.newPacketFromStream(sioDest)
        self.failUnlessEqual(pkt.tag, 11)
        self.failUnlessEqual(pkt.headerLength, 5)
        self.failUnlessEqual(pkt.bodyLength, 3)

        first = (openpgpfile.PKT_PUBLIC_KEY << 2) | 0x03
        sioDest = util.ExtendedStringIO(chr(first))
        self.failUnlessRaises(openpgpfile.InvalidPacketError,
                               PGP_Message.newPacketFromStream, sioDest)

        # Indeterminate length not supported
        first = 0x80 | (openpgpfile.PKT_PUBLIC_KEY << 2) | 0x03
        sioDest = util.ExtendedStringIO(chr(first))
        self.failUnlessRaises(NotImplementedError,
                              PGP_Message.newPacketFromStream, sioDest)

        # Short read
        for bll in [0x00, 0x01, 0x02]:
            first = 0x80 | (openpgpfile.PKT_PUBLIC_KEY << 2) | bll
            sioDest = util.ExtendedStringIO(chr(first))
            self.failUnlessRaises(openpgpfile.ShortReadError,
                                  PGP_Message.newPacketFromStream, sioDest)

        # New-style packets
        # 1-byte body length lengths
        first = 0x80 | 0x40 | openpgpfile.PKT_PUBLIC_KEY
        sioDest = util.ExtendedStringIO(chr(first) + chr(0))
        pkt = PGP_Message.newPacketFromStream(sioDest)
        self.failUnless(pkt._newStyle)
        self.failUnlessEqual(pkt.tag, openpgpfile.PKT_PUBLIC_KEY)
        self.failUnlessEqual(pkt.headerLength, 2)
        self.failUnlessEqual(pkt.bodyLength, 0)

        nsio = util.ExtendedStringIO()
        pkt.write(nsio)
        self.failUnlessEqual(sioDest.getvalue(), nsio.getvalue())
        # Invalid body length
        pkt.bodyLength = 193
        self.failUnlessRaises(openpgpfile.InvalidPacketError, pkt.write, nsio)
        pkt.headerLength = 1
        self.failUnlessRaises(openpgpfile.InvalidPacketError, pkt.write, nsio)

        sioDest = util.ExtendedStringIO(chr(first))
        self.failUnlessRaises(openpgpfile.ShortReadError,
                              PGP_Message.newPacketFromStream, sioDest)

        # 2-byte body length lengths
        bodyLen = 8000
        h1 = ((bodyLen - 192) >> 8) + 192
        h2 = (bodyLen - 192) % 256
        sioDest = util.ExtendedStringIO(chr(first) + chr(h1) + chr(h2)
            + ' ' * bodyLen )
        pkt = PGP_Message.newPacketFromStream(sioDest)
        self.failUnlessEqual(pkt.headerLength, 3)
        self.failUnlessEqual(pkt.bodyLength, bodyLen)

        nsio = util.ExtendedStringIO()
        pkt.write(nsio)
        self.failUnlessEqual(sioDest.getvalue(), nsio.getvalue())
        # Invalid body length
        pkt.bodyLength = 9000
        self.failUnlessRaises(openpgpfile.InvalidPacketError, pkt.write, nsio)
        pkt.bodyLength = 191
        self.failUnlessRaises(openpgpfile.InvalidPacketError, pkt.write, nsio)

        # Chop off one octet
        sioDest = util.ExtendedStringIO(sioDest.getvalue()[:-1])
        self.failUnlessRaises(openpgpfile.ShortReadError,
                              PGP_Message.newPacketFromStream, sioDest)

        sioDest = util.ExtendedStringIO(chr(first))
        self.failUnlessRaises(openpgpfile.ShortReadError,
                              PGP_Message.newPacketFromStream, sioDest)

        # 5-byte body length lengths
        bodyLen = bl = 0x01020304
        h4 = bl % 256; bl >>= 8
        h3 = bl % 256; bl >>= 8
        h2 = bl % 256; bl >>= 8
        h1 = bl
        h0 = 255
        sioDest = util.ExtendedStringIO(chr(first) + chr(h0) + chr(h1) +
            chr(h2) + chr(h3) + chr(h4) + " " * bodyLen)
        pkt = PGP_Message.newPacketFromStream(sioDest)
        self.failUnlessEqual(pkt.headerLength, 6)
        self.failUnlessEqual(pkt.bodyLength, bodyLen)

        nsio = util.ExtendedStringIO()
        pkt.write(nsio)
        self.failUnlessEqual(sioDest.getvalue(), nsio.getvalue())

        # Chop off one octet
        sioDest = util.ExtendedStringIO(sioDest.getvalue()[:-1])
        self.failUnlessRaises(openpgpfile.ShortReadError,
                              PGP_Message.newPacketFromStream, sioDest)

        sioDest = util.ExtendedStringIO(chr(first) + chr(255))
        self.failUnlessRaises(openpgpfile.ShortReadError,
                              PGP_Message.newPacketFromStream, sioDest)

        # Indeterminate length
        sioDest = util.ExtendedStringIO(chr(first) + chr(225))
        self.failUnlessRaises(NotImplementedError,
                              PGP_Message.newPacketFromStream, sioDest)

    def testNewPacketFromStream(self):
        f = open(self.getPublicFile())
        pkt = PGP_Message.newPacketFromStream(f)
        self.failUnlessEqual(pkt.tag, openpgpfile.PKT_PUBLIC_KEY)

    def testSigSubpackets(self):
        sio = util.ExtendedStringIO()
        # One-octet length length (& 0xC0 == 0)
        pktlen = 145
        sio.write(chr(pktlen))
        # Type
        sio.write(chr(16))
        # Data (short read expected)
        sio.write(" " * 10)

        # We need to wrap it into a SeekableNestedFile to avoid an IOError
        fobj = util.SeekableNestedFile(sio, sio.tell(), start = 0)
        try:
            openpgpfile.PGP_Signature._getNextSubpacket(fobj)
        except openpgpfile.ShortReadError, e:
            self.failUnlessEqual(e.expected, pktlen + 1)
            self.failUnlessEqual(e.actual, 12)

        sio.write(" " * (pktlen - 10 - 1))
        fobj = util.SeekableNestedFile(sio, sio.tell(), start = 0)
        pktt, dataf = openpgpfile.PGP_Signature._getNextSubpacket(fobj)
        self.failUnlessEqual(pktt, 16)

        sio.truncate(0)
        # Two-octet length
        pktlen = 203
        sio.write(chr(((pktlen - 192) >> 8) + 192))
        sio.write(chr((pktlen - 192)  & 0xFF))
        sio.write(chr(16))
        # Data (short read expected)
        sio.write(" " * 10)

        fobj = util.SeekableNestedFile(sio, sio.tell(), start = 0)
        try:
            openpgpfile.PGP_Signature._getNextSubpacket(fobj)
        except openpgpfile.ShortReadError, e:
            self.failUnlessEqual(e.expected, pktlen + 2)
            self.failUnlessEqual(e.actual, 13)

        sio.write(" " * (pktlen - 10 - 1))
        fobj = util.SeekableNestedFile(sio, sio.tell(), start = 0)
        pktt, dataf = openpgpfile.PGP_Signature._getNextSubpacket(fobj)
        self.failUnlessEqual(pktt, 16)

        sio.truncate(0)
        # Five-octet length
        pktlen = 203
        sio.write(chr(0xFF))
        sio.write(chr(0))
        sio.write(chr(0))
        sio.write(chr(pktlen >> 8))
        sio.write(chr(pktlen & 0xFF))
        sio.write(chr(16))
        # Data (short read expected)
        sio.write(" " * 10)

        fobj = util.SeekableNestedFile(sio, sio.tell(), start = 0)
        try:
            openpgpfile.PGP_Signature._getNextSubpacket(fobj)
        except openpgpfile.ShortReadError, e:
            self.failUnlessEqual(e.expected, pktlen + 5)
            self.failUnlessEqual(e.actual, 16)

        sio.write(" " * (pktlen - 10 - 1))
        fobj = util.SeekableNestedFile(sio, sio.tell(), start = 0)
        pktt, dataf = openpgpfile.PGP_Signature._getNextSubpacket(fobj)
        self.failUnlessEqual(pktt, 16)


    def testMessageIter(self):
        s = util.ExtendedStringIO(pubkey4)
        msg = openpgpfile.PGP_Message(s)
        for i, pkt in enumerate(msg.iterPackets()):
            pass
        self.failUnlessEqual(i, 7)

        s = util.ExtendedStringIO()
        msg = openpgpfile.PGP_Message(s)
        self.failUnlessRaises(StopIteration, msg.iterPackets().next)

    def testIterSubPackets(self):
        s = util.ExtendedStringIO()
        s.write(pubkey4)
        s.write(pubkey4)
        msg = openpgpfile.PGP_Message(s, 0)
        key = msg.iterPackets().next()
        for i, pkt in enumerate(key._iterSubPackets([openpgpfile.PKT_PUBLIC_KEY])):
            pass
        self.failUnlessEqual(i, 6)

    def testCountKeys(self):
        # Write the same key 3 times
        s = util.ExtendedStringIO()
        s.write(pubkey4)
        s.write(pubkey4)
        s.write(pubkey4)
        s.seek(0)
        self.failUnlessEqual(openpgpfile.countKeys(s), 3)

    def testXorStr(self):
        s1 = 'abc'
        s2 = '\0\0\0'
        self.failUnlessEqual(openpgpfile.xorStr(s1, s2), s1)
        self.failUnlessEqual(openpgpfile.xorStr(s1, s1), s2)
        self.failUnlessEqual(openpgpfile.xorStr(s1 + s1, s2), s1)
        self.failUnlessEqual(openpgpfile.xorStr(s1 + s2, s1), s2)

    def testSecretToPublic(self):
        s = util.ExtendedStringIO(seckey4)

        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterPackets().next()
        self.failUnlessEqual(pkt.tag, openpgpfile.PKT_SECRET_KEY)
        pkt = pkt.toPublicKey()

        sio1 = util.ExtendedStringIO()
        pkt.write(sio1)

        # Now read the public key

        pkt = openpgpfile.newKeyFromString(pubkey4)
        self.failUnlessEqual(pkt.tag, openpgpfile.PKT_PUBLIC_KEY)

        pkt = pkt.toPublicKey()
        sio2 = util.ExtendedStringIO()
        pkt.write(sio2)

        # Compare public key generated from private key with actual public key
        self.failUnlessEqual(sio1.getvalue(), sio2.getvalue())

    def testNewKeyFromString(self):
        self.failUnlessEqual(openpgpfile.newKeyFromString(""), None)

        pkt = openpgpfile.newKeyFromString(pubkey4)
        self.failUnless(pkt is not None)

        s = util.ExtendedStringIO()
        pkt.uids[0].write(s)
        self.failUnlessEqual(openpgpfile.newKeyFromString(s.getvalue()), None)

    def testGetKeyId(self):
        s = util.ExtendedStringIO(pubkey4)

        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterPackets().next()
        self.failUnlessEqual(pkt.tag, openpgpfile.PKT_PUBLIC_KEY)

        self.failUnlessEqual(pkt.getKeyFingerprint(),
            'A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5')

        self.failUnlessEqual(len(list(msg.iterByKeyId('91e3e6c5'))), 1)

        s = util.ExtendedStringIO(seckey4)
        self.failUnlessEqual(openpgpfile.getKeyId(s), 'A8E762BF91E3E6C5')


    def testGetUserIds(self):
        s = util.ExtendedStringIO(pubkey4)

        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterPackets().next()
        self.failUnlessEqual(pkt.tag, openpgpfile.PKT_PUBLIC_KEY)

        self.failUnlessEqual(list(pkt.getUserIds()),
                             ['Key 32 (Key 32) <misa+key32@rpath.com>'])

    def testIterKeys(self):
        s = util.ExtendedStringIO()
        s.write(seckey4)
        s.write(pubkey4)

        msg = openpgpfile.PGP_Message(s, 0)

        self.failUnlessEqual(len(list(msg.iterKeys())), 4)

        msg = openpgpfile.PGP_Message(self.getPrivateFile())
        pkt1 = msg.iterKeys().next()
        self.failUnlessEqual(pkt1.getKeyFingerprint(),
            '95B457D16843B21EA3FC73BBC7C32FC1F94E405E')

    def testGetFingerprints(self):
        s = util.ExtendedStringIO(pubkey4)

        self.failUnlessEqual(openpgpfile.getFingerprints(s),
            ['A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5',
             '7628DC8DA226B99BA8C0127D384E90BDA4F246A3'])

    def testGetFingerprint(self):
        fname = self.getPrivateFile()
        openpgpfile.getFingerprint('95B457D16843B21EA3FC73BBC7C32FC1F94E405E', fname)

    def testSeekParentKey(self):
        s = util.ExtendedStringIO(pubkey4)
        msg = openpgpfile.PGP_Message(s)
        pkt = msg.seekParentKey('a4f246a3')
        self.failIf(pkt == None)
        self.failUnlessEqual(pkt.getKeyFingerprint(),
            'A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5')

        pkt = msg.seekParentKey('91e3e6c5')
        self.failIf(pkt == None)
        self.failUnlessEqual(pkt.getKeyFingerprint(),
            'A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5')

    def testIterSignatures(self):
        s = util.ExtendedStringIO(pubkey4)
        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterKeys().next()
        self.failUnlessEqual(len(list(pkt.iterSignatures())), 0)
        uid = pkt.iterUserIds().next()
        self.failUnlessEqual(len(list(uid.iterSignatures())), 4)

        mpis = [ x.parseMPIs() for x in uid.iterSignatures() ]
        self.failUnlessEqual(mpis, [
            [1168068347633389024057926590005833843608616043828L,
                595964825934114565175348781234100148284520973233L],
            [821271448733149948345119563341076761482837395791L,
                654076866302568233756291427376364923552050626524L],
            [181372414576175374700012339219841736509870038901L,
                1088891501137002445099755118192266649532480357195L],
            [812476287238655482742968776229377983693375534522L,
                402689699558782885094775308192779683558940348977L]
        ])

    def testGetEndOfLife(self):
        s = util.ExtendedFile(self.getPublicFile(), buffering = False)
        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterByKeyId('DA44E4BD').next()

        pkt.uids[0].signatures[0].parse()
        pkt.uids[0].signatures[0].setVerifies(True)
        self.failUnlessEqual(pkt.getEndOfLife(), (False, 1142264067))

        pkt = msg.iterByKeyId('017111F7').next()
        pkt.bindingSig.parse()
        pkt.bindingSig.setVerifies(True)
        # Verify self sigs on the parent key too
        pkt.getMainKey().verifySelfSignatures()
        self.failUnlessEqual(pkt.getEndOfLife(), (False, 0))

    def testGetEndOfLife2(self):
        createdTimestamp = 1200007648
        offsets = [7788215, 15564288, 15564288, 31548693, 1309994]
        for i in range(5):
            fname = "expkey%s.gpg" % (i + 1)
            msg = openpgpfile.PGP_Message(self.getKeyring(fname))
            key = msg.iterMainKeys().next()
            key.verifySelfSignatures()
            revoked, eol = key.getEndOfLife()
            self.failIf(revoked)
            self.failUnlessEqual(eol, createdTimestamp + offsets[i])

    def testGetEndOfLife3(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('expkey6.gpg'))
        revokes = [0, 0, 0, 1200076923, 1200077503, 1200077503, 1200077503]
        keyIds = ['794ED826', 'DC5710AD', '75F744F4', 'A806D23D',
                  '843EABED', '28CA2785', 'E6D0F096']
        createdTimestamps = [ 1200007648, 1200072478, 1200077107, 1200072498,
                              1200077293, 1200077293, 1200077451 ]
        offsets = [15616820, 7776000, 31104000, 31104000,
                   5616000, 5616000, 4752000]
        for keyId, createdTimestamp, offset, revoke in zip(keyIds, createdTimestamps, offsets, revokes):
            key = msg.getKeyByKeyId(keyId)
            key.verifySelfSignatures()
            revoked, expired = key.getEndOfLife()
            self.failUnlessEqual(revoked, revoke)
            self.failUnlessEqual(expired, createdTimestamp + offset)

    def testReadKeyData(self):
        data = openpgpfile.exportKey('29BF4FCA',
                                       self.getKeyring('pk1.gpg'))
        self.failUnless(data)

        self.failUnlessRaises(openpgpfile.KeyNotFound,
                              openpgpfile.exportKey,
                                    'NOSUCHKEY',
                                    self.getKeyring('pk1.gpg'))

    def testSecretKeyDecrypt(self):
        s = util.ExtendedStringIO(seckey4)
        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterKeys().next()

        self.failUnlessEqual(pkt.decrypt('key32'),
            [708255119797086932650717083595073066992696472566L])

        self.failUnlessRaises(openpgpfile.BadPassPhrase, pkt.decrypt, 'blah')

        # DSA key
        skey = openpgpfile.seekKeyById('29BF4FCA',
                                       self.getKeyring('sk1.gpg'))
        skey.makePgpKey(passPhrase = 'key-revoc')

        # El-Gamal key
        skey = openpgpfile.seekKeyById('CBAE99C8',
                                       self.getKeyring('sk1.gpg'))
        self.failUnlessRaises(openpgpfile.MalformedKeyRing,
                              skey.makePgpKey, passPhrase = 'key-revoc')
        self.failUnlessRaises(openpgpfile.MalformedKeyRing,
                              skey.toPublicKey().makePgpKey)

    def testSecretKeyRecryptNoPass(self):
        s = util.ExtendedStringIO(seckey4)
        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterKeys().next()

        mpis = [708255119797086932650717083595073066992696472566L]
        self.failUnlessEqual(pkt.decrypt('key32'), mpis)

        subkeyMpis = [ sk.decrypt('key32') for sk in pkt.iterSubKeys() ]

        self.failUnlessRaises(openpgpfile.BadPassPhrase, pkt.decrypt, None)

        ret = pkt.recrypt('key32', None)
        self.failUnless(ret)
        sio = util.ExtendedStringIO()
        pkt.rewriteAll(sio)
        sio.seek(0)

        msg = openpgpfile.PGP_Message(sio)
        pkt = msg.iterKeys().next()
        self.failUnlessEqual(pkt.decrypt(None), mpis)

        self.failUnlessEqual([ sk.decrypt(None) for sk in pkt.iterSubKeys() ],
            subkeyMpis)

    def testSecretKeyRecryptWithPass(self):
        s = util.ExtendedStringIO(seckey4)
        msg = openpgpfile.PGP_Message(s)
        pkt = msg.iterKeys().next()

        mpis = [708255119797086932650717083595073066992696472566L]
        self.failUnlessEqual(pkt.decrypt('key32'), mpis)

        subkeyMpis = [ sk.decrypt('key32') for sk in pkt.iterSubKeys() ]

        self.failUnlessRaises(openpgpfile.BadPassPhrase, pkt.decrypt, None)

        newPassphrase = "dodo"

        ret = pkt.recrypt('key32', newPassphrase)
        self.failUnless(ret)
        sio = util.ExtendedStringIO()
        pkt.rewriteAll(sio)
        sio.seek(0)

        msg = openpgpfile.PGP_Message(sio)
        pkt = msg.iterKeys().next()
        self.failUnlessEqual(pkt.decrypt(newPassphrase), mpis)
        self.failUnlessEqual(
            [ sk.decrypt(newPassphrase) for sk in pkt.iterSubKeys() ],
            subkeyMpis)

    def testVerifySelfSignaturesO(self):
        s = util.ExtendedStringIO(pubkey4)
        openpgpfile.verifySelfSignatures('91E3E6C5', s)
        self.failUnlessRaises(openpgpfile.KeyNotFound, 
                              openpgpfile.verifySelfSignatures, 'NOSUCHKEY', s)

    def testVerifySelfSignatures(self):
        for keyrepr in (pubkey4, seckey4):
            s = util.ExtendedStringIO(keyrepr)
            msg = openpgpfile.PGP_Message(s)
            for pkt in msg.iterKeys():
                pkt.verifySelfSignatures()

                for subpkt in pkt.iterSubKeys():
                    subpkt.verifySelfSignatures()

                    # Mangle the binding sig to have an invalid short digest
                    if subpkt.bindingSig:
                        o = subpkt.bindingSig.hashSig
                        subpkt.bindingSig.setShortSigHash('\0\0')
                        subpkt.bindingSig._sigDigest = None
                        self.failUnlessRaises(BadSelfSignature,
                                              subpkt.verifySelfSignatures)
                        subpkt.bindingSig.hashSig = o

        pks = []
        for kid in [ 'CBAE99C8', '26423FB2' ]:
            pk = openpgpfile.seekKeyById(kid, self.getKeyring('pk3.gpg'))
            pk.verifySelfSignatures()
            pks.append(pk)

        # Munge one of the subkeys
        subk1 = pks[0]
        subk2 = pks[1]

        subk1.setBindingSig(subk2.bindingSig)
        subk2.verifySelfSignatures()
        self.failUnlessRaises(BadSelfSignature, subk1.verifySelfSignatures)

        # Random signatures
        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pubringrev.gpg'))
        subk1.setBindingSig(key.uids[0].signatures[0])
        self.failUnlessRaises(BadSelfSignature, subk1.verifySelfSignatures)

        key = openpgpfile.seekKeyById('91E3E6C5', util.ExtendedStringIO(pubkey4))
        subk1.setBindingSig(key.uids[0].signatures[0])
        self.failUnlessRaises(BadSelfSignature, subk1.verifySelfSignatures)

    def testVerifySelfSignaturesSecretSubKey(self):
        # CNY-2224, CNY-2258
        # If a secret key has an embedded signature, make sure we verify the
        # key against the public key, otherwise we'd need a passphrase to
        # decrypt the crypto key
        # The secret key in sk8 is also crippled (as witnessed by:
        # gnu-dummy S2K, algo: 3, SHA1 protection, hash: 2
        # in the output of gpg --list-packets < sk8.gpg
        key = openpgpfile.seekKeyById('0FD4B672',
                                      self.getKeyring('sk8.gpg'))
        key.subkeys[1].verifySelfSignatures()

    def testVerifyDocumentSignature(self):
        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pubringrev.gpg'))
        doc = file(self.getKeyring('pk2.gpg'))
        sigfile = self.getKeyring('pk2.gpg.sig')
        # Read signature
        msg = openpgpfile.PGP_Message(sigfile)
        sig = msg.iterPackets().next()

        digest = sig.getDocumentHash(doc)

        cryptoKey = key.getCryptoKey()
        sig.verifyDocument(cryptoKey, doc)

        # Pretend sig type is text
        sig.sigType = openpgpfile.SIG_TYPE_TEXT_DOC
        self.failUnlessRaises(openpgpfile.PGPError, sig.verifyDocument,
                              cryptoKey, doc)
        sig.sigType = openpgpfile.SIG_TYPE_BINARY_DOC

        # Alter the short hash sig
        hashSig = sig.getShortSigHash()
        sig.setShortSigHash('AA')
        self.failUnlessRaises(openpgpfile.SignatureError, sig.verifyDocument,
                              cryptoKey, doc)
        sig.setShortSigHash(hashSig)

        sig.verifyDocument(cryptoKey, doc)

        # Alter the signature
        x = util.ExtendedStringIO(sig.mpiFile.pread(sig.mpiFile.size, 0))
        x.seek(12)
        x.write('\0')
        sig.mpiFile = x

        self.failUnlessRaises(openpgpfile.SignatureError, sig.verifyDocument,
                              cryptoKey, doc)

        # Mess up the signature version
        sig.version = 5
        self.failUnlessRaises(openpgpfile.InvalidKey, sig.getDocumentHash, doc)

        # Verify v4 signature
        sigfile = self.getKeyring('pk2.gpg.sig2')
        # Read signature
        sig = openpgpfile.readSignature(file(sigfile))

        sig.verifyDocument(cryptoKey, doc)

    def testReadSignature(self):
        sigfile = self.getKeyring('pk2.gpg.sig')
        sig = openpgpfile.readSignature(file(sigfile))
        sigfile = self.getKeyring('pk2.gpg.sig2')
        sig = openpgpfile.readSignature(file(sigfile))
        sio = util.ExtendedStringIO("cadfadf")
        try:
            openpgpfile.readSignature(sio)
        except openpgpfile.InvalidPacketError, e:
            self.failUnlessEqual(str(e), "No data found")
        else:
            self.fail("Expected an InvalidPacketError")

        sio.truncate(0)
        try:
            openpgpfile.readSignature(sio)
        except openpgpfile.InvalidPacketError, e:
            self.failUnlessEqual(str(e), "No data found")
        else:
            self.fail("Expected an InvalidPacketError")

        sio.write('\x80adfadf')
        sio.seek(0)
        try:
            openpgpfile.readSignature(sio)
        except openpgpfile.InvalidPacketError, e:
            self.failUnlessEqual(str(e), "Error reading signature packet")
        else:
            self.fail("Expected an InvalidPacketError")

        try:
            openpgpfile.readSignature(open(self.getKeyring('pubringrev.gpg')))
        except openpgpfile.InvalidPacketError, e:
            self.failUnlessEqual(str(e), "Not a signature packet")
        else:
            self.fail("Expected an InvalidPacketError")


    def testInitSubPackets(self):
        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pubringrev.gpg'))
        self.failUnlessEqual(len(key.revsigs), 1)
        self.failUnlessEqual(len(key.uids), 3)
        self.failUnlessEqual(len(key.subkeys), 3)

        # This key is revoked
        key.verifySelfSignatures()
        revoked, timestamp = key.getEndOfLife()
        self.failUnlessEqual(revoked, 1185566101)
        self.failUnlessEqual(timestamp, 1193325420)

        # This key is revoked, let's verify its self signatures
        key.verifySelfSignatures()

    def testParseAsciiArmorKey(self):
        self.failUnlessEqual(openpgpfile.parseAsciiArmorKey(pubkeya4), pubkey4)

        # Check CRC
        nk = pubkeya4.replace('MgNw', 'zero')
        self.failUnlessRaises(openpgpfile.PGPError,
            openpgpfile.parseAsciiArmorKey, nk)

        # No CRC is ok
        nk = pubkeya4.replace('=MgNw\n', '')
        openpgpfile.parseAsciiArmorKey(nk)

        self.failUnlessEqual(openpgpfile.parseAsciiArmorKey(''), None)
        # Bad base64 encoding
        self.failUnlessEqual(openpgpfile.parseAsciiArmorKey('-\nSome text\n\n\0+++\n-\n'), None)
        self.failUnlessEqual(openpgpfile.parseAsciiArmorKey('-\nSome text\n\nA\n-\n'), None)

    def testParseAsciiArmor(self):
        sio = util.ExtendedStringIO()
        ret = openpgpfile.parseAsciiArmor(util.ExtendedStringIO(pubkeya4), sio)
        sio.seek(0)
        self.failUnlessEqual(sio.read(), pubkey4)
        # Binary key
        sio.truncate(0)
        ret = openpgpfile.parseAsciiArmor(util.ExtendedStringIO(pubkey4), sio)
        sio.seek(0)
        self.failUnlessEqual(sio.read(), pubkey4)

    # Not really a test, just a way to mangle keys by removing revocations
    def ttestManglePacket(self):
        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pubringrev.gpg'))
        key.revsigs = []
        key.writeAll(open("/tmp/npk", "w"))

        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('secringrev.gpg'))
        key.revsigs = []
        key.writeAll(open("/tmp/nsk", "w"))

    def testSigV3(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('v3key.gpg'))
        key = msg.iterKeys().next()
        # We can now verify self signatures on v3 keys
        key.verifySelfSignatures()
        self.failUnlessEqual(key.getKeyId(), '7123BC13ED9D77D5')
        self.failUnlessEqual(key.getKeyFingerprint(), 'D334F25FD714E0906203EF2D7E4AA598')
        # Test that we can fetch the key both by fingerprint and by key id
        # (which, for v3 keys, have no relationship one to another)
        pkt = msg.iterByKeyId('ED9D77D5').next()
        pkt = msg.iterByKeyId('EF2D7E4AA598').next()

    def testIsSupersetOf(self):
        pkey0 = openpgpfile.seekKeyById('29BF4FCA',
                                        self.getKeyring('pubringrev.gpg'))
        pkey0.verifySelfSignatures()
        revoc0, ts0 = pkey0.getEndOfLife()
        self.failUnless(revoc0)
        self.failUnlessEqual(ts0, 1193325420)
        self.failUnless(pkey0.isSupersetOf(pkey0))

        pkey1 = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk1.gpg'))
        pkey1.verifySelfSignatures()
        revoc1, ts1 = pkey1.getEndOfLife()
        self.failIf(revoc1)
        self.failUnlessEqual(ts1, 1193757244)
        self.failUnless(pkey1.isSupersetOf(pkey1))

        pkey3 = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk3.gpg'))
        pkey3.verifySelfSignatures()
        revoc3, ts3 = pkey3.getEndOfLife()
        self.failIf(revoc3)
        self.failUnlessEqual(ts3, 1217523028)
        self.failUnless(pkey3.isSupersetOf(pkey3))

        for origkey in [ pkey0, pkey1, pkey3 ]:
            newkey = origkey.clone()
            newkey.initSubPackets()
            if origkey.revsigs:
                # Drop revocations
                newkey.revsigs = []
                self.failUnless(origkey.isSupersetOf(newkey))
                self.failIf(newkey.isSupersetOf(origkey))

            # Drop a user ID
            newkey = origkey.clone()
            newkey.initSubPackets()
            newkey.uids = newkey.uids[1:]
            self.failUnless(origkey.isSupersetOf(newkey))
            self.failIf(newkey.isSupersetOf(origkey))

            # Drop a subkey
            newkey = origkey.clone()
            newkey.initSubPackets()
            newkey.subkeys = newkey.subkeys[1:]
            self.failUnless(origkey.isSupersetOf(newkey))
            self.failIf(newkey.isSupersetOf(origkey))

    def testSigUserAttr(self):
        pkey3 = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk3.gpg'))
        pkey3.verifySelfSignatures()
        userIdsList = [ x for x in pkey3.iterUserIds()]
        self.failUnlessEqual(len(userIdsList), 3)
        self.failUnlessEqual(len(pkey3.getUserIds()), 3)
        userAttr = userIdsList[2]
        self.failUnlessEqual(userAttr.tag, openpgpfile.PKT_USER_ATTRIBUTE)

        # Reorder uids
        pkey3.uids = [ userIdsList[2] ] + userIdsList[:2]

        sio = util.ExtendedStringIO()
        pkey3.writeAll(sio)
        sio.seek(0)

        npk3 = openpgpfile.PGP_Message(sio).iterMainKeys().next()
        userIdsList = [ x for x in npk3.iterUserIds()]
        self.failUnlessEqual(len(userIdsList), 3)
        userAttr = userIdsList[0]
        self.failUnlessEqual(userAttr.tag, openpgpfile.PKT_USER_ATTRIBUTE)


    def testSecretKeySelfSig(self):
        # CNY-2064
        pkt = openpgpfile.seekKeyById('05C54D73',
                                      self.getKeyring('sk4.gpg'))
        try:
            pkt.verifySelfSignatures()
        except openpgpfile.BadSelfSignature:
            raise testhelp.SkipTestException("Need to fix CNY-2047")
        else:
            raise Exception("Remove SkipTestException")

    def testSomeFailingSelfSigs(self):
        # CNY-2439
        msg = PGP_Message(self.getKeyring('pk-bad-self-sigs.gpg'))
        key = msg.iterMainKeys().next()

        key.verifySelfSignatures()


    def testGetPrivateKeyWarning(self):
        # CNY-2047
        msg = PGP_Message(self.getKeyring('sk4.gpg'))
        key = msg.getKeyByKeyId('05C54D73')
        ret, txt = self.captureOutput(key.getCryptoKey, '',
                           _returnException = True)
        if not isinstance(ret, openpgpfile.BadPassPhrase):
            raise Exception("BadPassPhrase not raised")
        substr = "self-signature on private key does not verify"
        self.failUnless(substr in txt, "%s not in %s" % (
                        repr(substr), repr(txt)))

    def testWriteSigSubpacket(self):
        stream = util.ExtendedStringIO()
        wr = openpgpfile.PGP_Signature._writeSubpacket
        rd = openpgpfile.PGP_Signature._decodeSigSubpackets

        # Subpacket length includes the subpacket type octet.
        # The interesting values are < 192, < 16320, >= 16320
        packets = []
        # 1-octet length
        packets.append((1, util.ExtendedStringIO(" " * 190)))
        # 2-octet length
        packets.append((2, util.ExtendedStringIO(" " * 16318)))
        # 5-octet length
        packets.append((5, util.ExtendedStringIO(" " * 16319)))

        for spktType, spktStream in packets:
            wr(stream, spktType, spktStream)
        stream.seek(0)

        ret = rd(stream)
        for spkt, tgt in zip(packets, ret):
            self.failUnlessEqual(spkt[0], tgt[0])
            spkt[1].seek(0)
            tgt[1].seek(0)
            self.failUnlessEqual(spkt[1].read(), tgt[1].read())

    def testIterCertifications(self):
        key = openpgpfile.seekKeyById('29BF4FCA',
            self.getKeyring('pubringrev.gpg'))
        certs = [ x for x in key.iterCertifications() ]
        self.failUnlessEqual(len(certs), 7)

    def testMergeKeys(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('pk2.gpg'))
        pkt = msg.iterKeys().next()

        merged = pkt.merge(pkt)
        self.failIf(merged)

        pkt1 = self._cloneKey(pkt)
        # Drop both the binding sig and the revocation from one of the subkeys
        pkt1.subkeys[0].bindingSig = None

        self.failUnlessRaises(BadSelfSignature, pkt1.subkeys[0].verifySelfSignatures)
        self.failUnlessRaises(BadSelfSignature, pkt1.merge, pkt)

        pkt2 = self._cloneKey(pkt)
        self.failUnlessRaises(BadSelfSignature, pkt2.merge, pkt1)

        # Revoked key - make sure the revocation is merged
        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pubringrev.gpg'))
        pkt1 = self._cloneKey(key)
        self.failUnlessEqual(len(pkt1.revsigs), 1)
        del pkt1.revsigs[:]
        self.failUnlessEqual(len(pkt1.revsigs), 0)

        merged = pkt1.merge(key)
        self.failUnless(merged)
        self.failUnlessEqual(len(pkt1.revsigs), 1)

        otherkey = openpgpfile.seekKeyById('91E3E6C5',
                util.ExtendedStringIO(pubkey4))
        self.failUnlessRaises(openpgpfile.MergeError,
                otherkey.merge, pkt1)

    def testMergeKeyUids(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('pk2.gpg'))
        pkt = msg.iterKeys().next()

        pkt1 = self._cloneKey(pkt)
        pkt2 = self._cloneKey(pkt)

        del pkt1.uids[1]
        del pkt2.uids[2]

        del pkt1.uids[0].signatures[1]
        del pkt2.uids[0].signatures[2:]
        self.failUnlessEqual(len(pkt1.uids[0].signatures), 3)
        self.failUnlessEqual(len(pkt2.uids[0].signatures), 2)

        set1 = set(pkt1.getUserIds())
        set2 = set(pkt2.getUserIds())

        merged = pkt1.merge(pkt2)
        self.failUnless(merged)
        self.failUnlessEqual(set1.union(set2), set(pkt1.getUserIds()))

        self.failUnlessEqual(len(pkt1.uids[0].signatures), 4)

        # Change in signatures only
        pkt1 = self._cloneKey(pkt)
        pkt2 = self._cloneKey(pkt)

        del pkt1.uids[0].signatures[1]
        del pkt2.uids[0].signatures[2:]
        merged = pkt1.merge(pkt2)
        self.failUnless(merged)

        self.failUnlessEqual(len(pkt1.uids[0].signatures), 4)

        # Verify that we can't merge random uids
        self.failUnlessRaises(openpgpfile.MergeError,
                              pkt1.uids[0].merge, pkt1.uids[1])

        # Verify that we can't merge random sigs
        sig1 = pkt1.uids[0].signatures[0]
        sig2 = pkt1.uids[0].signatures[1]
        self.failUnlessRaises(openpgpfile.MergeError,
                              sig1.merge, sig2)

        sig2 = pkt2.uids[0].signatures[0]
        # Mangle one of the digest sigs
        sig1.hashSig = [0, 0]
        self.failUnlessRaises(openpgpfile.MergeError, sig1.merge, sig2)

    def testMergeAssertions(self):
        # Verify assertions
        msg = openpgpfile.PGP_Message(self.getKeyring('pk3.gpg'))
        pkt = msg.iterKeys().next()

        uid = pkt.uids[0]
        uatt = pkt.uids[2]
        sig = uid.signatures[0]
        sub = pkt.subkeys[0]

        self.failUnlessEqual(uid.tag, openpgpfile.PKT_USERID)
        self.failUnlessEqual(uatt.tag, openpgpfile.PKT_USER_ATTRIBUTE)

        self.failUnlessRaises(AssertionError, sig.merge, uatt)
        self.failUnlessRaises(AssertionError, sig.merge, uid)
        self.failUnlessRaises(AssertionError, sig.merge, pkt)
        self.failUnlessRaises(AssertionError, uid.merge, uatt)
        self.failUnlessRaises(AssertionError, uid.merge, sig)
        self.failUnlessRaises(AssertionError, uid.merge, pkt)
        self.failUnlessRaises(AssertionError, pkt.merge, sig)
        self.failUnlessRaises(AssertionError, pkt.merge, uid)
        self.failUnlessRaises(AssertionError, sub.merge, pkt)
        self.failUnlessRaises(AssertionError, sub.merge, uid)

    def testMergeKeySubkeys(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('pk2.gpg'))
        pkt = msg.iterKeys().next()

        pkt1 = self._cloneKey(pkt)
        pkt2 = self._cloneKey(pkt)

        del pkt1.subkeys[0]
        del pkt2.subkeys[1:]

        merged = pkt1.merge(pkt2)
        self.failUnless(merged)
        self.failUnlessEqual(len(pkt1.subkeys), 3)

        # Both subkeys keys are equal in terms of signatures
        pkt1 = self._cloneKey(pkt)
        pkt2 = self._cloneKey(pkt)

        pkt1.subkeys[2].bindingSig = None
        pkt2.subkeys[2].bindingSig = None
        merged = pkt1.merge(pkt2)
        self.failIf(merged)

        # This forces it - different revocation (though it's not a revocation)
        pkt2.subkeys[2].setRevocationSig(pkt.subkeys[2].bindingSig.clone())
        # We should prefer ours
        merged = pkt1.merge(pkt2)
        self.failIf(merged)

        # The other key doesn't have a revocation, and our key has both a
        # binding sig and a revocation. Our binding sig should go
        pkt1 = self._cloneKey(pkt)
        pkt2 = self._cloneKey(pkt)
        self.failIf(pkt1.subkeys[2].bindingSig == None)
        self.failIf(pkt1.subkeys[2].revocationSig == None)
        pkt2.subkeys[2].revocationSig = None

        merged = pkt1.merge(pkt2)
        self.failUnless(merged)
        self.failUnlessEqual(pkt1.subkeys[2].bindingSig, None)

        pkt1 = self._cloneKey(pkt)
        pkt2 = self._cloneKey(pkt)
        sig1 = pkt1.subkeys[0].bindingSig

        osig = pkt1.subkeys[1].bindingSig
        osig.decodeUnhashedSubpackets()

        sig2 = pkt1.subkeys[0].bindingSig
        sig2.decodeUnhashedSubpackets()

        # Copy embedded sig from another sig
        self.failUnlessEqual(osig._unhashedSubPackets[1][0], 32)
        sig2._unhashedSubPackets.append(osig._unhashedSubPackets[1])

        # The embedded signature gets ignored
        pkt1.subkeys[0].verifySelfSignatures()

    def _cloneKey(self, key):
        nkey = key.clone()
        nkey.revsigs = []
        for rsig in key.revsigs:
            nrsig = rsig.clone()
            nkey.revsigs.append(nrsig)
            nrsig.setParentPacket(nkey)

        nkey.uids = []
        for uid in key.iterUserIds():
            nuid = uid.clone()
            nkey.uids.append(nuid)
            nuid.setParentPacket(nkey)
            nuid.signatures = [ x.clone() for x in uid.iterSignatures() ]
            for sig in nuid.signatures:
                sig.setParentPacket(nuid)
        nkey.subkeys = []
        for skey in key.iterSubKeys():
            nskey = skey.clone()
            nkey.subkeys.append(nskey)
            nskey.setParentPacket(nkey)
            if skey.bindingSig:
                nskey.setBindingSig(skey.bindingSig.clone())
            if skey.revocationSig:
                nskey.setRevocationSig(skey.revocationSig.clone())
        return nkey

    def testSignatureRewrite(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('pk3.gpg'))
        pkt = msg.iterKeys().next()

        # Grab one of the signatures
        sig = pkt.uids[0].signatures[0]
        sig.parse()
        subpackets = sig.decodeUnhashedSubpackets()

        # Add a notation on the key
        stream = util.ExtendedStringIO()
        flags = [ 0x80, 0, 0, 0 ]
        name = "notation@rpath.com"
        value = "some value"
        for f in flags:
            stream.write(chr(f))
        for v in [name, value]:
            l = len(v)
            stream.write(chr((l >> 8) & 0xFF))
            stream.write(chr(l & 0xFF))
        for v in [name, value]:
            stream.write(v)

        subpackets.append((20, stream))
        sig._prepareSubpackets()
        x = util.ExtendedStringIO()
        sig.rewriteBody()
        # At this point, some of the internal structures should have been
        # reset
        self.failUnlessEqual(sig._hashedSubPackets, None)
        self.failUnlessEqual(sig._unhashedSubPackets, None)

        sig.decodeUnhashedSubpackets()
        self.failUnlessEqual(len(sig._unhashedSubPackets), 2)
        self.failUnlessEqual(sig._unhashedSubPackets[1][0], 20)

        # Invalid pubkey alg
        sig.pubKeyAlg = 99
        self.failUnlessRaises(openpgpfile.UnsupportedEncryptionAlgorithm,
            sig.parseMPIs)
        try:
            sig.parseMPIs()
        except openpgpfile.UnsupportedEncryptionAlgorithm, e:
            self.failUnlessEqual(str(e), "Unsupported encryption algorithm code 99")

        # Make sure parse() does nothing
        sig.parse()
        self.failUnlessEqual(sig.pubKeyAlg, 99)

        # Force the parsing - this should reset everything
        sig.parse(force = True)
        self.failUnlessEqual(sig.pubKeyAlg, 17)
        self.failUnlessEqual(sig._hashedSubPackets, None)
        self.failUnlessEqual(sig._unhashedSubPackets, None)

    def testSign(self):
        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk3.gpg'))
        sigkey1 = openpgpfile.seekKeyById('7A746BAE',
                                      self.getKeyring('secringrev.gpg'))
        # This is an RSA key
        sigkey2 = openpgpfile.seekKeyById('D5E8FF1A',
                                      self.getKeyring('sk6.gpg'))

        uid = key.uids[2]

        trustRegex = r'<[^>]+[@.]rpath\.com>$'
        for (sigkey, passphrase) in [(sigkey1, 'key20'), (sigkey2, 'key-rsa')]:
            expsec = 3600 * 24 * 7
            cryptoKey = sigkey.toPublicKey().makePgpKey()
            sigkey.sign(uid, expiration = expsec,
                        trustLevel = 1, trustAmount = 40,
                        trustRegex = trustRegex,
                        passwordCallback = (lambda x=None: passphrase),)

            # Verify signature
            sig = uid.signatures[-1]
            sig.verify(cryptoKey, key.getKeyId())
            # Check sig expiration
            self.failUnlessEqual(expsec, sig.getExpiration())
            # Make sure we have a trust level
            pkts = [ x[1] for x in sig.decodeHashedSubpackets()
                     if x[0] == openpgpfile.SIG_SUBPKT_TRUST ]
            self.failUnlessEqual(len(pkts), 1)
            pkts[0].seek(0)
            tl, ta = sig._readBin(pkts[0], 2)
            self.failUnlessEqual(tl, 1)
            self.failUnlessEqual(ta, 40)

            # And a trust regex - critical packet (hence 0x80)
            pkts = [ x[1] for x in sig.decodeHashedSubpackets()
                     if x[0] == (0x80 | openpgpfile.SIG_SUBPKT_REGEX) ]
            self.failUnlessEqual(len(pkts), 1)
            pkts[0].seek(0)
            # Null-terminated
            self.failUnlessEqual(pkts[0].read(), trustRegex + '\0')

            # Testing getTrust
            self.failUnlessEqual(sig.getTrust(), (1, 40, trustRegex))

            # Add signature that expires when the key expires

            keyExpiration = uid.getExpiration()
            keyCreation = key.createdTimestamp

            # expiration = None should create a sig that expires with the key
            # (or doesn't expire at all, if the uid doesn't specify a key
            # expiration)
            expsec = 1024
            sigCreation = keyCreation + keyExpiration - expsec

            sigkey.sign(uid, expiration = None, creation = sigCreation,
                        passwordCallback = (lambda x=None: passphrase),)
            sig = uid.signatures[-1]
            sig.verify(cryptoKey, key.getKeyId())
            # Check sig expiration
            self.failUnlessEqual(expsec, sig.getExpiration())

            # Signature that does not expire
            sigkey.sign(uid, expiration = -1,
                        sigType = openpgpfile.SIG_TYPE_CERT_3,
                        passwordCallback = (lambda x=None: passphrase),)
            sig = uid.signatures[-1]
            sig.verify(cryptoKey, key.getKeyId())
            # Check sig expiration
            self.failUnlessEqual(None, sig.getExpiration())
            self.failUnlessEqual(sig.sigType, openpgpfile.SIG_TYPE_CERT_3)

        # A key that doesn't expire
        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk2.gpg'))
        uid = key.uids[2]

        for (sigkey, passphrase) in [(sigkey1, 'key20'), (sigkey2, 'key-rsa')]:
            cryptoKey = sigkey.toPublicKey().makePgpKey()
            sigkey.sign(uid, passwordCallback = (lambda x=None: passphrase),)

            sig = uid.signatures[-1]
            sig.verify(cryptoKey, key.getKeyId())
            # Check sig expiration
            self.failUnlessEqual(None, sig.getExpiration())

        key = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk7.gpg'))
        uid = key.uids[2]

        # expiration should be -1, key is revoked
        self.failUnlessEqual(uid.getExpiration(), -1)

        for (sigkey, passphrase) in [(sigkey1, 'key20'), (sigkey2, 'key-rsa')]:
            cryptoKey = sigkey.toPublicKey().makePgpKey()
            self.failUnlessRaises(openpgpfile.SignatureError,
                sigkey.sign, uid,
                passwordCallback = (lambda x=None: passphrase),)

    def testAdoptSignature(self):
        # We want to make sure that, as a side-effect of verifying self
        # signatures, subpackets get their parent set. This is because I had
        # weird cases where I was expecting something to fail and it wasn't,
        # just because the signature I was copying around was verifying, since
        # it pointed to a different key or user id
        key1 = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk3.gpg'))
        key2 = openpgpfile.seekKeyById('7A746BAE',
                                      self.getKeyring('secringrev.gpg'))

        sig1 = key1.uids[0].signatures[0].clone()
        sig1.setParentPacket(None)

        key1.revsigs.append(sig1)
        # We expect this to fail
        self.failUnlessRaises(BadSelfSignature, key1.verifySelfSignatures)
        self.failUnlessEqual(sig1.getParentPacket().getKeyId(), key1.getKeyId())

        # Same thing, with a totally random key as a parent
        sig1.setParentPacket(key2)

        self.failUnlessRaises(BadSelfSignature, key1.verifySelfSignatures)
        self.failUnlessEqual(sig1.getParentPacket().getKeyId(), key1.getKeyId())

        # Same for uids
        key1.revsigs = []

        sig1.setParentPacket(None)
        # We need to make sure we don't have a good self signature side by
        # side with a bad one. CNY-2439 had a problem with a valid key from
        # gnupg's perspective, even though it had invalid self signatures
        key1.uids[1].signatures = [sig1]
        self.failUnlessRaises(BadSelfSignature, key1.verifySelfSignatures)
        self.failUnlessEqual(sig1.getParentPacket().id, key1.uids[1].id)

        sig1.setParentPacket(key1.uids[0])
        key1.uids[1].signatures.append(sig1)
        self.failUnlessRaises(BadSelfSignature, key1.verifySelfSignatures)
        self.failUnlessEqual(sig1.getParentPacket().id, key1.uids[1].id)

        key1 = openpgpfile.seekKeyById('29BF4FCA',
                                      self.getKeyring('pk3.gpg'))

        subk = key1.subkeys[0]

        # Finally, same for subkeys
        sig1.setParentPacket(None)
        subk.bindingSig = sig1
        self.failUnlessRaises(BadSelfSignature, subk.verifySelfSignatures)
        self.failUnlessEqual(sig1.getParentPacket().getKeyId(), subk.getKeyId())

        sig1.setParentPacket(key1.subkeys[1])
        subk.bindingSig = sig1
        self.failUnlessRaises(BadSelfSignature, subk.verifySelfSignatures)
        self.failUnlessEqual(sig1.getParentPacket().getKeyId(), subk.getKeyId())

    def test_crc24(self):
        data = [ ('',       '\xb7\x04\xce', 'twTO'),
                 ('a',      '\xf2\x57\x13', '8lcT'),
                 ('aa',     '\xad\x83\xdb', 'rYPb'),
                 ('abc',    '\xba\x1c\x7b', 'uhx7'),
               ]
        for src, expected, b64exp in data:
            self.failUnlessEqual(openpgpfile.crc24(src), expected)
            self.failUnlessEqual(openpgpfile.crc24base64(src), b64exp)

    def testGetSignatureHash(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('pk2.gpg'))
        pkt = msg.iterKeys().next()

        sig = pkt.uids[0].signatures[0]
        shash = sig.getSignatureHash()
        s2a = openpgpfile.stringToAscii
        b2s = openpgpfile.binSeqToString

        self.failUnlessEqual(s2a(shash), 'B7719A5519EF8CFCDF91074A465CDAC4E0848FDC')
        self.failUnlessEqual(sig.hashSig, '\xB7\x71')

        # Mangle the sig to have an invalid parent
        sig._sigDigest = None
        sig._parentPacket = pkt.uids[0].signatures[1]
        self.failUnlessRaises(openpgpfile.InvalidPacketError, sig.getSignatureHash)

    def testMPIOps(self):
        tests = [0, 1, 127, 511, 987654321987654321987654321987654321987654321,
                 0, 1]
        wr = openpgpfile.PGP_Signature._writeMPI
        rd = openpgpfile.PGP_Signature._readCountMPIs
        stream = util.ExtendedStringIO()
        for i, tst in enumerate(tests):
            stream.seek(0, 2)
            wr(stream, tst)
            stream.seek(0)
            mpis = rd(stream, i + 1, discard = False)
            self.failUnlessEqual(mpis, tests[:i+1])

    def testIgnoreV2Keyses(self):
        # CNY-2155
        # Create a v2 key
        sio = util.SeekableNestedFile(util.ExtendedStringIO("\02"), 1)
        pkt = PGP_Message.newPacket(openpgpfile.PKT_PUBLIC_KEY, sio)

        # Write it
        stream = util.ExtendedStringIO()
        pkt.write(stream)

        # Write a valid key after it
        stream.write(pubkey4)
        stream.seek(0)

        msg = openpgpfile.PGP_Message(stream)

        # Make sure the v2 key is ignored
        self.failUnlessEqual(1, len([ x for x in msg.iterMainKeys() ]))

    def testDirectKeySig(self):
        # CNY-2120
        pk4 = openpgpfile.seekKeyById('B3334559',
                                      self.getKeyring('pk4.gpg'))

        pk5 = openpgpfile.seekKeyById('F3C198B6',
                                      self.getKeyring('pk5.gpg'))
        pk5.verifySelfSignatures()

    def testAddKey(self):
        # Test that adding keys works and properly locks the keyring
        k1 = openpgpfile.seekKeyById('60800B90',
                                     self.getKeyring('pubringrev.gpg'))
        k2 = openpgpfile.seekKeyById('E28A4D73',
                                     self.getKeyring('pubringrev.gpg'))
        k3 = openpgpfile.seekKeyById('7A746BAE',
                                     self.getKeyring('pubringrev.gpg'))

        keyring = os.path.join(self.workDir, "test-keyring")
        try:
            os.unlink(keyring)
        except OSError:
            pass

        stream = open(keyring, "w")
        tsks = k2.subkeys
        k2.subkeys = []

        k2.writeAll(stream)
        stream.close()

        # Add subkeys back
        k2.subkeys = tsks

        # We want to make sure the child process is the first to add the keys,
        # and that in the parent we have both keys.
        pip = os.pipe()
        pid = os.fork()
        try:
            if pid == 0:
                # In the child
                os.close(pip[0])
                stream = open(keyring, "r+")
                # Lock the stream
                fcntl.lockf(stream.fileno(), fcntl.LOCK_EX)
                # Unblock parent
                os.write(pip[1], "1")
                os.close(pip[1])
                openpgpfile.addKeys([k1, k2], keyring)
                os._exit(0)

            os.close(pip[1])
            # Wait for child to lock
            data = os.read(pip[0], 1)
            os.close(pip[0])
            openpgpfile.addKeys([k2, k3], keyring)

            msg = openpgpfile.PGP_Message(keyring, start = 0)
            keys = [ k for k in msg.iterMainKeys() ]
            self.failUnlessEqual(len(keys), 3)
            self.failUnlessEqual(keys[0].getKeyId(), k2.getKeyId())
            self.failUnlessEqual(keys[1].getKeyId(), k1.getKeyId())
            self.failUnlessEqual(keys[2].getKeyId(), k3.getKeyId())
            # Make sure key 2 has subkeys
            self.failUnlessEqual(len(keys[0].subkeys), 1)
        finally:
            os.waitpid(pid, 0)

    def testExportKey(self):
        keyring = self.getKeyring('pubringrev.gpg')
        kId1 = 'A8E762BF91E3E6C5'
        sio = openpgpfile.exportKey(kId1, keyring)
        key1 = openpgpfile.PGP_Message(sio).iterMainKeys().next()
        self.failUnlessEqual(key1.getKeyId(), kId1)
        # Subkey
        kId2 = '384E90BDA4F246A3'
        sio = openpgpfile.exportKey(kId2, keyring)
        key2 = openpgpfile.PGP_Message(sio).iterMainKeys().next()
        self.failUnlessEqual(key2.getKeyId(), kId1)
        self.failUnlessEqual(key2.subkeys[0].getKeyId(), kId2)

        # Finally, export as armored
        sio = openpgpfile.exportKey(kId2, keyring, armored = True)
        self.failUnlessEqual(sio.pread(5, 0), '-----')

        self.failUnlessRaises(openpgpfile.KeyNotFound,
                              openpgpfile.exportKey, 'aaa', '/no/such/file')

    def testVersion3KeySelfSig(self):
        # CNY-2420
        for f in ['v3key.gpg', 'v3key.gpg']:
            msg = openpgpfile.PGP_Message(self.getKeyring(f))
            key = msg.iterMainKeys().next()
            key.verifySelfSignatures()

    def testVersion3Sig(self):
        sig = self.getKeyring('ver3.sig.asc')
        sig = openpgpfile.parseAsciiArmorKey(file(sig).read())
        sio = util.ExtendedStringIO(sig)
        msg = openpgpfile.PGP_Message(sio)
        sigpkt = msg.iterPackets().next()
        self.failUnlessEqual(sigpkt.getSigId(), '\xc8k\xa0jQ}\x0f\x0e')

    def testKeyWithVersion2Sig(self):
        # CNY-2417
        msg = openpgpfile.PGP_Message(self.getKeyring('keyv2sig.gpg'))
        key = msg.iterMainKeys().next()
        self.failUnless(key.verifySelfSignatures())

    def testKeyWithExtraSigsOnSubkey(self):
        # CNY-2490
        # Concatenate the key several times, to make sure we stop at the right
        # place
        kr = os.path.join(self.workDir, 'keyring')
        f = file(kr, "w+")
        src = file(self.getKeyring('pk-extra-subkey-sigs.gpg'))
        for i in range(3):
            src.seek(0)
            openpgpfile.PGP_BasePacket._copyStream(src, f)
        src.close()
        msg = openpgpfile.PGP_Message(f, start=0)
        self.failUnlessEqual(len(list(msg.iterMainKeys())), 3)


class OpenPGPTrustTest(BaseTestHelper):
    class MockKeySignature(openpgpkey.OpenPGPKeySignature):
        """A key signature on a key"""
        def __init__(self, **kwargs):
            kwargs['pubKeyAlg'] = 0
            kwargs['hashAlg'] = 0
            kwargs['signature'] = ''
            openpgpkey.OpenPGPKeySignature.__init__(self, **kwargs)
            # Set it to False to simulate a signature that does not verify
            self._verifies = kwargs.pop('verifies', True)

    class MockKey:
        """A key"""
        def __init__(self, **kwargs):
            self.id = kwargs.pop('id')
            self.creation = kwargs.pop('creation')
            self.expiration = kwargs.pop('expiration', None)
            self.revocation = kwargs.pop('revocation', None)
            self.signatures = kwargs.pop('signatures', [])

        def getKeyId(self):
            return self.id

        def __hash__(self):
            return self.id.__hash__()

    class KeyCache(openpgpkey.OpenPGPKeyCache):
        def addKey(self, key):
            self.publicDict[key.id] = key

        def getPublicKey(self, keyId):
            if keyId in self.publicDict:
                return self.publicDict[keyId]
            raise KeyNotFound(keyId)

    def __init__(self, *args, **kwargs):
        BaseTestHelper.__init__(self, *args, **kwargs)
        self.creation = time.time()
        self.exp90d = 3600 * 24 * 90
        self.exp1y = 3600 * 24 * 365
        self.exp3y = self.exp1y * 3

    def testOldTrust(self):
        MK = self.MockKey
        MKSig = self.MockKeySignature
        kc = self.KeyCache()

        corpKey = MK(id = 'CORPKEY', creation = self.creation,
                     expiration = self.exp3y)
        kc.addKey(corpKey)
        sig = MKSig(sigId = 'CORP-KEY-SIG',
                    signer = corpKey.id, creation = self.creation,
                    expiration = self.exp1y)
        dsks = []
        for i in range(5):
            dsk = MK(id = 'DSK%s' % i,
                     creation = self.creation, expiration = self.exp1y,
                     signatures = [ sig ])
            dsks.append(dsk)
            kc.addKey(dsk)
        binsigs = [ MKSig(sigId = 'DSK-KEY-SIG',
                    signer = dsks[i].id, creation = self.creation,
                    expiration = self.exp90d) for i in range(0, 5, 2) ]
        binKey = MK(id = 'BINKEY1',
                    creation = self.creation, expiration = self.exp90d,
                    signatures = binsigs)
        kc.addKey(binKey)

        toplevel = [ 'CORPKEY', 'aaa' ]
        t = openpgpkey.Trust(toplevel)
        trust, depth = t.computeTrust(binKey.id, keyRetrievalCallback=kc.getPublicKey)
        self.failUnlessEqual(trust[binKey.id], (8, 40, 120))
        self.failUnlessEqual(trust[dsks[0].id], (9, 40, 120))
        self.failUnlessEqual(trust[dsks[2].id], (9, 40, 120))
        self.failUnlessEqual(trust[dsks[4].id], (9, 40, 120))
        self.failUnlessEqual(trust[corpKey.id], (10, 120, 120))

        # Not enough sigs on the bin key
        del binsigs[-1]
        trust, depth = t.computeTrust(binKey.id, keyRetrievalCallback=kc.getPublicKey)
        self.failUnlessEqual(trust[binKey.id], (8, 40, 80))

    def testNewTrust(self):
        MK = self.MockKey
        MKSig = self.MockKeySignature
        kc = self.KeyCache()

        corpKey = MK(id = 'CORPKEY', creation = self.creation,
                     expiration = self.exp3y)
        kc.addKey(corpKey)
        sig = MKSig(sigId = 'CORP-KEY-SIG',
                    signer = corpKey.id, creation = self.creation,
                    expiration = self.exp1y,
                    trustLevel = 1, trustAmount = 30)
        dsks = []
        for i in range(7):
            dsk = MK(id = 'DSK%s' % i,
                     creation = self.creation, expiration = self.exp1y,
                     signatures = [ sig ])
            dsks.append(dsk)
            kc.addKey(dsk)
        binsigs = [ MKSig(sigId = 'DSK-KEY-SIG',
                    signer = dsks[i].id, creation = self.creation,
                    expiration = self.exp90d, trustLevel = 0, trustAmount =
                    0) for i in range(0, 7, 2) ]
        binKey = MK(id = 'BINKEY1',
                    creation = self.creation, expiration = self.exp90d,
                    signatures = binsigs)
        kc.addKey(binKey)

        sig2 = MKSig(sigId = 'DSK-KEY-SIG-1',
                     signer = binKey.id, creation = self.creation,
                     expiration = self.exp90d)
        binKey2 = MK(id = 'BINKEY2',
                    creation = self.creation, expiration = self.exp90d,
                    signatures = [ sig2 ])
        kc.addKey(binKey2)

        toplevel = [ 'CORPKEY', 'aaa' ]
        t = openpgpkey.Trust(toplevel)
        trust, depth = t.computeTrust(binKey.id, keyRetrievalCallback=kc.getPublicKey)

        self.failUnlessEqual(trust[binKey.id], (0, 0, 120))
        self.failUnlessEqual(trust[dsks[0].id], (1, 30, 120))
        self.failUnlessEqual(trust[dsks[2].id], (1, 30, 120))
        self.failUnlessEqual(trust[dsks[4].id], (1, 30, 120))
        self.failUnlessEqual(trust[dsks[6].id], (1, 30, 120))
        self.failUnlessEqual(trust[corpKey.id], (10, 120, 120))
        self.failUnlessEqual(depth[binKey.id], 2)

        # Not enough sigs on the bin key
        oldBinSigs = binsigs[:]
        del binsigs[-1]
        trust, depth = t.computeTrust(binKey.id, keyRetrievalCallback=kc.getPublicKey)
        self.failUnlessEqual(trust[binKey.id], (0, 0, 90))

        # Restore sigs
        binsigs[:] = oldBinSigs
        trust, depth = t.computeTrust(binKey2.id, keyRetrievalCallback=kc.getPublicKey)
        self.failUnlessEqual(trust[binKey.id], (0, 0, 120))
        self.failIf(binKey2.id in trust)

    def testMultiLevel(self):
        MK = self.MockKey
        MKSig = self.MockKeySignature
        kc = self.KeyCache()

        corpKey = MK(id = 'CORPKEY', creation = self.creation,
                     expiration = self.exp3y)
        kc.addKey(corpKey)
        sig = MKSig(sigId = 'CORP-KEY-SIG',
                    signer = corpKey.id, creation = self.creation,
                    expiration = self.exp1y,
                    trustLevel = 2, trustAmount = 40)
        dsks0 = []
        for i in range(6):
            dsk = MK(id = 'DSK0%s' % i,
                     creation = self.creation, expiration = self.exp1y,
                     signatures = [ sig ])
            dsks0.append(dsk)
            kc.addKey(dsk)
        tsigs1 = [ MKSig(sigId = 'DSK-KEY-SIG',
                    signer = dsks0[i].id, creation = self.creation,
                    expiration = self.exp90d,
                    trustLevel = 1, trustAmount = 30) for i in range(0, 6, 2) ]
        tsigs2 = [ MKSig(sigId = 'DSK-KEY-SIG-1',
                    signer = dsks0[i].id, creation = self.creation,
                    expiration = self.exp90d,
                    trustLevel = 1, trustAmount = 30) for i in range(1, 6, 2) ]
        tsigs = [ tsigs1, tsigs2 ]

        dsks1 = []
        for i in range(4):
            dsk = MK(id = 'DSK1%s' % i,
                     creation = self.creation, expiration = self.exp1y,
                     signatures = tsigs[i % 2])
            dsks1.append(dsk)
            kc.addKey(dsk)

        binsigs = [ MKSig(sigId = 'BIN-KEY-SIG',
                    signer = x.id, creation = self.creation,
                    expiration = self.exp90d,
                    trustLevel = 0, trustAmount = 0) for x in dsks1 ]

        binKey = MK(id = 'BINKEY1',
                    creation = self.creation, expiration = self.exp90d,
                    signatures = binsigs)
        kc.addKey(binKey)

        toplevel = [ 'CORPKEY', 'aaa' ]
        t = openpgpkey.Trust(toplevel)
        trust, depth = t.computeTrust(binKey.id, keyRetrievalCallback=kc.getPublicKey)

        self.failUnlessEqual(trust[binKey.id], (0, 0, 120))
        for dsk in dsks1:
            self.failUnlessEqual(trust[dsk.id], (1, 30, 120))
            self.failUnlessEqual(depth[dsk.id], 2)
        for dsk in dsks0:
            self.failUnlessEqual(trust[dsk.id], (2, 40, 120))
            self.failUnlessEqual(depth[dsk.id], 1)
        self.failUnlessEqual(trust[corpKey.id], (10, 120, 120))
        self.failUnlessEqual(depth[binKey.id], 3)

        # Some of the level 2 sigs are not complete
        dsks1[-1].signatures = dsks1[-1].signatures[:-1]

        trust, depth = t.computeTrust(binKey.id, keyRetrievalCallback=kc.getPublicKey)
        self.failUnlessEqual(trust[binKey.id], (0, 0, 90))

    def testRealKeyTrust(self):
        kc = openpgpkey.OpenPGPKeyFileCache()
        kc.setPublicPath(self.getKeyring('pubringrev.gpg'))

        toplevel = ['60800B90']

        t = openpgpkey.Trust(toplevel)
        trust, depth = t.computeTrust('8FF88B92', keyRetrievalCallback=kc.getPublicKey)
        self.failUnlessEqual(t.getTrust('7D9D292960800B90'), (10, 120, 120))
        self.failUnlessEqual(t.getTrust('9A7E8B99E28A4D73'), (2, 120, 120))
        self.failUnlessEqual(t.getTrust('41EA567A8FF88B92'), (1, 60, 120))
        self.failUnlessEqual(t.getTrust('8FF88B92'), (1, 60, 120))

        self.failUnlessEqual(t.getDepth('8FF88B92'), 2)
        self.failUnlessEqual(t.getDepth('41EA567A8FF88B92'), 2)
        self.failUnlessEqual(t.getDepth('9A7E8B99E28A4D73'), 1)
        self.failUnlessEqual(t.getDepth('7D9D292960800B90'), 0)

    def testTrust1(self):
        self.cfg.configLine('trustedKeys 2E82A3FF')
        self.cfg.configLine('trustThreshold 1')

        pubring = os.path.join(self.workDir, 'pubring')
        cb = openpgpkey.KeyringCacheCallback(self.getKeyring('trust1.gpg'),
            cfg=self.cfg)
        kc = openpgpkey.OpenPGPKeyFileCache(cb)
        kc.setPublicPath(pubring)
        # BSK signed by 3 DSKs, each with a trust amount of 40
        key = kc.getPublicKey('359DE6E5')
        self.failUnlessEqual(key.getTrustLevel(), 120)
        # BSK signed by 2 DSKs, each with a trust amount of 40. Not good
        # enough
        key = kc.getPublicKey('207EFBC1')
        self.failUnlessEqual(key.getTrustLevel(), 80)

        # BSK signed by 3 DSKs, old-style (no trust sigs)
        key = kc.getPublicKey('7011C585')
        self.failUnlessEqual(key.getTrustLevel(), 120)

        # BSK signed by 2 DSKs, old-style (no trust sigs). Not good enough
        key = kc.getPublicKey('74A01C9D')
        self.failUnlessEqual(key.getTrustLevel(), 80)

    def testTrust2(self):
        # Some non-existing top-level key. Nothing is trusted
        self.cfg.configLine('trustedKeys AABBCCDD')
        self.cfg.configLine('trustThreshold 1')

        pubring = os.path.join(self.workDir, 'pubring')
        cb = openpgpkey.KeyringCacheCallback(self.getKeyring('trust1.gpg'),
            cfg=self.cfg)
        kc = openpgpkey.OpenPGPKeyFileCache(cb)
        kc.setPublicPath(pubring)

        # BSK signed by 3 DSKs, each with a trust amount of 40
        key = kc.getPublicKey('359DE6E5')
        self.failUnlessEqual(key.getTrustLevel(), 0)

        # BSK signed by 3 DSKs, old-style (no trust sigs)
        key = kc.getPublicKey('7011C585')
        self.failUnlessEqual(key.getTrustLevel(), 0)

    def testTrust3(self):
        # Some non-existing top-level key, but with a 0 trust threshold.
        # Everything is trusted
        self.cfg.configLine('trustedKeys AABBCCDD')
        self.cfg.configLine('trustThreshold 0')

        pubring = os.path.join(self.workDir, 'pubring')
        cb = openpgpkey.KeyringCacheCallback(self.getKeyring('trust1.gpg'),
            cfg=self.cfg)
        kc = openpgpkey.OpenPGPKeyFileCache(cb)
        kc.setPublicPath(pubring)

        # BSK signed by 2 DSKs, each with a trust amount of 40. Not good
        # enough normally
        key = kc.getPublicKey('207EFBC1')
        self.failUnlessEqual(key.getTrustLevel(), 120)

        # BSK signed by 2 DSKs, old-style (no trust sigs). Not good enough
        # normally
        key = kc.getPublicKey('74A01C9D')
        self.failUnlessEqual(key.getTrustLevel(), 120)

    def testKeyTimestampPacket(self):
        msg = openpgpfile.PGP_Message(self.getKeyring('pubringrev.gpg'))
        for pkt in msg.iterPackets():
            if isinstance(pkt, openpgpfile.PGP_Trust):
                pass
        body = util.ExtendedStringIO()
        body.write('\x01')
        body.write('\x0a\x1b\x2c\x3d\x4e\x5f\x98\x76')
        tm = 1196173781
        body.write(openpgpfile.binSeqToString(openpgpfile.int4ToBytes(tm)))
        body.write("\0" * 25)
        body.seek(0)
        pkt = openpgpfile.KeyTimestampPacket(body)
        sio = util.ExtendedStringIO()
        pkt.write(sio)
        self.failUnlessEqual(len(sio.getvalue()), 40)
        pkt.parse()
        pkt.parse()
        self.failUnlessEqual(pkt.getKeyId(), '0A1B2C3D4E5F9876');
        self.failUnlessEqual(pkt.getRefreshTimestamp(), tm)

        body.seek(0)
        pkt.rewriteBody()
        self.failUnlessEqual(pkt.readBody(), body.read())

        pkt2 = openpgpfile.KeyTimestampPacket(body)
        pkt2.setKeyId(pkt.getKeyId())
        pkt2.setRefreshTimestamp(tm + 1)
        pkt2.rewriteBody()

        trustdbPath = os.path.join(self.workDir, "trustdb")
        # Create the trust db first
        trustdb = util.ExtendedFile(trustdbPath, "w+", buffering = False)
        openpgpfile.addKeyTimestampPackets([pkt, pkt2], trustdbPath)
        msg = openpgpfile.PGP_Message(trustdbPath)
        pkts = [ x for x in msg.iterPackets() ]
        self.failUnlessEqual(len(pkts), 1)
        self.failUnlessEqual(pkts[0].readBody(), pkt2.readBody())

        pkt3 = openpgpfile.KeyTimestampPacket(body)
        pkt3.setKeyId(pkt.getKeyId()[:-1] + 'A')
        pkt3.setRefreshTimestamp(tm + 2)
        pkt3.rewriteBody()

        openpgpfile.addKeyTimestampPackets([pkt, pkt2, pkt3], trustdbPath)
        msg = openpgpfile.PGP_Message(trustdbPath)
        pkts = [ x for x in msg.iterPackets() ]
        self.failUnlessEqual(len(pkts), 2)
        self.failUnlessEqual(pkts[0].readBody(), pkt2.readBody())
        self.failUnlessEqual(pkts[1].readBody(), pkt3.readBody())

    def testPublicKeyringUpdateTimestamps(self):
        pubringPath = os.path.join(self.workDir,
                                   'some-dir/some-otherdir/some-pubring')
        tsDbPath = os.path.join(self.workDir,
                                'some-dir/some-otherdir/some-ts-db')
        pubKR = openpgpfile.PublicKeyring(pubringPath, tsDbPath)

        keyIds = [('0A1B2C3D4E5F9876', 10), ('0A1B2C3D4E5F9871', 20)]
        for keyId, ts in keyIds:
            pubKR.updateTimestamps([keyId], timestamp = ts)

        for keyId, ts in keyIds:
            self.failUnlessEqual(pubKR.getKeyTimestamp(keyId), ts)

        # Make the mtime change observable
        pubKR._timeIncrement = 100000
        mtime0 = os.stat(tsDbPath)[openpgpfile.stat.ST_MTIME]

        # Update to the latest timestamp
        keyId, ts = keyIds[1]
        pubKR.updateTimestamps([keyId])

        mtime1 = os.stat(tsDbPath)[openpgpfile.stat.ST_MTIME]
        self.failIf(pubKR.getKeyTimestamp(keyId) == ts)

        self.failUnless(mtime1 - mtime0 >= 100000)
        pubKR._timeIncrement = 1

    def testPublicKeyringAddKeys(self):
        pubringPath = os.path.join(self.workDir, 'some-pubring')
        tsDbPath = os.path.join(self.workDir, 'some-ts-db')

        pubKR = openpgpfile.PublicKeyring(pubringPath, tsDbPath)

        msg = openpgpfile.PGP_Message(self.getKeyring('pubringrev.gpg'))
        ts = 100
        keyFingerprints = pubKR.addKeys(msg.iterMainKeys(), timestamp=ts)
        expKeyFingerprints = [
            'BDE51CEF292C0864F00851B47D9D292960800B90',
            '92F85A5140AA660E8D7B7F309A7E8B99E28A4D73',
            '303458DA464FEA61FFABCA1919861D447A746BAE',
            '8C3A5D63993DC117DFCC212CC299BCAE9311D24B',
            'DCF7ADA5079570DFA7B5025541EA567A8FF88B92',
            'B62D990E6FA4E78CBFC6F657A588DBFB342E964C',
            'E7F956E11DD6E7054B6947A022E3AE1839D0A082',
            'A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5',
            '0A721ECA615E914C4424A27E2F61383F29BF4FCA',
        ]
        self.failUnlessEqual(keyFingerprints, expKeyFingerprints)
        for keyFpr in keyFingerprints:
            self.failUnlessEqual(pubKR.getKeyTimestamp(keyFpr), ts)

    def testPublicKeyringAddKeysAsStrings(self):
        # Add an armored and an unarmored key
        # The unarmored key is a secret key exported with gpg, but it behaves
        # exactly as if the whole main key was exported
        pubringPath = os.path.join(self.workDir, 'some-pubring')
        tsDbPath = os.path.join(self.workDir, 'some-ts-db')

        # This has a subkey too
        fp0 = '99F3EA61523474E5147938BA45AD480E0FD4B672'
        pk8 = open(self.getKeyring('pk8.gpg')).read()

        ts = 145

        fp1 = 'A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5'
        pubKR = openpgpfile.PublicKeyring(pubringPath, tsDbPath)
        ret = pubKR.addKeysAsStrings([ pubkeya4, pk8], timestamp = ts)
        self.failUnlessEqual(ret, [ fp1, fp0 ])

        # Can we fetch the key(s)?
        subKeyFp = '5C43B5D9658D2A81934088C75235F9765DC9BB15'
        subKeyId = '5235F9765DC9BB15'
        k0 = pubKR.getKey(fp0)
        self.failUnlessEqual(k0.getKeyFingerprint(), fp0)
        k1 = pubKR.getKey(subKeyId)
        self.failUnlessEqual(k1.getKeyFingerprint(), subKeyFp)
        self.failUnlessEqual(pubKR.getKeyTimestamp(subKeyId), ts)


class ServerInteractionTest(BaseTestHelper):
    def testAsciiKeyTransformation(self):
        # Grab one key
        fingerprint = "03B9CDDB42E9764275181784910E85FD7FA9DDBC"
        key = seekKeyById(fingerprint, self.getPublicFile())
        self.failUnless(key is not None)

        sio = util.ExtendedStringIO()
        key.write(sio)

        repos = self.openRepository()
        repos.addNewPGPKey(self.defLabel, 'test', sio.getvalue())

        ret = repos.getAsciiOpenPGPKey(self.defLabel, fingerprint)
        self.failUnlessEqual(ret[:36], '-----BEGIN PGP PUBLIC KEY BLOCK-----')

    def testKeyCacheCallback(self):
        self.openRepository()
        client = conaryclient.ConaryClient(self.cfg)
        client.repos.addNewPGPKey(self.defLabel, 'test', pubkey4)

        fingerprint = "A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5"

        kc = openpgpkey.getKeyCache()
        kc.setPublicPath(os.path.join(self.workDir, 'pubring'))

        self.failUnlessRaises(KeyNotFound, kc.callback.getPublicKey, 
                              'ADFADF', self.defLabel)

        self.failUnless(kc.callback.getPublicKey(fingerprint, self.defLabel))

    def testKeyCacheCallbackNonWritableKeyring(self):
        self.openRepository()
        client = conaryclient.ConaryClient(self.cfg)
        client.repos.addNewPGPKey(self.defLabel, 'test', pubkey4)

        fingerprint = "A47FB129D45AC2472DFA5D59A8E762BF91E3E6C5"

        kc = openpgpkey.getKeyCache()
        kc.setPublicPath('/some/path/that/cannot/be/created')

        # Should become an error when RMK-791 is fixed
        #self.failUnlessRaises(openpgpfile.KeyringError,
        #                      kc.getPublicKey, fingerprint,
        #                      self.defLabel)
        self.logFilter.add()
        kc.getPublicKey(fingerprint, self.defLabel)
        self.logFilter.remove()
        self.logFilter.compare("error: While caching PGP key: Permission denied: /some")

pubkey4 = (
    '\x99\x01\xa2\x04F\x92\xbd\x99\x11\x04\x00\xd5\xbf\x19Ru\x9b\xfd\x07\xc0'
    '\xa2tPC\xb1\x90\x8d\xd2\xc7\x8d\x90G\xdc\xfd\xfa\xff2\xaaTc\x7f\xd6\xaf'
    '\xdf\xf1\xb1N\\\x81\x922\x9b\xd8R\x0f_\xe8\xa0[\xf8\xc6\xf7\\\xba\x82'
    '\xdf}\xcc\x92\xa0\xcd\xfc\x11\x1d\xc94--m\xa0\x94:N\x19\xa7\x93OH\xf1'
    '\xc8\x1b*\xca\xb8\xad\xf3\x00i"\x86"\xde2K\x90Q\x9a\xaaL\xd76\x15\x85'
    '\xe9}\xdb\x80\xebi\xba\x00I\xc6\xf2\xb9\xdd\xf7\xb2V\xa49\x19\xe2\x97K'
    '\x87\x02\x1c\xfb\x00\xa0\xec%\xe4zgx\xb2[$\xb7\xb2>\xa3\xc0>>z\xe8\xc8U'
    '\x03\xffb \xfb\x9a\x90\\0\x8d\\\xb0>\n\x9f#\xf6\xc7_\xd7j+\xee-o\xd6\xd4'
    '\xc7\xdd+M`\xf8\xdf\xe6\x10^\xea\x85\xado\x83p\xf2\xc0Li0t\xeb\x1d-\xdex'
    '\xfa?\x01\xad\x93\xc1\x87c"\xb65?\xd7\xe6t\xcf]\xb6\x9d4D\n\x83\xedQu'
    '\x85\x10\xfb\xe0qv\xb7_\xd0\x12\x95\xf3\x06%\xa3a\x07\xb8\xaf\x9bL\xa4'
    '\xb1\xef\x84\xcf0;\x9b8\xeb4\xef\x9e\xf3\xc8\x99]\xf0\xe4\xd4\xdc\xf8'
    '\xc0?U\r\x84X4\x03\xfe\'tv[:\x9d\x0cx\xb3?1\xdeJiS\xdb\x1d\xb49;s6\x88'
    '\x0b\xfa\xe1$\xa5i\xb6\xb1\xb5\x94\xea,\xbc\x00w5\xefAX\x04)O\xe0\xbaR'
    '\xa1\t\x96g$\xd8\x1e\x08s~.\x04\x01\x92\x15\x9b)\xff\xa2\rzr\xb0\xa6k'
    '\xb14\t\x8bm\xcf\x83\x03\xcb\xb8TT\x85\xd3j9\x86\x8e\x94\xc9\xa9\xd5Pk'
    '\x8cWG@Eb\xaag\x05\x97\xaf\xf6W$T\xeb\xa5%\x93>\x95\xdf\x19\x8dV\xf8\x97'
    '\x98l\xe0\xc9\xb4&Key 32 (Key 32) <misa+key32@rpath.com>\x88`\x04\x13'
    '\x11\x02\x00 \x05\x02F\x92\xbd\x99\x02\x1b\x03\x06\x0b\t\x08\x07\x03\x02'
    '\x04\x15\x02\x08\x03\x04\x16\x02\x03\x01\x02\x1e\x01\x02\x17\x80\x00\n\t'
    '\x10\xa8\xe7b\xbf\x91\xe3\xe6\xc5\x7f\x08\x00\xa0\xcc\x99\xff\x08\x0b'
    '\x1eW\x01qC\xe3PD3\x0b5y~\x054\x00\x9fhc\xfc\xafo\x93\xcb\xc6dR\x13\xaf'
    '\x11VU1oD\xc3\xb1\x88J\x04\x10\x11\x02\x00\n\x05\x02F\x92\xbei\x03\x05'
    '\x01<\x00\n\t\x10\x19\x86\x1dDztk\xae\x80/\x00\xa0\x8f\xdb\x15\xdf1\x92'
    '\x02\x95\xea\xeb\xa1\x14\x81\'\xa3A\x86\x08\x89O\x00\x9fr\x91\xd21[\x04F'
    '\x1c\xe0\x86\xc2E\n\xe6\x891\x14\xe6\xbf\xdc\x88J\x04\x10\x11\x02\x00\n'
    '\x05\x02F\x92\xbe\xe8\x03\x05\x01<\x00\n\t\x10\xc2\x99\xbc\xae\x93\x11'
    '\xd2Km\x03\x00\x9d\x1f\xc5\x055\xd2\xbf6e&\xa0\xfc)\xd3\xef$X\xcd\xb3'
    '\xdfu\x00\xa0\xbe\xbb\x95h\x9c\xc4\x16\xa6G2-\x131\xcd\xa6\xfd:\xc4\x03K'
    '\x88J\x04\x10\x11\x02\x00\n\x05\x02F\x92\xbf\x14\x03\x05\x01<\x00\n\t'
    '\x10A\xeaVz\x8f\xf8\x8b\x92\xbb>\x00\xa0\x8eP\xb2^\x90\xb5)c%\x88\xc6:S'
    '\xa4\xcb\xbd\xe7\xfd\xf5\xba\x00\x9fF\x89;\x0bj\x07\xf1\x8fE\xe9\xbf>'
    '\xba\xd1\xecY\x1aZ\x121\xb9\x02\r\x04F\x92\xbd\x99\x10\x08\x00\xe2\xdd'
    '\xe0\xb7\xe3\x94\xd2\xaa2`\xec\x8d\xf3\xde\xdd)\xa4\xa2\x01\xca\x82,#'
    '\xfa\x0c\x8e\x0bav]\xc7I\xe5\xbf>Y\xc3\xd9\xd7\x85\xdd\x1aN~\xb9\x9b\xa7'
    '\x00o\x81\x81\xbcN\xc8TJ\x01\xc8}H\xbc`m\xd9\xe0\x07\x00\xea\xdc\xe5'
    '\xc0za\x00\x08\x149v\x0b\xac\x7f\xccm\xb0\xc6\x06\x1f\xae9L\xf5\x95?8'
    '\xde\x8f\xf9\x82\xe6TX\xce\x87Oe\x14X\x8dzP\x86d\xfe\xd50_^\xba\'vz'
    '\x7fJ\xbf\xd3\x9e0\x9b\x1c\x07\x88\x04]\xec\xfc\x08\x0b\xb210k<\xb6\xe2 '
    '\xd5\xfb\xc9rP+\r\xdc\x12\x9f\x0b/\xce\x16(>\xba[\x9e1(yZ\x0cib%\x7f,r'
    '\x0c\x178\xc28#\x84\xc1\x95\xe8^x{jEYx[p\xca\x1c\xe5\xde\x92\xfb=\x123'
    '\x7foRN\xd2\xec\xc0g\x82b\xd5\xc6H\x13H\x1f\x97O\x16g\xe4od\x1315\xa6V'
    '\xdeW\xfd\xab\xa66\xbe\x18\x12\x90\xa4\x89\xdbvB\xa4O\xf3b\xa0\x8f\xf6y#'
    '\xef\x00\x03\x05\x07\xffM\xa1#\xb0\xfbG\xb7\xde\x1e\x88~\xbf{\xda2\xf0'
    '\x19i\xc5<zg\x98\xbdD\xdd\x8c,LR\x9f\x9b\xc5{5\xb6?Oi\xb7\xb25\x84:\x8b^'
    '\x87r\xda\xbc\x0f\x14\xaeG\xb5\xa9r\xfc*\xcfP\xad\xb6=\x8d\xe4\xfb\x9d'
    '\xa2\xc9\x80\xf1ne\xec\xed\x92nCm0\xd4\xa3\xb6$\xaa\xad\x1d\xf2\x17'
    '\x0cyj\xfd\xc3H\xa9\xf6\x00\x84\xad\xe63\x0b`\xd2\xaa>\x18\x18\xb8_\x9e'
    '\xb6\x91\x1b[\x1c\xaa\xe6-x\'5\x94\x17)\xddU\xc0\x84\xf4\xb0n\xc2\x07'
    '\x89K\xa7<\xaf\xe1\x81\\\x1e\xeb\xe2t\xf8R\t\x88\x1b\xdfQ\xed!MB\xbf\x84'
    '\x14\rf\xe5\xbd\x85\xd9\xd0\xaf\xca\x99\xcc\xe5\x01\xf8\xd6\xd0\xd4\x97'
    '\x1c\xbd,\xb5X\xb7f\xb9P\x10\x04\xa2\xe1\x16\xb6\x04\x85_0\x02\xc0r\xd75'
    '\x94T\xc0\x97Q\x9f\x96M\xffo\xf8\xecI\xe7\x02\xeb\x89$B\x99*t\xac\xf7Co'
    '\xa7\xfe\xc0\xea\x02\x99\x96\x82RC\xba\xc9\xfb\x1b\x0bSf\x8b\x14\x85'
    '\x11\xafX\xee\xfd\xdc\x88I\x04\x18\x11\x02\x00\t\x05\x02F\x92\xbd\x99'
    '\x02\x1b\x0c\x00\n\t\x10\xa8\xe7b\xbf\x91\xe3\xe6\xc5c\xb5\x00\x99\x01a'
    '\xc8CV\xc7s\xe1\xaf*v,3\x15\xceC\x03\xf4\xf8\xde\x00\xa0\xccP\xef\xd3N'
    '\xf6\x80N\xf7@,}!\xe7\xab,y[\xc7\x1a'
)

seckey4 = (
    '\x95\x01\xe1\x04F\x92\xbd\x99\x11\x04\x00\xd5\xbf\x19Ru\x9b\xfd\x07'
    '\xc0\xa2tPC\xb1\x90\x8d\xd2\xc7\x8d\x90G\xdc\xfd\xfa\xff2\xaaTc\x7f'
    '\xd6\xaf\xdf\xf1\xb1N\\\x81\x922\x9b\xd8R\x0f_\xe8\xa0[\xf8\xc6\xf7\\'
    '\xba\x82\xdf}\xcc\x92\xa0\xcd\xfc\x11\x1d\xc94--m\xa0\x94:N\x19\xa7'
    '\x93OH\xf1\xc8\x1b*\xca\xb8\xad\xf3\x00i"\x86"\xde2K\x90Q\x9a\xaaL'
    '\xd76\x15\x85\xe9}\xdb\x80\xebi\xba\x00I\xc6\xf2\xb9\xdd\xf7\xb2V\xa49'
    '\x19\xe2\x97K\x87\x02\x1c\xfb\x00\xa0\xec%\xe4zgx\xb2[$\xb7\xb2>\xa3'
    '\xc0>>z\xe8\xc8U\x03\xffb \xfb\x9a\x90\\0\x8d\\\xb0>\n\x9f#\xf6\xc7_'
    '\xd7j+\xee-o\xd6\xd4\xc7\xdd+M`\xf8\xdf\xe6\x10^\xea\x85\xado\x83p\xf2'
    '\xc0Li0t\xeb\x1d-\xdex\xfa?\x01\xad\x93\xc1\x87c"\xb65?\xd7\xe6t\xcf]'
    '\xb6\x9d4D\n\x83\xedQu\x85\x10\xfb\xe0qv\xb7_\xd0\x12\x95\xf3\x06%'
    '\xa3a\x07\xb8\xaf\x9bL\xa4\xb1\xef\x84\xcf0;\x9b8\xeb4\xef\x9e\xf3\xc8'
    '\x99]\xf0\xe4\xd4\xdc\xf8\xc0?U\r\x84X4\x03\xfe\'tv[:\x9d\x0cx\xb3?1'
    '\xdeJiS\xdb\x1d\xb49;s6\x88\x0b\xfa\xe1$\xa5i\xb6\xb1\xb5\x94\xea,\xbc'
    '\x00w5\xefAX\x04)O\xe0\xbaR\xa1\t\x96g$\xd8\x1e\x08s~.\x04\x01\x92\x15'
    '\x9b)\xff\xa2\rzr\xb0\xa6k\xb14\t\x8bm\xcf\x83\x03\xcb\xb8TT\x85\xd3j9'
    '\x86\x8e\x94\xc9\xa9\xd5Pk\x8cWG@Eb\xaag\x05\x97\xaf\xf6W$T\xeb\xa5%'
    '\x93>\x95\xdf\x19\x8dV\xf8\x97\x98l\xe0\xc9\xfe\x03\x03\x02)\xfa\xa2D'
    '\xb9C|\x0e`l\xe8\xd2\x9b\x17,\xda\x16i\xffSn\xb0o\xfd)\xc3n\x1b\xe4#'
    '\x07d\xf8\xa1\xa4\x8c-D\x9f\x11\x0cC\xf0\x07\x85\x8b\x97\x0bU 1YD\x99o'
    '\xa3\x1dn\x87\xb4&Key 32 (Key 32) <misa+key32@rpath.com>\x88`\x04\x13'
    '\x11\x02\x00 \x05\x02F\x92\xbd\x99\x02\x1b\x03\x06\x0b\t\x08\x07\x03'
    '\x02\x04\x15\x02\x08\x03\x04\x16\x02\x03\x01\x02\x1e\x01\x02\x17\x80'
    '\x00\n\t\x10\xa8\xe7b\xbf\x91\xe3\xe6\xc5\x7f\x08\x00\xa0\xcc\x99\xff'
    '\x08\x0b\x1eW\x01qC\xe3PD3\x0b5y~\x054\x00\x9fhc\xfc\xafo\x93\xcb'
    '\xc6dR\x13\xaf\x11VU1oD\xc3\xb1\x9d\x02c\x04F\x92\xbd\x99\x10\x08\x00'
    '\xe2\xdd\xe0\xb7\xe3\x94\xd2\xaa2`\xec\x8d\xf3\xde\xdd)\xa4\xa2\x01'
    '\xca\x82,#\xfa\x0c\x8e\x0bav]\xc7I\xe5\xbf>Y\xc3\xd9\xd7\x85\xdd\x1aN~'
    '\xb9\x9b\xa7\x00o\x81\x81\xbcN\xc8TJ\x01\xc8}H\xbc`m\xd9\xe0\x07\x00'
    '\xea\xdc\xe5\xc0za\x00\x08\x149v\x0b\xac\x7f\xccm\xb0\xc6\x06\x1f'
    '\xae9L\xf5\x95?8\xde\x8f\xf9\x82\xe6TX\xce\x87Oe\x14X\x8dzP\x86d\xfe'
    '\xd50_^\xba\'vz\x7fJ\xbf\xd3\x9e0\x9b\x1c\x07\x88\x04]\xec\xfc\x08\x0b'
    '\xb210k<\xb6\xe2 \xd5\xfb\xc9rP+\r\xdc\x12\x9f\x0b/\xce\x16(>\xba['
    '\x9e1(yZ\x0cib%\x7f,r\x0c\x178\xc28#\x84\xc1\x95\xe8^x{jEYx[p\xca\x1c'
    '\xe5\xde\x92\xfb=\x123\x7foRN\xd2\xec\xc0g\x82b\xd5\xc6H\x13H\x1f\x97O'
    '\x16g\xe4od\x1315\xa6V\xdeW\xfd\xab\xa66\xbe\x18\x12\x90\xa4\x89\xdbvB'
    '\xa4O\xf3b\xa0\x8f\xf6y#\xef\x00\x03\x05\x07\xffM\xa1#\xb0\xfbG\xb7'
    '\xde\x1e\x88~\xbf{\xda2\xf0\x19i\xc5<zg\x98\xbdD\xdd\x8c,LR\x9f\x9b'
    '\xc5{5\xb6?Oi\xb7\xb25\x84:\x8b^\x87r\xda\xbc\x0f\x14\xaeG\xb5\xa9r'
    '\xfc*\xcfP\xad\xb6=\x8d\xe4\xfb\x9d\xa2\xc9\x80\xf1ne\xec\xed\x92nCm0'
    '\xd4\xa3\xb6$\xaa\xad\x1d\xf2\x17\x0cyj\xfd\xc3H\xa9\xf6\x00\x84\xad'
    '\xe63\x0b`\xd2\xaa>\x18\x18\xb8_\x9e\xb6\x91\x1b[\x1c\xaa\xe6-x\'5\x94'
    '\x17)\xddU\xc0\x84\xf4\xb0n\xc2\x07\x89K\xa7<\xaf\xe1\x81\\\x1e\xeb'
    '\xe2t\xf8R\t\x88\x1b\xdfQ\xed!MB\xbf\x84\x14\rf\xe5\xbd\x85\xd9\xd0'
    '\xaf\xca\x99\xcc\xe5\x01\xf8\xd6\xd0\xd4\x97\x1c\xbd,\xb5X\xb7f\xb9P'
    '\x10\x04\xa2\xe1\x16\xb6\x04\x85_0\x02\xc0r\xd75\x94T\xc0\x97Q\x9f'
    '\x96M\xffo\xf8\xecI\xe7\x02\xeb\x89$B\x99*t\xac\xf7Co\xa7\xfe\xc0\xea'
    '\x02\x99\x96\x82RC\xba\xc9\xfb\x1b\x0bSf\x8b\x14\x85\x11\xafX\xee\xfd'
    '\xdc\xfe\x03\x03\x02)\xfa\xa2D\xb9C|\x0e`\xf8\xb3\x89g\xda\xa9;\xf3'
    '\xcf\x89\xb3)H\xfe\xa2\xf0 \x1cJ\xeb9j\x856TwcG\x06Uy\xaeq\xaa\x10;'
    '\xcb\xa7\xee@k\xb3\xaf\x81\xc4_c\xdd\n\x11\x18\xea\xf8\x156}\xe4\xfc'
    '\xe7\xe6\xd9\xf5kQ@H+A\x96\xdd\x86G\xb9\x88I\x04\x18\x11\x02\x00\t\x05'
    '\x02F\x92\xbd\x99\x02\x1b\x0c\x00\n\t\x10\xa8\xe7b\xbf\x91\xe3\xe6'
    '\xc5c\xb5\x00\xa0\xe7\xf3\x93\xb7 \xaf\x05\x04C\xf2\xbd\xf1\xb0TM\x96'
    '\x94D\x0b\xec\x00\xa0\xb5\x94\xb6\xb1\xf4\xcf\xc8\x1b\xfe\xa2\x9b'
    '\xa2qR\x02\x05\xd6{\xc4\xb8'
)

pubkeya4 = """\
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.7 (GNU/Linux)

mQGiBEaSvZkRBADVvxlSdZv9B8CidFBDsZCN0seNkEfc/fr/MqpUY3/Wr9/xsU5c
gZIym9hSD1/ooFv4xvdcuoLffcySoM38ER3JNC0tbaCUOk4Zp5NPSPHIGyrKuK3z
AGkihiLeMkuQUZqqTNc2FYXpfduA62m6AEnG8rnd97JWpDkZ4pdLhwIc+wCg7CXk
emd4slskt7I+o8A+PnroyFUD/2Ig+5qQXDCNXLA+Cp8j9sdf12or7i1v1tTH3StN
YPjf5hBe6oWtb4Nw8sBMaTB06x0t3nj6PwGtk8GHYyK2NT/X5nTPXbadNEQKg+1R
dYUQ++Bxdrdf0BKV8wYlo2EHuK+bTKSx74TPMDubOOs0757zyJld8OTU3PjAP1UN
hFg0A/4ndHZbOp0MeLM/Md5KaVPbHbQ5O3M2iAv64SSlabaxtZTqLLwAdzXvQVgE
KU/gulKhCZZnJNgeCHN+LgQBkhWbKf+iDXpysKZrsTQJi23PgwPLuFRUhdNqOYaO
lMmp1VBrjFdHQEViqmcFl6/2VyRU66Ulkz6V3xmNVviXmGzgybQmS2V5IDMyIChL
ZXkgMzIpIDxtaXNhK2tleTMyQHJwYXRoLmNvbT6IYAQTEQIAIAUCRpK9mQIbAwYL
CQgHAwIEFQIIAwQWAgMBAh4BAheAAAoJEKjnYr+R4+bFfwgAoMyZ/wgLHlcBcUPj
UEQzCzV5fgU0AJ9oY/yvb5PLxmRSE68RVlUxb0TDsYhKBBARAgAKBQJGkr5pAwUB
PAAKCRAZhh1EenRrroAvAKCP2xXfMZIClerroRSBJ6NBhgiJTwCfcpHSMVsERhzg
hsJFCuaJMRTmv9yISgQQEQIACgUCRpK+6AMFATwACgkQwpm8rpMR0kttAwCdH8UF
NdK/NmUmoPwp0+8kWM2z33UAoL67lWicxBamRzItEzHNpv06xANLiEoEEBECAAoF
AkaSvxQDBQE8AAoJEEHqVnqP+IuSuz4AoI5Qsl6QtSljJYjGOlOky73n/fW6AJ9G
iTsLagfxj0Xpvz660exZGloSMbkCDQRGkr2ZEAgA4t3gt+OU0qoyYOyN897dKaSi
AcqCLCP6DI4LYXZdx0nlvz5Zw9nXhd0aTn65m6cAb4GBvE7IVEoByH1IvGBt2eAH
AOrc5cB6YQAIFDl2C6x/zG2wxgYfrjlM9ZU/ON6P+YLmVFjOh09lFFiNelCGZP7V
MF9euid2en9Kv9OeMJscB4gEXez8CAuyMTBrPLbiINX7yXJQKw3cEp8LL84WKD66
W54xKHlaDGliJX8scgwXOMI4I4TBleheeHtqRVl4W3DKHOXekvs9EjN/b1JO0uzA
Z4Ji1cZIE0gfl08WZ+RvZBMxNaZW3lf9q6Y2vhgSkKSJ23ZCpE/zYqCP9nkj7wAD
BQf/TaEjsPtHt94eiH6/e9oy8BlpxTx6Z5i9RN2MLExSn5vFezW2P09pt7I1hDqL
Xody2rwPFK5Htaly/CrPUK22PY3k+52iyYDxbmXs7ZJuQ20w1KO2JKqtHfIXDHlq
/cNIqfYAhK3mMwtg0qo+GBi4X562kRtbHKrmLXgnNZQXKd1VwIT0sG7CB4lLpzyv
4YFcHuvidPhSCYgb31HtIU1Cv4QUDWblvYXZ0K/KmczlAfjW0NSXHL0stVi3ZrlQ
EASi4Ra2BIVfMALActc1lFTAl1Gflk3/b/jsSecC64kkQpkqdKz3Q2+n/sDqApmW
glJDusn7GwtTZosUhRGvWO793IhJBBgRAgAJBQJGkr2ZAhsMAAoJEKjnYr+R4+bF
Y7UAmQFhyENWx3Phryp2LDMVzkMD9PjeAKDMUO/TTvaATvdALH0h56sseVvHGg==
=MgNw
-----END PGP PUBLIC KEY BLOCK-----
"""
