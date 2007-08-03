# Copyright (c) 2005-2006 rPath, Inc.
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

import base64
import os
import sha
import md5
import struct

try:
    from Crypto.Hash import RIPEMD
except ImportError:
    RIPEMD = 'RIPEMD'
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from Crypto.Cipher import AES
from Crypto.Cipher import DES3
from Crypto.Cipher import Blowfish
from Crypto.Cipher import CAST
from Crypto.PublicKey import RSA
from Crypto.PublicKey import DSA

from conary.lib import util

# key types defined in RFC 2440 page 49
PK_ALGO_RSA                  = 1
PK_ALGO_RSA_ENCRYPT_ONLY     = 2  # deprecated
PK_ALGO_RSA_SIGN_ONLY        = 3  # deprecated
PK_ALGO_ELGAMAL_ENCRYPT_ONLY = 16
PK_ALGO_DSA                  = 17
PK_ALGO_ELLIPTIC_CURVE       = 18
PK_ALGO_ECDSA                = 19
PK_ALGO_ELGAMAL              = 20

PK_ALGO_ALL_RSA = (PK_ALGO_RSA, PK_ALGO_RSA_ENCRYPT_ONLY,
                   PK_ALGO_RSA_SIGN_ONLY)
PK_ALGO_ALL_ELGAMAL = (PK_ALGO_ELGAMAL_ENCRYPT_ONLY, PK_ALGO_ELGAMAL)

# packet tags are defined in RFC 2440 - 4.3. Packet Tags
PKT_RESERVED           = 0  # a packet type must not have this value
PKT_PUB_SESSION_KEY    = 1  # Public-Key Encrypted Session Key Packet
PKT_SIG                = 2  # Signature Packet
PKT_SYM_SESSION_KEY    = 3  # Symmetric-Key Encrypted Session Key Packet
PKT_ONE_PASS_SIG       = 4  # One-Pass Signature Packet
PKT_SECRET_KEY         = 5  # Secret Key Packet
PKT_PUBLIC_KEY         = 6  # Public Key Packet
PKT_SECRET_SUBKEY      = 7  # Secret Subkey Packet
PKT_COMPRESSED_DATA    = 8  # Compressed Data Packet
PKT_SYM_ENCRYPTED_DATA = 9  # Symmetrically Encrypted Data Packet
PKT_MARKER             = 10 # Marker Packet
PKT_LITERAL_DATA       = 11 # Literal Data Packet
PKT_TRUST              = 12 # Trust Packet
PKT_USERID             = 13 # User ID Packet
PKT_PUBLIC_SUBKEY      = 14 # Public Subkey Packet
# Additions from http://tools.ietf.org/html/draft-ietf-openpgp-rfc2440bis-17
PKT_USER_ATTRIBUTE     = 17 # User Attribute Packet
PKT_DATA_PACKET        = 18 # Sym. Encrypted and Integrity Protected Data Packet
PKT_MOD_DETECTION      = 19 # Modification Detection Code Packet
PKT_PRIVATE1           = 60 # 60 to 63 -- Private or Experimental Values
PKT_PRIVATE2           = 61
PKT_PRIVATE3           = 62
PKT_PRIVATE4           = 63

PKT_ALL_SECRET = (PKT_SECRET_KEY, PKT_SECRET_SUBKEY)
PKT_ALL_PUBLIC = (PKT_PUBLIC_KEY, PKT_PUBLIC_SUBKEY)
PKT_ALL_KEYS = PKT_ALL_SECRET + PKT_ALL_PUBLIC
PKT_MAIN_KEYS = (PKT_SECRET_KEY, PKT_PUBLIC_KEY)
PKT_SUB_KEYS = (PKT_SECRET_SUBKEY, PKT_PUBLIC_SUBKEY)

# 5.2.1 Signature Types
SIG_TYPE_BINARY_DOC    = 0x00
SIG_TYPE_TEXT_DOC      = 0x01
SIG_TYPE_STANDALONE    = 0x02
SIG_TYPE_CERT_0        = 0x10
SIG_TYPE_CERT_1        = 0x11
SIG_TYPE_CERT_2        = 0x12
SIG_TYPE_CERT_3        = 0x13
SIG_TYPE_SUBKEY_BIND   = 0x18
SIG_TYPE_DIRECT_KEY    = 0x1F
SIG_TYPE_KEY_REVOC     = 0x20
SIG_TYPE_SUBKEY_REVOC  = 0x28
SIG_TYPE_CERT_REVOC    = 0x30
SIG_TYPE_TIMESTAMP     = 0x40

SIG_CERTS = (SIG_TYPE_CERT_0, SIG_TYPE_CERT_1,
             SIG_TYPE_CERT_2, SIG_TYPE_CERT_3, )
SIG_KEY_REVOCS = (SIG_TYPE_KEY_REVOC, SIG_TYPE_SUBKEY_REVOC)

# 5.2.3.1 Signature Subpacket Types
SIG_SUBPKT_CREATION       = 2
SIG_SUBPKT_SIG_EXPIRE     = 3
SIG_SUBPKT_EXPORTABLE     = 4
SIG_SUBPKT_TRUST          = 5
SIG_SUBPKT_REGEX          = 6
SIG_SUBPKT_REVOCABLE      = 7
SIG_SUBPKT_KEY_EXPIRE     = 9
SIG_SUBPKT_PLACEHOLDER    = 10
SIG_SUBPKT_PREF_SYM_ALGS  = 11
SIG_SUBPKT_REVOC_KEY      = 12
SIG_SUBPKT_ISSUER_KEYID   = 16
SIG_SUBPKT_NOTATION_DATA  = 20
SIG_SUBPKT_PREF_HASH_ALGS = 21
SIG_SUBPKT_PREF_COMP_ALGS = 22
SIG_SUBPKT_KEYSRVR_PREFS  = 23
SIG_SUBPKT_PREF_KEYSRVR   = 24
SIG_SUBPKT_PRIM_UID       = 25
SIG_SUBPKT_POLICY_URL     = 26
SIG_SUBPKT_KEY_FLAGS      = 27
SIG_SUBPKT_SIGNERS_UID    = 28
SIG_SUBPKT_REVOC_REASON   = 29
SIG_SUBPKT_INTERNAL_0     = 100
SIG_SUBPKT_INTERNAL_1     = 101
SIG_SUBPKT_INTERNAL_2     = 102
SIG_SUBPKT_INTERNAL_3     = 103
SIG_SUBPKT_INTERNAL_4     = 104
SIG_SUBPKT_INTERNAL_5     = 105
SIG_SUBPKT_INTERNAL_6     = 106
SIG_SUBPKT_INTERNAL_7     = 107
SIG_SUBPKT_INTERNAL_8     = 108
SIG_SUBPKT_INTERNAL_9     = 109
SIG_SUBPKT_INTERNAL_A     = 110

# 3.6.2.1. Secret key encryption
ENCRYPTION_TYPE_UNENCRYPTED    = 0x00
ENCRYPTION_TYPE_S2K_SPECIFIED  = 0xff
# GPG man page hints at existence of "sha cehcksum" and claims it
#     will be part of "the new forthcoming extended openpgp specs"
#     for now: experimentally determined to be 0xFE
ENCRYPTION_TYPE_SHA1_CHECK = 0xfe

OLD_PKT_LEN_ONE_OCTET  = 0
OLD_PKT_LEN_TWO_OCTET  = 1
OLD_PKT_LEN_FOUR_OCTET = 2

# trust levels
TRUST_UNTRUSTED = 0
TRUST_MARGINAL  = 4
TRUST_FULL      = 5
TRUST_ULTIMATE  = 6

#trust packet headers
TRP_VERSION     = chr(1)
TRP_KEY         = chr(12)
TRP_USERID      = chr(13)

TRUST_PACKET_LENGTH = 40

SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

class PGPError(Exception):
    pass

class InvalidPacketError(PGPError):
    pass

class MalformedKeyRing(PGPError):
    def __str__(self):
        return self.error

    def __init__(self, reason="Malformed Key Ring"):
        self.error = "Malformed Key Ring: %s" %reason

class UnsupportedEncryptionAlgorithm(PGPError):
    def __init__(self, alg):
        self.alg = alg

    def __str__(self):
        return "Unsupported encryption algorithm code %s" % self.alg

class IncompatibleKey(PGPError):
    def __str__(self):
        return self.error

    def __init__(self, reason="Incompatible Key"):
        self.error = "Incompatible Key: %s" %reason

class InvalidKey(PGPError):
    def __str__(self):
        return self.error

    def __init__(self, reason="Invalid Key"):
        self.error = "Invalid Key: %s" %reason

class KeyNotFound(PGPError):
    def __str__(self):
        return self.error

    def __init__(self, keyId, reason=None):
        if keyId:
            self.error = "OpenPGP key not found for key ID %s" %keyId
            if isinstance(keyId, list):
                self.keys = keyId
            else:
                self.keys = [keyId]
        else:
            self.error = "No OpenPGP keys found"
        if reason:
            self.error += ': %s' %reason

class BadPassPhrase(PGPError):
    def __str__(self):
        return self.error

    def __init__(self, reason="Bad passphrase"):
        self.error = reason

class BadSelfSignature(PGPError):
    def __str__(self):
        return "Key %s failed self signature check" % self.keyId

    def __init__(self, keyId):
        self.keyId = keyId

class InvalidBodyError(PGPError):
    pass

class ShortReadError(InvalidBodyError):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual

def getKeyId(keyRing):
    pkt = newPacketFromStream(keyRing, start = -1)
    assert pkt is not None
    return pkt.getKeyId()

def seekKeyById(keyId, keyRing):
    if isinstance(keyRing, str):
        keyRing = util.ExtendedFile(keyRing, buffering = False)
    msg = PGP_Message(keyRing)
    try:
        return msg.iterByKeyId(keyId).next()
    except StopIteration:
        return False

def readKeyData(stream, keyId):
    """Read the key from the keyring and export it"""
    msg = PGP_Message(stream, start = 0)
    try:
        pkt = msg.iterByKeyId(keyId).next()
    except StopIteration:
        raise KeyNotFound(keyId)
    sio = StringIO()
    pkt.writeAll(sio)
    return sio.getvalue()

def verifySelfSignatures(keyId, stream):
    msg = PGP_Message(stream, start = 0)
    try:
        pkt = msg.iterByKeyId(keyId).next()
    except StopIteration:
        raise KeyNotFound(keyId)

    return pkt.verifySelfSignatures()

def fingerprintToInternalKeyId(fingerprint):
    if len(fingerprint) == 0:
        return ''
    fp = fingerprint[-16:]
    return ''.join([ chr(int(x + y, 16))
                   for x, y in zip(fp[0::2], fp[1::2])] )

def binSeqToString(sequence):
    """sequence is a sequence if unsigned chars.
    Return the string with a corresponding char for each item"""
    return "".join([ chr(x) for x in sequence ])

def simpleS2K(passPhrase, hash, keySize):
    # RFC 2440 3.6.1.1.
    r = ''
    iteration = 0
    keyLength = ((keySize + 7) // 8)
    while len(r) < keyLength:
        d = hash.new(chr(0) * iteration)
        d.update(passPhrase)
        r += d.digest()
        iteration += 1
    return r[:keyLength]

def saltedS2K(passPhrase, hash, keySize, salt):
    # RFC 2440 3.6.1.2.
    r = ''
    iteration = 0
    keyLength = ((keySize + 7) // 8)
    while(len(r) < keyLength):
        d = hash.new()
        buf = chr(0) * iteration
        buf += salt + passPhrase
        d.update(buf)
        r += d.digest()
        iteration += 1
    return r[:keyLength]

def iteratedS2K(passPhrase, hash, keySize, salt, count):
    # RFC 2440 3.6.1.3.
    r=''
    iteration = 0
    count=(16 + (count & 15)) << ((count >> 4) + 6)
    buf = salt + passPhrase
    keyLength = (keySize + 7) // 8
    while(len(r) < keyLength):
        d = hash.new()
        d.update(iteration * chr(0))
        total = 0
        while (count - total) > len(buf):
            d.update(buf)
            total += len(buf)
        if total:
            d.update(buf[:count-total])
        else:
            d.update(buf)
        r += d.digest()
        iteration += 1
    return r[:keyLength]

def getPublicKey(keyId, keyFile=''):
    if keyFile == '':
        if 'HOME' not in os.environ:
            keyFile = None
        else:
            keyFile=os.environ['HOME'] + '/.gnupg/pubring.gpg'
    try:
        keyRing = util.ExtendedFile(keyFile, buffering = False)
    except IOError:
        raise KeyNotFound(keyId, "Couldn't open pgp keyring")
    return _getPublicKey(keyId, keyRing)

def _getPublicKey(keyId, stream):
    msg = PGP_Message(stream, start = 0)
    try:
        pkt = msg.iterByKeyId(keyId).next()
    except StopIteration:
        raise KeyNotFound(keyId)
    pkt.verifySelfSignatures()
    return pkt.toPublicKey().makePgpKey()

def getPrivateKey(keyId, passPhrase='', keyFile=''):
    if keyFile == '':
        if 'HOME' not in os.environ:
            keyFile = None
        else:
            keyFile=os.environ['HOME'] + '/.gnupg/secring.gpg'
    try:
        keyRing = util.ExtendedFile(keyFile, buffering = False)
    except IOError:
        raise KeyNotFound(keyId, "Couldn't open pgp keyring")
    return _getPrivateKey(keyId, keyRing, passPhrase)

def _getPrivateKey(keyId, stream, passPhrase):
    msg = PGP_Message(stream, start = 0)
    try:
        pkt = msg.iterByKeyId(keyId).next()
    except StopIteration:
        raise KeyNotFound(keyId)
    pkt.verifySelfSignatures()
    return pkt.makePgpKey(passPhrase)

def getPublicKeyFromString(keyId, data):
    keyRing = util.ExtendedStringIO(data)
    return _getPublicKey(keyId, keyRing)

def getKeyEndOfLifeFromString(keyId, data):
    keyRing = util.ExtendedStringIO(data)
    return _getKeyEndOfLife(keyId, keyRing)

def getUserIdsFromString(keyId, data):
    keyRing = util.ExtendedStringIO(data)
    key = seekKeyById(keyId, keyRing)
    if key is None:
        return []
    return list(key.getUserIds())

def getFingerprint(keyId, keyFile=''):
    if keyFile == '':
        if 'HOME' not in os.environ:
            keyFile = None
        else:
            keyFile=os.environ['HOME'] + '/.gnupg/pubring.gpg'
    try:
        keyRing = util.ExtendedFile(keyFile, buffering = False)
    except IOError:
        raise KeyNotFound(keyId, "Couldn't open keyring")
    keyRing.seek(0, SEEK_END)
    limit = keyRing.tell()
    if limit == 0:
        # no keys in a zero length file
        raise KeyNotFound(keyId, "Couldn't open keyring")
    keyRing.seek(0, SEEK_SET)
    msg = PGP_Message(keyRing)
    try:
        pkt = msg.iterByKeyId(keyId).next()
    except StopIteration:
        raise KeyNotFound(keyId)
    return pkt.getKeyId()

def getKeyEndOfLife(keyId, keyFile=''):
    if keyFile == '':
        if 'HOME' not in os.environ:
            keyFile = None
        else:
            keyFile=os.environ['HOME'] + '/.gnupg/pubring.gpg'
    try:
        keyRing = util.ExtendedFile(keyFile, buffering = False)
    except IOError:
        raise KeyNotFound(keyId, "Couldn't open keyring")

    return _getKeyEndOfLife(keyId, keyRing)

def _getKeyEndOfLife(keyId, stream):
    msg = PGP_Message(stream, start = 0)
    try:
        pkt = msg.iterByKeyId(keyId).next()
    except StopIteration:
        raise KeyNotFound(keyId, "Key not found")
    return pkt.getEndOfLife()

def verifyRFC2440Checksum(data):
    # RFC 2440 5.5.3 - Secret Key Packet Formats documents the checksum
    if len(data) < 2:
        return 0
    checksum = [ ord(x) for x in data[-2:] ]
    checksum = int2bytes(*checksum)
    runningCount=0
    for i in range(len(data) - 2):
        runningCount += ord(data[i])
        runningCount %= 65536
    return (runningCount == checksum)

def verifySHAChecksum(data):
    if len(data) < 20:
        return 0
    m = sha.new()
    m.update(data[:-20])
    return m.digest() == data[-20:]

def xorStr(str1, str2):
    return ''.join(chr(ord(x) ^ ord(y)) for x, y in zip(str1, str2))

def countKeys(keyRing):
    # counts the public and private keys in a key ring (does not count subkeys)
    msg = PGP_Message(keyRing)
    return len([pkt for pkt in msg.iterPackets()
        if pkt.tag in (PKT_SECRET_KEY, PKT_PUBLIC_KEY)])

def getFingerprints(keyRing):
    # returns the fingerprints for all keys in a key ring file
    msg = PGP_Message(keyRing)
    return [ x.getKeyId() for x in msg.iterKeys() ]

def parseAsciiArmorKey(asciiData):
    data = StringIO(asciiData)
    nextLine=' '

    try:
        while(nextLine[0] != '-'):
            nextLine = data.readline()
        while (nextLine[0] != "\r") and (nextLine[0] != "\n"):
            nextLine = data.readline()
        buf = ""
        nextLine = data.readline()
        while(nextLine[0] != '='):
            buf = buf + nextLine
            nextLine = data.readline()
    except IndexError:
        data.close()
        return
    data.close()

    keyData = base64.b64decode(buf)
    return keyData

# this function will enforce the following rules
# rule 1: cannot switch main keys
# rule 2: a PGP Key in the repo may never lose a subkey
# rule 3: No revocations may be lost
# rules one and two are to prevent repo breakage
# rule three is to enforce a modicum of sanity to the security posture
def assertReplaceKeyAllowed(origKey, newKey):
    if not newKey.isSupersetOf(origKey):
        raise IncompatibleKey("Attempting to replace a key with a non-superset")

# this code is GnuPG specific. RFC 2440 indicates the existence of trust
# packets inside a keyring. GnuPG ignores this convention and keeps trust
# in a separate file generally called trustdb.gpg
# records are always 40 bytes long
# tags we care about are:
# 1: version stuff. always the first data packet
# 2 thru 11: we don't care
# 12: key trust packet
# 13: userid trust packet
# the formats of packets tagged 12 and 13 (by reverse engineering)
# offset 0: packet tag
# offset 1: reserved
# offsets 2-21: fingerprint of key/hash of userId 20 bytes either way
# offset 22: trust/validity value.
# offsets 23-39 don't matter for our purposes
# the trust is in the key packet. that will be what's returned once
# we establish the validity of the key (found in the userid packets)
def getKeyTrust(trustFile, fingerprint):
    # give nothing, get nothing
    if not fingerprint:
        return TRUST_UNTRUSTED
    try:
        trustDb = open(trustFile, 'r')
    except IOError:
        return TRUST_UNTRUSTED
    except:
        trustDb.close()
        raise
    # FIXME: verify trustdb version is 3
    found = 0
    done = 0
    # alter fingerprint to be the form found in the trustDB
    data = int (fingerprint, 16)
    keyId = ''
    while data:
        keyId = chr(data%256) + keyId
        data //= 256
    # seek for the right key record in the trust db
    while not done:
        dataChunk = trustDb.read(TRUST_PACKET_LENGTH)
        if len(dataChunk) == TRUST_PACKET_LENGTH:
            if (dataChunk[0] == TRP_KEY) and (dataChunk[2:22] == keyId):
                done = 1
                found = 1
        else:
            done = 1
    if not found:
        trustDb.close()
        return TRUST_UNTRUSTED
    trust = ord(dataChunk[22])
    # gnupg assigns lineal order to such things as expired and invalid
    # in a less than logical fashion. for our purposes, we'll simply
    # treat them all as untrusted
    if trust < TRUST_MARGINAL:
        trust = TRUST_UNTRUSTED
    # before returning this value, establish the validity of the key
    # the overall validity of a key is equal to the greatest validity
    # of any one userId that key has
    done = 0
    maxValidity = TRUST_UNTRUSTED
    while not done:
        dataChunk = trustDb.read(TRUST_PACKET_LENGTH)
        if (len(dataChunk) == TRUST_PACKET_LENGTH) and (dataChunk[0] == TRP_USERID):
            maxValidity = max(maxValidity, ord(dataChunk[22]))
        else:
            done = 1
    trustDb.close()
    # if the key isn't fully valid, by convention, it can't propogate any
    # imbued trust to the signatures made by that key
    if maxValidity >= TRUST_FULL:
        return trust
    return TRUST_UNTRUSTED


### New-style

class PGP_Message(object):
    __slots__ = ['_f', 'pos']
    def __init__(self, message, start = -1):
        if isinstance(message, str):
            # Assume a path
            self._f = util.ExtendedFile(message, buffering = False)
        else:
            # Be tolerant, accept non-Extended objects
            if isinstance(message, file) and not hasattr(message, "pread"):
                # Try to reopen as an ExtendedFile
                f = util.ExtendedFile(message.name, buffering = False)
                f.seek(message.tell())
                message = f
            if not hasattr(message, "pread"):
                raise MalformedKeyRing("Not an ExtendedFile object")
            self._f = message
        self.pos = start

    def _getPacket(self):
        pkt = newPacketFromStream(self._f, start = self.pos)
        return pkt

    def iterPackets(self):
        pkt = self._getPacket()
        while 1:
            if pkt is None:
                break
            yield pkt
            pkt = pkt.next()

    def iterKeys(self):
        """Iterate over all keys"""
        for pkt in self.iterMainKeys():
            yield pkt
            for subkey in pkt.iterSubKeys():
                yield subkey

    def iterMainKeys(self):
        """Iterate over main keys"""
        for pkt in self.iterPackets():
            if isinstance(pkt, PGP_MainKey):
                pkt.initSubPackets()
                yield pkt

    def iterByKeyId(self, keyId):
        """Iterate over the keys with this key ID"""
        for pkt in self.iterKeys():
            if keyId.upper() in pkt.getKeyId():
                yield pkt

    def seekParentKey(self, keyId):
        """Get a parent key with this keyId or with a subkey with this
        keyId"""
        for pkt in self.iterKeys():
            if isinstance(pkt, PGP_MainKey):
                if keyId.upper() in pkt.getKeyId():
                    # This is a main key and it has the keyId we need
                    return pkt
            elif isinstance(pkt, PGP_SubKey):
                if keyId.upper() in pkt.getKeyId():
                    # This is a subkey, return the main key
                    return pkt.getMainKey()

class PacketTypeDispatcher(object):
    _registry = {}

    @staticmethod
    def addPacketType(klass):
        PacketTypeDispatcher._registry[klass.tag] = klass

    @staticmethod
    def getClass(tag):
        return PacketTypeDispatcher._registry.get(tag, PGP_Packet)

class PGP_PacketFromStream(object):
    __slots__ = ['_f', 'tag', 'headerLength', 'bodyLength']
    def __init__(self):
        self.tag = None
        self.headerLength = self.bodyLength = 0
        self._f = None

    def read(self, fileobj, start = -1):
        """Create packet from stream
        Return a PGP_Packet instance"""
        self._f = util.SeekableNestedFile(fileobj, 1, start)
        first = self._f.read(1)
        if not first:
            # No more packets to read from this file object
            return

        first = ord(first)

        if not (first & 0x80):
            raise InvalidPacketError("First bit not 1")

        if first & 0x40:
            newStyle = True
            self._newHeader(first)
        else:
            newStyle = False
            self._oldHeader(first)

        _bodyStream = util.SeekableNestedFile(self._f.file,
                     self.bodyLength, self._f.start + self.headerLength)
        if self.bodyLength:
            # Read one octet from the end
            data = _bodyStream.pread(1, self.bodyLength - 1)
            if not data:
                raise ShortReadError(self.bodyLength, -1)
            _bodyStream.seek(0)
        nextStreamPos = self._f.start + self.headerLength + self.bodyLength

        pkt = newPacket(self.tag, _bodyStream, newStyle = newStyle,
                        minHeaderLen = self.headerLength)
        pkt.setNextStream(fileobj, nextStreamPos)
        return pkt

    def _oldHeader(self, first):
        self.tag = (first & 0x3C) >> 2
        lengthType = first & 0x03
        if lengthType in (0, 1, 2):
            headerLength = lengthType + 2
            if lengthType == 2:
                headerLength += 1
            blLen = headerLength - 1
            # Extend file
            self._f.__init__(self._f.file, headerLength, self._f.start)
            self._f.seek(1)
        else:
            headerLength = 1
            blLen = None
            raise NotImplementedError("Indeterminate length not supported")

        self.headerLength = headerLength
        bbytes = PGP_BasePacket._readBin(self._f, blLen)

        bodyLength = 0
        for i in bbytes:
            bodyLength <<= 8
            bodyLength += i
        self.bodyLength = bodyLength

    def _newHeader(self, first):
        # New style
        self.tag = (first & 0x3F)
        # Extend by one more byte
        self._f.__init__(self._f.file, 2, self._f.start)
        self._f.seek(1)

        body1, = PGP_BasePacket._readBin(self._f, 1)

        if body1 & 0xC0 == 0:
            # 4.2.2.1. One-Octet Lengths (less than 192)
            self.headerLength = 2
            self.bodyLength = body1
            return

        if 192 <= body1 < 223:
            # 4.2.2.2. Two-Octet Lengths (between 192 and 223):
            self.headerLength = 3
            self._f.__init__(self._f.file, self.headerLength, self._f.start)
            self._f.seek(2)

            body2, = PGP_BasePacket._readBin(self._f, 1)
            self.bodyLength = len2bytes(body1, body2)
            return

        if body1 == 0xFF:
            # 4.2.2.3. Five-Octet Lengths (exactly 255)
            self.headerLength = 6

            self._f.__init__(self._f.file, self.headerLength, self._f.start)
            self._f.seek(2)

            rest = PGP_BasePacket._readBin(self._f, 4)
            self.bodyLength = int4bytes(*rest)
            return
        # 4.2.2.4. Partial Body Lengths
        partialBodyLength = 1 << (body1 & 0x1F)
        raise NotImplementedError("Patial body lengths not implemented")

class PGP_BasePacket(object):
    __slots__ = ['_bodyStream', 'headerLength', 'bodyLength',
                 '_newStyle', '_nextStream', '_nextStreamPos' ]

    tag = None
    BUFFER_SIZE = 16384

    def __init__(self, bodyStream, newStyle = False, minHeaderLen = 2):
        assert hasattr(bodyStream, 'pread')
        self._newStyle = newStyle
        self._bodyStream = bodyStream
        self.bodyLength = self._getBodyLength()
        self.headerLength = self._getHeaderLength(minHeaderLen = minHeaderLen)
        # Keep a reference to the next stream we link to
        self._nextStream = None
        self._nextStreamPos = 0
        self.validate()

    def setNextStream(self, stream, pos):
        if stream:
            assert hasattr(stream, 'pread')
        self._nextStream = stream
        self._nextStreamPos = pos

    def clone(self):
        """Produce another packet identical with this one"""
        # Create new body stream sharing the same file
        newBodyStream = util.SeekableNestedFile(self._bodyStream.file,
            self._bodyStream.size, self._bodyStream.start)

        newPkt = newPacket(self.tag, newBodyStream,
                    newStyle = self._newStyle, minHeaderLen = self.headerLength)
        newPkt.setNextStream(self._nextStream, self._nextStreamPos)
        return newPkt

    def validate(self):
        """To be overridden by various subclasses"""
        pass

    def _getHeaderLength(self, minHeaderLen = 2):
        # bsrepr is the body size representation
        if self._newStyle:
            # For new style, we can't really force the minimum header length
            if self.bodyLength < 192:
                return 2
            if 192 <= self.bodyLength < 8384:
                return 3
            return 6
        if minHeaderLen > 3 or self.bodyLength > 65535:
            # 4-byte packet length field
            bsrepr = 4
        elif minHeaderLen > 2 or self.bodyLength > 255:
            # 2-byte packet length field
            bsrepr = 2
        else:
            # 1 byte packet-length field
            bsrepr = 1

        return bsrepr + 1

    def _getBodyLength(self):
        """Determine the body length"""
        pos = self._bodyStream.tell()
        self._bodyStream.seek(0, SEEK_END)
        blen = self._bodyStream.tell()
        self._bodyStream.seek(pos, SEEK_SET)
        return blen

    def writeHeader(self, stream):
        # Generate packet header
        if self._newStyle:
            return self._writeHeaderNewStyle(stream)

        return self._writeHeaderOldStyle(stream)

    def _writeHeaderNewStyle(self, stream):
        # bit 7 is set, bit 6 is set (new packet format)
        fbyte = 0xC0

        # Add the tag.
        fbyte |= self.tag

        stream.write(chr(fbyte))

        if self.headerLength == 6:
            # 5-byte body length length, first byte is 255
            stream.write(chr(255))
            blen = self.bodyLength & 0xffffffff
            for i in range(1, 5):
                stream.write(chr((blen >> ((4 - i) << 3)) & 0xff))
            return
        if self.headerLength == 3:
            # 2-byte body length length
            if not (192 <= self.bodyLength < 8384):
                raise InvalidPacketError("Invalid body length %s for "
                    "header length %s" % (self.bodyLength, self.headerLength))
            stream.write(chr(((self.bodyLength - 192) >> 8) + 192))
            stream.write(chr((self.bodyLength - 192) & 0xff))
            return 
        if self.headerLength == 2:
            # 1-byte body length length
            if not (self.bodyLength < 192):
                raise InvalidPacketError("Invalid body length %s for "
                    "header length %s" % (self.bodyLength, self.headerLength))
            stream.write(chr(self.bodyLength))
            return
        raise InvalidPacketError("Invalid header length %s" % self.headerLength)

    def _writeHeaderOldStyle(self, stream):
        # bit 7 is set, bit 6 is not set (old packet format)
        fbyte = 0x80

        # Add the tag, bits 5432. For old-style headers, they are represented
        # on 4 bits only.
        fbyte |= (0x0f & self.tag) << 2

        # bsrepr is the body size representation
        if self.headerLength == 5:
            # 4-byte packet length field
            fbyte |= 2
            bsrepr = 4
        elif self.headerLength == 3:
            # 2-byte packet length field
            fbyte |= 1
            bsrepr = 2
        else:
            # 1 byte packet-length field (no changes to first byte needed)
            bsrepr = 1

        stream.write(chr(fbyte))
        # prepare the size octets
        for i in range(1, bsrepr + 1):
            stream.write(chr((self.bodyLength >> ((bsrepr - i) << 3)) & 0xff))

    def writeBody(self, stream):
        self.resetBody()
        self._bodyStream.seek(0)
        self._copyStream(self._bodyStream, stream)

    def write(self, stream):
        self.writeHeader(stream)
        self.writeBody(stream)

    def writeAll(self, stream):
        # Write this packet and all subpackets
        self.write(stream)
        for pkt in self.iterSubPackets():
            pkt.write(stream)

    def resetBody(self):
        self._bodyStream.seek(0)

    def readBody(self, bytes = -1):
        """Read bytes from stream"""
        return self._bodyStream.read(bytes = bytes)

    def seek(self, pos, whence = SEEK_SET):
        return self._bodyStream.seek(pos, whence)

    @staticmethod
    def _readExact(stream, bytes):
        """Read bytes from stream, checking that enough bytes were read"""
        data = stream.read(bytes)
        if bytes > 0 and len(data) != bytes:
            raise ShortReadError(bytes, len(data))
        return data

    @staticmethod
    def _readBin(stream, bytes):
        """Read bytes from stream, checking that enough bytes were read.
        Return a list of bytes"""
        return [ ord(x) for x in PGP_BasePacket._readExact(stream, bytes) ]

    def readExact(self, bytes):
        """Read bytes from stream, checking that enough bytes were read"""
        return self._readExact(self._bodyStream, bytes)

    def readBin(self, bytes):
        """Read bytes from stream, checking that enough bytes were read.
        Return a list of bytes"""
        return self._readBin(self._bodyStream, bytes)

    @staticmethod
    def _writeBin(stream, bytes):
        """Write the bytes in binary format"""
        for b in bytes:
            stream.write(chr(b))

    @staticmethod
    def _copyStream(src, dst):
        """Copy stream src into dst"""
        while 1:
            buf = src.read(PGP_BasePacket.BUFFER_SIZE)
            if not buf:
                break
            dst.write(buf)

    @staticmethod
    def _updateHash(hashObj, stream):
        """Update the hash object with data from the stream"""
        while 1:
            buf = stream.read(PGP_BasePacket.BUFFER_SIZE)
            if not buf:
                break
            hashObj.update(buf)

    @staticmethod
    def checkStreamLength(stream, length):
        """Checks that the stream has exactly the length specified"""
        pos = stream.tell()
        stream.seek(0, SEEK_END)
        if length != stream.tell() - pos:
            raise ShortReadError(length, stream.tell() - pos)
        # Rewind
        stream.seek(pos)

    @staticmethod
    def readTimestamp(stream):
        """Reads a timestamp from the stream"""
        PGP_BasePacket.checkStreamLength(stream, 4)
        return len4bytes(*PGP_BasePacket._readBin(stream, 4))

    def isEmpty(self):
        return self.headerLength == 0

    def next(self):
        if self._nextStream is None:
            raise StopIteration()

        newPkt = newPacketFromStream(self._nextStream, self._nextStreamPos)
        if newPkt is None:
            raise StopIteration()

        return newPkt

    def getBodyStream(self):
        return self._bodyStream

    def _iterSubPackets(self, limitTags):
        """Iterate over the packets following this packet, until we reach a
        packet of the specified type as the limit"""
        pkt = self.next()
        while not pkt.isEmpty() and pkt.tag not in limitTags:
            yield pkt
            pkt = pkt.next()

    @staticmethod
    def _hashSet(items):
        """Hashes the items in items through sha, and return a set of the
        computed digests.
        Each item is expected to be a stream"""

        ret = set([])
        for stream in items:
            stream.seek(0)
            hobj = sha.new()
            PGP_BasePacket._updateHash(hobj, stream)
            ret.add(hobj.digest())
        return ret

class PGP_Packet(PGP_BasePacket):
    """Anonymous PGP packet"""
    __slots__ = ['tag']
    def setTag(self, tag):
        self.tag = tag

class PGP_BaseKeySig(PGP_BasePacket):
    """Base class for keys and signatures"""
    __slots__ = []

    def _getMPICount(self, algType):
        """This returns the right number of MPIs for converting a private key
        to a public key. Overwrite in subclasses for any other usage"""
        if algType in PK_ALGO_ALL_RSA:
            numMPI = 2
        elif algType in PK_ALGO_ALL_ELGAMAL:
            numMPI = 3
        elif algType == PK_ALGO_DSA:
            numMPI = 4
        else:
            # unhandled algorithm
            raise UnsupportedEncryptionAlgorithm(algType)
        return numMPI


    def _readMPIs(self, stream, algType, discard = True):
        """Read the corresponding number of MPIs for the specified algorithm
        type from the stream
        @raise UnsupportedEncryptionAlgorithm
        """
        numMPI = self._getMPICount(algType)
        return self._readCountMPIs(stream, numMPI, discard = discard)

    def _readCountMPIs(self, stream, count, discard = True):
        """Read count MPs from the current position in stream.
        @raise UnsupportedEncryptionAlgorithm
        """

        ret = []
        for i in range(count):
            buf = self._readBin(stream, 2)
            mLen = (int2bytes(*buf) + 7) // 8
            if discard:
                # Skip the MPI len
                self._readExact(stream, mLen)
                ret.append(None)
            else:
                data = self._readBin(stream, mLen)
                r = 0L
                for i in data:
                    r = r * 256 + i
                ret.append(r)
        return ret

    def skipMPIs(self, stream, algType):
        self._readMPIs(stream, algType, discard = True)

    def readMPIs(self, stream, algType):
        return self._readMPIs(stream, algType, discard = False)

class PGP_Signature(PGP_BaseKeySig):
    __slots__ = ['version', 'sigType', 'pubKeyAlg', 'hashAlg', 'hashSig',
                 'mpiFile', 'signerKeyId', 'hashedFile', 'unhashedFile',
                 '_parsed']
    tag = PKT_SIG

    def validate(self):
        self.version = self.sigType = self.pubKeyAlg = self.hashAlg = None
        self.hashSig = self.mpiFile = self.signerKeyId = None
        self.hashedFile = self.unhashedFile = None
        self._parsed = False

    def parse(self):
        """Parse the signature body and initializes the internal data
        structures for other operations"""
        self.resetBody()
        sigVersion, = self.readBin(1)
        if sigVersion not in [3, 4]:
            raise InvalidBodyError("Invalid signature version %s" % sigVersion)
        self.version = sigVersion
        if sigVersion == 3:
            self._readSigV3()
        else:
            self._readSigV4()
        self._parsed = True

    def _getMPICount(self, algType):
        if algType in PK_ALGO_ALL_RSA:
            numMPI = 1
        elif algType in PK_ALGO_ALL_ELGAMAL:
            numMPI = 2
        elif algType == PK_ALGO_DSA:
            numMPI = 2
        else:
            # unhandled algorithm
            raise UnsupportedEncryptionAlgorithm(algType)
        return numMPI

    def parseMPIs(self):
        if not self._parsed:
            self.parse()
        assert hasattr(self, 'mpiFile') and self.mpiFile is not None
        self.mpiFile.seek(0)
        return self.readMPIs(self.mpiFile, self.pubKeyAlg)

    def _readSigV3(self):
        hLen, sigType = self.readBin(2)
        if hLen != 5:
            raise PGPError('Expected 5 octets of length of hashed material, '
                           'got %d' % hLen)

        creation = self.readBin(4)
        self.signerKeyId = self.readBin(8)
        pkAlg, hashAlg, sig0, sig1 = self.readBin(4)

        self.sigType = sigType
        self.pubKeyAlg = pkAlg
        self.hashAlg = hashAlg
        self.hashSig = (sig0, sig1)
        raise IncompatibleKey("Must be a V4 signature")

    def _readSigV4(self):
        sigType, pkAlg, hashAlg = self.readBin(3)
        # Hashed subpacket data length
        arr = self.readBin(2)
        hSubPktLen = (arr[0] << 8) + arr[1]
        hSubpktsFile = util.SeekableNestedFile(self._bodyStream, hSubPktLen)

        # Skip over the packets, we've decoded them already
        self.seek(hSubPktLen, SEEK_CUR)

        # Unhashed subpacket data length
        arr = self.readBin(2)
        uSubPktLen = (arr[0] << 8) + arr[1]

        uSubpktsFile = util.SeekableNestedFile(self._bodyStream, uSubPktLen)
        # Skip over the packets, we've decoded them already
        self.seek(uSubPktLen, SEEK_CUR)

        # Two-octet field holding left 16 bits of signed hash value.
        hashSig = self.readBin(2)

        # MPI data
        mpiFile = util.SeekableNestedFile(self._bodyStream,
            self.bodyLength - self._bodyStream.tell())

        self.sigType = sigType
        self.pubKeyAlg = pkAlg
        self.hashAlg = hashAlg
        self.mpiFile = mpiFile
        self.hashSig = hashSig
        self.hashedFile = hSubpktsFile
        self.unhashedFile = uSubpktsFile

    def getSigId(self):
        """Get the key ID of the issuer for this signature.
        Return None if the packet did not contain an issuer key ID"""
        if not self._parsed:
            self.parse()
        if self.signerKeyId is not None:
            return binSeqToString(self.signerKeyId)
        # Version 3 packets should have already set signerKeyId
        assert self.version == 4
        for spktType, dataf in self.decodeSigSubpackets(self.unhashedFile):
            if spktType != SIG_SUBPKT_ISSUER_KEYID:
                continue
            # Verify it only contains 8 bytes
            try:
                self.checkStreamLength(dataf, 8)
            except ShortReadError, e:
                raise InvalidPacketError("Expected %s bytes, got %s instead" %
                    (e.expected, e.actual))
            self.signerKeyId = self._readBin(dataf, 8)
            return binSeqToString(self.signerKeyId)

    def decodeSigSubpackets(self, fobj):
        fobj.seek(0)
        while fobj.size != fobj.tell():
            yield self._getNextSubpacket(fobj)

    def _getNextSubpacket(self, fobj):
        len0, = self._readBin(fobj, 1)

        # Sect 5.2.3.1 of RFC2440 implies there should be a 2-octet scalar
        # count of the length of the set of subpackets, but I can't seem to
        # find it here.

        if len0 & 0xC0 == 0:
            pktlenlen = 1
            pktlen = len0
        elif len0 == 0xFF:
            pktlenlen = 5
            data = self._readBin(fobj, 4)
            pktlen = len4bytes(*data)
        else:
            pktlenlen = 2
            len1, = self._readBin(fobj, 1)
            pktlen = len2bytes(len0, len1)

        spktType, = self._readBin(fobj, 1)

        # The packet length includes the subpacket type
        dataf = util.SeekableNestedFile(fobj, pktlen - 1)
        # Do we have enough data?
        dataf.seek(0, SEEK_END)
        if dataf.tell() != pktlen - 1:
            raise ShortReadError(pktlen + pktlenlen, dataf.tell())
        dataf.seek(0, SEEK_SET)

        # Skip the data
        fobj.seek(pktlen - 1, SEEK_CUR)
        return spktType, dataf

    def _finalizeSelfSig(self, dataFile, mainKey):
        """Append more data to dataFile and compute the self signature"""
        if not self._parsed:
            self.parse()
        if self.version != 4:
            raise InvalidKey("Self signature is not a V4 signature")
        dataFile.seek(0, SEEK_END)
        digSig = self.parseMPIs()

        # (re)compute the hashed packet subpacket data length
        self.hashedFile.seek(0, SEEK_END)
        hSubPktLen = self.hashedFile.tell()
        self.hashedFile.seek(0, SEEK_SET)

        # Write signature version, sig type, pub alg, hash alg
        self._writeBin(dataFile, [ self.version, self.sigType, self.pubKeyAlg,
                                   self.hashAlg ])
        # Write hashed data length
        self._writeBin(dataFile, [ hSubPktLen // 256, hSubPktLen % 256 ])
        # Write the hashed data
        self._copyStream(self.hashedFile, dataFile)

        # We've added 6 bytes for the header
        dataLen = hSubPktLen + 6

        # Append trailer - 5-byte header
        self._writeBin(dataFile, [ 0x04, 0xFF,
            (dataLen // 0x1000000) & 0xFF, (dataLen // 0x10000) & 0xFF,
            (dataLen // 0x100) & 0xFF, dataLen & 0xFF ])
        hashAlgList = [ None, md5, sha]
        hashFunc = hashAlgList[self.hashAlg]
        hashObj = hashFunc.new()

        # Rewind dataFile, we need to hash it
        dataFile.seek(0, SEEK_SET)
        self._updateHash(hashObj, dataFile)
        sigString = hashObj.digest()
        # if this is an RSA signature, it needs to properly padded
        # RFC 2440 5.2.2 and RFC 2313 10.1.2

        if self.pubKeyAlg in PK_ALGO_ALL_RSA:
            # hashPads from RFC2440 section 5.2.2
            hashPads = [ '', '\x000 0\x0c\x06\x08*\x86H\x86\xf7\r\x02\x05\x05\x00\x04\x10', '\x000!0\t\x06\x05+\x0e\x03\x02\x1a\x05\x00\x04\x14' ]
            padLen = (len(hex(mainKey.n)) - 5 - 2 * (len(sigString) + len(hashPads[self.hashAlg]))) // 2 -1
            sigString = chr(1) + chr(0xFF) * padLen + hashPads[self.hashAlg] + sigString

        if not mainKey.verify(sigString, digSig):
            raise BadSelfSignature(None)


PacketTypeDispatcher.addPacketType(PGP_Signature)

class PGP_UserID(PGP_BasePacket):
    __slots__ = ['id', 'signatures']
    tag = PKT_USERID

    def validate(self):
        self.resetBody()
        self.id = self.readBody()
        # Signatures for this user ID
        self.signatures = None

    def toString(self):
        return self.id

    def addSignatures(self, signatures):
        """Add signatures to this UserID"""
        if self.signatures is None:
            self.signatures = []
        for sig in signatures:
            assert isinstance(sig, PGP_Signature)
            self.signatures.append(sig)

    def iterSignatures(self):
        """Iterate over this user's UserID"""
        if self.signatures is not None:
            return iter(self.signatures)
        raise PGPError("Key packet not parsed")

    def iterKeySignatures(self, keyId):
        intKeyId = fingerprintToInternalKeyId(keyId)
        # Look for a signature by this key
        for pkt in self.iterSignatures():
            if intKeyId != pkt.getSigId():
                continue
            yield pkt

    def writeHash(self, stream):
        """Write a UserID packet in a stream, in order to be hashed.
        Described in RFC 2440 5.2.4 computing signatures."""
        assert len(self.id) == self.bodyLength
        stream.write(chr(0xB4))
        stream.write(struct.pack("!I", self.bodyLength))
        stream.write(self.id)

PacketTypeDispatcher.addPacketType(PGP_UserID)

class PGP_Key(PGP_BaseKeySig):
    __slots__ = ['_parsed', 'version', 'createdTimestamp', 'pubKeyAlg',
                 'mpiFile', 'mpiLen', 'daysValid', '_keyId']
    # Base class for public/secret keys/subkeys
    tag = None

    def validate(self):
        self.version = self.createdTimestamp = self.pubKeyAlg = None
        self.mpiFile = self.mpiLen = None
        self.daysValid = None
        # Cache
        self._keyId = None
        self._parsed = False

    def parse(self):
        """Parse the signature body and initializes the internal data
        structures for other operations"""
        self.resetBody()
        keyVersion, = self.readBin(1)
        if keyVersion not in [3, 4]:
            raise InvalidBodyError("Invalid key version %s" % keyVersion)
        self.version = keyVersion

        if keyVersion == 3:
            self._readKeyV3()
        else:
            self._readKeyV4()
        self._parsed = True

    def _readKeyV3(self):
        # RFC 2440, sect. 5.5.2
        # We only support V4 keys
        raise InvalidKey("Version 3 keys not supported")
        #self.createdTimestamp = len4bytes(*self._readBin(self._bodyStream, 4))

        ## daysValid
        #data = self.readBin(2)
        #self.daysValid = int2bytes(*data)

        ## Public key algorithm
        #self.pubKeyAlg, = self.readBin(1)

        # Record current position in body
        #mpiStart = self._bodyStream.tell()
        ## Read and discard 2 MPIs
        #self._readCountMPIs(self._bodyStream, count, discard = True)
        #self.mpiLen = self._bodyStream.tell() - mpiStart
        #self.mpiFile = util.SeekableNestedFile(self._bodyStream, self.mpiLen,
        #    start = mpiStart)

    def _readKeyV4(self):
        # RFC 2440, sect. 5.5.2
        # Key creation
        self.createdTimestamp = len4bytes(*self._readBin(self._bodyStream, 4))

        # Public key algorithm
        self.pubKeyAlg, = self.readBin(1)

        # Record current position in body
        mpiStart = self._bodyStream.tell()
        # Skip over the MPIs
        self.skipMPIs(self._bodyStream, self.pubKeyAlg)
        self.mpiLen = self._bodyStream.tell() - mpiStart
        self.mpiFile = util.SeekableNestedFile(self._bodyStream, self.mpiLen,
            start = mpiStart)

    def getKeyId(self):
        if self._keyId is not None:
            return self._keyId

        # Convert to public key

        pkt = self.toPublicKey(minHeaderLen = 3)

        # Why minHeaderLen = 3?

        # This is a holdover from the days of PGP 2.6.2
        # RFC 2440 section 11.2 does a really bad job of explaining this.
        # RFC 2440 section 5.2.4 refers to this for self signature computation.
        # One of the least documented gotchas of Key fingerprints:
        # they're ALWAYS calculated as if they were a public key main key block.
        # this means private keys will be treated as public keys, and subkeys
        # will be treated as main keys for the purposes of this test.
        # Furthermore if the length was one byte long it must be translated
        # into a 2 byte long length (upper octet is 0)
        # not doing this will result in key fingerprints which do not match the
        # output produced by OpenPGP compliant programs.
        # this will result in the first octet ALWYAS being 0x99
        # in binary 10 0110 01
        # 10 indicates old style PGP packet
        # 0110 indicates public key
        # 01 indicates 2 bytes length

        m = sha.new()
        sio = util.ExtendedStringIO()
        # Write only the header, we can copy the body directly from the
        # body stream
        pkt.writeHeader(sio)
        m.update(sio.getvalue())

        pkt.resetBody()
        while 1:
            buf = pkt.readBody(self.BUFFER_SIZE)
            if not buf:
                break
            m.update(buf)

        self._keyId = m.hexdigest().upper()
        return self._keyId

    def getEndOfLife(self):
        """Parse self signatures to find timestamp(s) of key expiration.
        Also seek out any revocation timestamps.
        We don't need to actually verify these signatures.
        See verifySelfSignatures()
        Returns bool, timestamp (is revoked, expiration)
        """
        parentExpire = 0
        parentRevoked = False

        if self.tag in PKT_SUB_KEYS:
            # Look for parent key's expiration
            parentRevoked, parentExpire = self.getMainKey().getEndOfLife()

        expireTimestamp = revocTimestamp = 0

        # Iterate over self signatures
        for pkt in self.iterAllSelfSignatures():
            if pkt.sigType in SIG_CERTS:
                eTimestamp = cTimestamp = 0
                for spktType, dataf in pkt.decodeSigSubpackets(pkt.hashedFile):
                    if spktType == SIG_SUBPKT_KEY_EXPIRE:
                        eTimestamp = self.readTimestamp(dataf)
                    elif spktType == SIG_SUBPKT_CREATION:
                        cTimestamp = self.readTimestamp(dataf)
                # if there's no expiration, DON'T COMPUTE this, otherwise
                # it will appear as if the key expired the very moment
                # it was created.
                if eTimestamp:
                    ts = eTimestamp + cTimestamp
                    expireTimestamp = max(expireTimestamp, ts)
            elif pkt.sigType in SIG_KEY_REVOCS:
                # parse this revocation to look for the creation timestamp
                # we're ultimately looking for the most stringent revocation
                for spktType, dataf in pkt.decodeSigSubpackets(pkt.hashedFile):
                    if spktType == SIG_SUBPKT_CREATION:
                        ts = self.readTimestamp(dataf)
                        if revocTimestamp:
                            revocTimestamp = min(expireTimestamp, ts)
                        else:
                            revocTimestamp = ts

        # return minimum non-zero value of the three expirations
        # unless they're ALL zero. 8-)
        if not (revocTimestamp or expireTimestamp or parentExpire):
            return False, 0

        # make no assumptions about how big a timestamp is.
        ts = max(revocTimestamp, expireTimestamp, parentExpire)
        if revocTimestamp:
            ts = min(ts, revocTimestamp)
        if expireTimestamp:
            ts = min(ts, expireTimestamp)
        if parentExpire:
            ts = min(ts, parentExpire)
        return (revocTimestamp != 0) and (not parentRevoked), ts

    def iterSelfSignatures(self):
        return self._iterSelfSignatures(self.getKeyId())

    def _iterSelfSignatures(self, keyId):
        """Iterate over all the self-signatures"""
        if self._parsed is False:
            self.parse()

        intKeyId = fingerprintToInternalKeyId(keyId)
        # Look for a self signature
        for pkt in self.iterSignatures():
            if intKeyId != pkt.getSigId():
                continue
            yield pkt

    def iterAllSelfSignatures(self):
        """Iterate over direct signatures and UserId signatures"""
        return self._iterAllSelfSignatures(self.getKeyId())

    def _iterAllSelfSignatures(self, keyId):
        for pkt in self.iterSelfSignatures():
            yield pkt
        intKeyId = fingerprintToInternalKeyId(keyId)
        for uid in self.iterUserIds():
            for pkt in uid.iterSignatures():
                if intKeyId != pkt.getSigId():
                    continue
                yield pkt

    def assertSigningKey(self):
        # Find self signature of this key
        # first search for the public key algortihm octet. if the key is really
        # old, this might be the only hint that it's legal to use this key to
        # make digital signatures.
        if self._parsed is False:
            self.parse()

        if self.pubKeyAlg in (PK_ALGO_RSA_SIGN_ONLY, PK_ALGO_DSA):
            # the public key algorithm octet satisfies this test. no more
            # checks required.
            return True

        keyId = self.getKeyId()

        # If it's a subkey, look for the master key
        if self.tag in PKT_SUB_KEYS:
            pkt = self.getMainKey()
            return pkt.assertSigningKey()

        # Look for a self signature
        for pkt in self.iterAllSelfSignatures():
            # We know it's a ver4 packet, otherwise getSigId would have failed
            for spktType, dataf in pkt.decodeSigSubpackets(pkt.hashedFile):
                if spktType == SIG_SUBPKT_KEY_FLAGS:
                    # RFC 2440, sect. 5.2.3.20
                    foct, = self._readBin(dataf, 1)
                    if foct & 0x02:
                        return True
        # No subpacket or no key flags
        raise IncompatibleKey('Key %s is not a signing key.'% keyId)

    def getPublicKeyTuple(self):
        """Return the key material"""
        if not self._parsed:
            self.parse()
        self.mpiFile.seek(0, SEEK_SET)
        return self.readMPIs(self.mpiFile, self.pubKeyAlg)

    def makePgpKey(self, passPhrase = None):
        assert passPhrase is None
        pkTuple = self.getPublicKeyTuple()
        if self.pubKeyAlg in PK_ALGO_ALL_RSA:
            n, e = pkTuple
            return RSA.construct((n, e))
        if self.pubKeyAlg == PK_ALGO_DSA:
            p, q, g, y = pkTuple
            return DSA.construct((y, g, p, q))
        raise MalformedKeyRing("Can't use El-Gamal keys in current version")

class PGP_MainKey(PGP_Key):
    def initSubPackets(self):
        if hasattr(self, "subkeys"):
            # Already processed
            return

        self.revsigs = []
        self.uids = []
        self.subkeys = []

        subpkts = [ x for x in self._iterSubPackets(PKT_MAIN_KEYS) ]

        # Start reading signatures until we hit a UserID or another key
        limit = set(PKT_SUB_KEYS)
        limit.add(PKT_USERID)
        i = 0
        for pkt in subpkts:
            if pkt.tag in limit:
                # UserID or subkey
                break
            i += 1
            if not isinstance(pkt, PGP_Signature):
                continue
            pkt.parse()
            if pkt.sigType == SIG_TYPE_KEY_REVOC:
                # Key revocation
                self.revsigs.append(pkt)
                continue
            # According to sect. 10.1, there should not be other signatures
            # here.
            assert False, "Unexpected signature type %s" % pkt.sigType

        sigLimit = i

        # Read until we hit a subkey
        limit = set(PKT_SUB_KEYS)
        i = 0
        for pkt in subpkts[sigLimit:]:
            if pkt.tag in limit:
                break
            i += 1
            # Certification revocations live together with regular signatures
            # or so is the RFC saying
            if isinstance(pkt, PGP_UserID):
                self.uids.append(pkt)
                continue
            if isinstance(pkt, PGP_Signature):
                # This can't be the first packet, or we wouldn't have stopped
                # in the previous loop
                # Add this signature to the last user id we found
                self.uids[-1].addSignatures([pkt])
                continue
            # We ignore other packets (like trust)

        uidLimit = sigLimit + i

        # Read until the end
        # We don't want to point back to ourselves, or we'll create a
        # circular loop.
        newMainKey = self.clone()
        # Don't call initSubPackets on newMainKey here, or you end up with an
        # infinite loop.
        for pkt in subpkts[uidLimit:]:
            if isinstance(pkt, PGP_SubKey):
                pkt.mainKey = newMainKey
                self.subkeys.append(pkt)
                continue
            if isinstance(pkt, PGP_Signature):
                # This can't be the first packet, or we wouldn't have stopped
                # in the previous loop
                subkey = self.subkeys[-1]
                pkt.parse()
                if pkt.sigType == SIG_TYPE_SUBKEY_REVOC:
                    subkey.bindingSigRevoc = pkt
                    continue
                if pkt.sigType == SIG_TYPE_SUBKEY_BIND:
                    subkey.bindingSig = pkt
                    continue
                # There should not be any other type of signature here
                assert False, "Unexpected signature type %s" % pkt.sigType
            # Ignore other packets

    def iterUserIds(self):
        self.initSubPackets()
        return iter(self.uids)

    def iterSubPackets(self):
        for sig in self.iterSignatures():
            yield sig
        for uid in self.iterUserIds():
            yield uid
            for sig in uid.iterSignatures():
                yield sig
        for subkey in self.iterSubKeys():
            yield subkey
            for pkt in subkey.iterSubPackets():
                yield pkt

    def iterSignatures(self):
        self.initSubPackets()
        return iter(self.revsigs)

    def iterSubKeys(self):
        self.initSubPackets()
        return iter(self.subkeys)

    def verifySelfSignatures(self):
        """
        Verify the self signatures on this key.
        If successful, returns the public key packet associated with this key,
        and crypto key.
        @return (pubKeyPacket, cryptoKey)
        @raises BadSelfSignature
        """
        # Convert to a public key (even if it's already a public key)
        pkpkt = self.toPublicKey(minHeaderLen = 3)
        keyId = pkpkt.getKeyId()
        pgpKey = pkpkt.makePgpKey()
        for sig in self.iterSelfSignatures():
            sio = util.ExtendedStringIO()
            pkpkt.write(sio)
            try:
                sig._finalizeSelfSig(sio, pgpKey)
            except BadSelfSignature:
                raise BadSelfSignature(keyId)
        for uid in self.iterUserIds():
            for sig in uid.iterKeySignatures(keyId):
                sio = util.ExtendedStringIO()
                pkpkt.write(sio)
                uid.writeHash(sio)
                try:
                    sig._finalizeSelfSig(sio, pgpKey)
                except BadSelfSignature:
                    raise BadSelfSignature(keyId)
                # Only verify the first sig on the user ID.
                # XXX Why? No idea yet
                break
            else: # for
                # No signature. Not good, according to our standards
                raise BadSelfSignature(keyId)

        return pkpkt, pgpKey

    def isSupersetOf(self, key):
        """Check if this key is a superset of key
        We try to make sure that:
        - the keys have the same ID
        - this key's set of revocation signatures is a superset of the other
          key's revocations
        - this key's set of subkeys is a superset of the other key's subkeys
        - this key's set of userids is a superset of the other key's userids
        """
        if self.tag != key.tag:
            raise IncompatibleKey("Attempting to compare different key types")
        if self.getKeyId() != key.getKeyId():
            raise IncompatibleKey("Attempting to compare different keys")

        thisSubkeyIds = dict((x.getKeyId(), x) for x in self.iterSubKeys())
        otherSubkeyIds = dict((x.getKeyId(), x) for x in key.iterSubKeys())
        if not set(thisSubkeyIds).issuperset(otherSubkeyIds):
            # Missing subkey
            return False

        thisUids = dict((x.id, x) for x in self.iterUserIds())
        otherUids = dict((x.id, x) for x in key.iterUserIds())
        if not set(thisUids).issuperset(otherUids):
            # Missing uid
            return False

        thisRevSigs = self._hashSet(x.getBodyStream() for x in self.revsigs)
        otherRevSigs = self._hashSet(x.getBodyStream() for x in key.revsigs)
        if not thisRevSigs.issuperset(otherRevSigs):
            # Missing revocation signature
            return False

        # XXX More work to be done here, we would have to verify that
        # signatures don't change. This is what the old code was doing (and it
        # wasn't actually verifying user ids either ) -- misa
        return True

    def getUserIds(self):
        return [ pkt.id for pkt in self.iterUserIds() ]

class PGP_PublicAnyKey(PGP_Key):
    pubTag = None
    def toPublicKey(self, minHeaderLen = 2):
        return newPacket(self.pubTag, self._bodyStream,
                         minHeaderLen = minHeaderLen)

class PGP_PublicKey(PGP_PublicAnyKey, PGP_MainKey):
    tag = PKT_PUBLIC_KEY
    pubTag = PKT_PUBLIC_KEY

class PGP_SecretAnyKey(PGP_Key):
    __slots__ = ['s2k', 'symmEncAlg', 's2kType', 'hashAlg', 'salt',
                 'count', 'initialVector', 'encMpiFile']
    pubTag = None

    _hashes = [ 'Unknown', md5, sha, RIPEMD, 'Double Width SHA',
                'MD2', 'Tiger/192', 'HAVAL-5-160' ]
    # Ciphers and their associated key sizes
    _ciphers = [ ('Unknown', 0), ('IDEA', 0), (DES3, 192), (CAST, 128),
                 (Blowfish, 128), ('SAFER-SK128', 0), ('DES/SK', 0),
                 (AES, 128), (AES, 192), (AES, 256), ]
    _legalCiphers = set([ 2, 3, 4, 7, 8, 9 ])

    def validate(self):
        PGP_Key.validate(self)
        self.s2k = self.symmEncAlg = self.s2kType = None
        self.hashAlg = self.salt = self.count = None
        self.initialVector = self.encMpiFile = None

    def parse(self):
        PGP_Key.parse(self)

        # Seek to the end of the MPI file, just to be safe (we should be there
        # already)
        self._bodyStream.seek(self.mpiFile.start + self.mpiLen, SEEK_SET)

        self.s2k, = self.readBin(1)

        if self.s2k in [ENCRYPTION_TYPE_SHA1_CHECK,
                        ENCRYPTION_TYPE_S2K_SPECIFIED]:
            self.symmEncAlg, self.s2kType, self.hashAlg = self.readBin(3)
            if self.s2kType:
                if self.s2kType not in (0x01, 0x03):
                    raise IncompatibleKey('Unknown string-to-key type %s' %
                                          self.s2kType)
                self.salt = self.readExact(8)
                if self.s2kType == 0x03:
                    self.count, = self.readBin(1)
        # The MPIs are most likely encrypted, we'll just have to trust that
        # there are enough of them for now.
        dataLen = self._bodyStream.size - self._bodyStream.tell()
        self.encMpiFile = util.SeekableNestedFile(self._bodyStream, dataLen)

    def _getSecretMPICount(self):
        if self.pubKeyAlg in PK_ALGO_ALL_RSA:
            return 4
        if self.pubKeyAlg == PK_ALGO_DSA:
            return 1
        if self.pubKeyAlg in PK_ALGO_ALL_ELGAMAL:
            return 1
        raise PGPError("Unsupported public key algorithm %s" % self.pubKeyAlg)

    def toPublicKey(self, minHeaderLen = 2):
        if not self._parsed:
            self.parse()

        # Create a nested file starting at the beginning of the body's and
        # with the length equal to the position in the body up to the MPIs
        io = util.SeekableNestedFile(self._bodyStream,
            self.mpiFile.start + self.mpiLen, start = 0)
        pkt = newPacket(self.pubTag, io, minHeaderLen = minHeaderLen)
        return pkt

    def decrypt(self, passPhrase):
        if not self._parsed:
            self.parse()
        self.encMpiFile.seek(0, SEEK_SET)

        if self.s2k == ENCRYPTION_TYPE_UNENCRYPTED:
            return self._readCountMPIs(self.encMpiFile,
                self._getSecretMPICount(), discard = False)

        if self.symmEncAlg not in self._legalCiphers:
            if self.symmetricEngAlg >= len(self._ciphers):
                raise IncompatibleKey("Unknown cipher %s" %
                                      self.symmetricEngAlg)
            
            cipher, cipherKeySize = self._ciphers[self.symmEncAlg]
            raise IncompatibleKey("Cipher %s is unusable" % cipher)

        if self.hashAlg >= len(self._hashes):
            raise IncompatibleKey("Unknown hash algorithm %s" % self.hashAlg)
        hashAlg = self._hashes[self.hashAlg]
        if isinstance(hashAlg, str):
            raise IncompatibleKey('Hash algorithm %s is not implemented. '
                                  'Key not readable' % hashAlg)

        cipherAlg, cipherKeySize = self._ciphers[self.symmEncAlg]
        if self.s2kType == 0x00:
            key = simpleS2K(passPhrase, hashAlg, cipherKeySize)
        elif self.s2kType == 0x01:
            key = saltedS2K(passPhrase, hashAlg, cipherKeySize, self.salt)
        elif self.s2kType == 0x03:
            key = iteratedS2K(passPhrase, hashAlg, cipherKeySize, self.salt,
                              self.count)
        # Dark magic here --misa
        if self.symmEncAlg > 6:
            cipherBlockSize = 16
        else:
            cipherBlockSize = 8

        io = util.ExtendedStringIO()
        cipher = cipherAlg.new(key,1)
        block = self._readExact(self.encMpiFile, cipherBlockSize)
        FRE = cipher.encrypt(block)
        while 1:
            block = self.encMpiFile.read(cipherBlockSize)
            io.write(xorStr(FRE, block))
            if len(block) != cipherBlockSize:
                break
            FRE = cipher.encrypt(block)
        unenc = io.getvalue()
        if self.s2k == ENCRYPTION_TYPE_S2K_SPECIFIED:
            check = verifyRFC2440Checksum(unenc)
        else:
            check = verifySHAChecksum(unenc)

        if not check:
            raise BadPassPhrase('Pass phrase incorrect')

        io.seek(0)
        return self._readCountMPIs(io, self._getSecretMPICount(),
                                   discard = False)

    def makePgpKey(self, passPhrase = None):
        assert passPhrase is not None
        # Secret keys have to be signing keys
        self.assertSigningKey()
        pkTuple = self.getPublicKeyTuple()
        secMPIs = self.decrypt(passPhrase)
        if self.pubKeyAlg in PK_ALGO_ALL_RSA:
            n, e = pkTuple
            d, p, q, u = secMPIs
            return RSA.construct((n, e, d, p, q, u))
        if self.pubKeyAlg == PK_ALGO_DSA:
            p, q, g, y = pkTuple
            x, = secMPIs
            return DSA.construct((y, g, p, q, x))
        raise MalformedKeyRing("Can't use El-Gamal keys in current version")

class PGP_SecretKey(PGP_SecretAnyKey, PGP_MainKey):
    tag = PKT_SECRET_KEY
    pubTag = PKT_PUBLIC_KEY

class PGP_SubKey(PGP_Key):
    # Subkeys are promoted to main keys when converted to public keys
    pubTag = PKT_PUBLIC_KEY

    def validate(self):
        PGP_Key.validate(self)
        self.mainKey = None
        self.bindingSig = None
        self.bindingSigRevoc = None

    def iterSubPackets(self):
        # Stop at another key
        if self.bindingSig:
            yield self.bindingSig
        if self.bindingSigRevoc:
            yield self.bindingSigRevoc

    def iterUserIds(self):
        # Subkeys don't have user ids
        return []

    def iterSelfSignatures(self):
        return self._iterSelfSignatures(self.getMainKey().getKeyId())

    def iterAllSelfSignatures(self):
        """Iterate over direct signatures and UserId signatures"""
        return self._iterAllSelfSignatures(self.getMainKey().getKeyId())

    def getMainKey(self):
        """Return the main key for this subkey"""
        return self.mainKey

    def verifySelfSignatures(self):
        # seek the main key associated with this subkey
        mainKey = self.getMainKey()
        # since this is a subkey, let's go ahead and make sure the
        # main key is valid before we continue
        mainpkpkt, mainPgpKey = mainKey.verifySelfSignatures()

        # Convert this subkey to a public key
        pkpkt = self.toPublicKey(minHeaderLen = 3)

        # Only verify direct signatures
        keyId = pkpkt.getKeyId()
        for sig in self.iterSelfSignatures():
            # There should be exactly one signature, according to
            # RFC 2440 11.1
            sio = util.ExtendedStringIO()
            mainpkpkt.write(sio)
            pkpkt.write(sio)
            try:
                sig._finalizeSelfSig(sio, mainPgpKey)
            except BadSelfSignature:
                raise BadSelfSignature(keyId)
            # Stop after the first sig
            break
        else: # for
            # No signatures on the subkey
            raise BadSelfSignature(keyId)

    def iterSubKeys(self):
        # Nothing to iterate over, subkeys don't have subkeys
        return []

    def iterSignatures(self):
        for pkt in self.iterSubPackets():
            yield pkt


class PGP_PublicSubKey(PGP_SubKey, PGP_PublicAnyKey):
    __slots__ = []
    tag = PKT_PUBLIC_SUBKEY

class PGP_SecretSubKey(PGP_SubKey, PGP_SecretAnyKey):
    __slots__ = []
    tag = PKT_SECRET_SUBKEY

# Register class processors
for klass in [PGP_PublicKey, PGP_SecretKey, PGP_PublicSubKey, PGP_SecretSubKey]:
    PacketTypeDispatcher.addPacketType(klass)

def newPacket(tag, bodyStream, newStyle = False, minHeaderLen = 2):
    """Create a new Packet"""
    klass = PacketTypeDispatcher.getClass(tag)
    pkt = klass(bodyStream, newStyle = newStyle, minHeaderLen = minHeaderLen)
    if not hasattr(pkt, 'tag'): # No special class for this packet
        pkt.setTag(tag)
    return pkt

def newPacketFromStream(stream, start = -1):
    if isinstance(stream, file) and not hasattr(stream, "pread"):
        # Try to reopen as an ExtendedFile
        f = util.ExtendedFile(stream.name, buffering = False)
        f.seek(stream.tell())
        stream = f
    return PGP_PacketFromStream().read(stream, start = start)

def newKeyFromString(data):
    """Create a new (main) key from the data
    Returns None if a key was not found"""
    return newKeyFromStream(util.ExtendedStringIO(data))

def newKeyFromStream(stream):
    """Create a new (main) key from the stream
    Returns None if a key was not found"""
    pkt = newPacketFromStream(stream)
    if pkt is None:
        return None
    if not isinstance(pkt, PGP_MainKey):
        return None
    pkt.initSubPackets()
    return pkt


def len2bytes(v1, v2):
    """Return the packet body length when represented on 2 bytes"""
    return ((v1 - 192) << 8) + v2 + 192

def len4bytes(v1, v2, v3, v4):
    """Return the packet body length when represented on 4 bytes"""
    return (v1 << 24) | (v2 << 16) | (v3 << 8) | v4

def int2bytes(v1, v2):
    return (v1 << 8) + v2

def int4bytes(v1, v2, v3, v4):
    return len4bytes(v1, v2, v3, v4)
