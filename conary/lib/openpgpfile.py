#
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


def readBlockType(keyRing):
    r=keyRing.read(1)
    if r != '':
        return ord(r)
    else:
        return -1

def convertPrivateKey(privateBlock):
    # converts a private key into a public one, used for generating
    # keyids
    if not privateBlock:
        # give us nothing, you get nothing from us
        return ''

    packetType = ord(privateBlock[0])
    if not (packetType & 128):
        raise MalformedKeyRing("Not an OpenPGP packet.")
    if packetType & 64:
        return ''
    if ((packetType >> 2) & 15) in PKT_ALL_PUBLIC:
        # if it's already a public key, there's nothing to do
        return privateBlock
    if ((packetType >> 2) & 15) not in PKT_ALL_SECRET:
        # other types of packets aren't secret, so return empty string
        return ''
    blockSize=0

    # form up a cipher type byte (CTB). See RFC 1991 - 4.1
    # the high 2 bits are 10b (binary) to indicate a normal CTB. So OR
    # 0x80 with the correct packet type shifted left 2 (which happens
    # to correspond to the correct packet type bits) to get the CTB
    # (with no size set yet, that will come later)
    if ((packetType >> 2) & 15) == PKT_SECRET_KEY:
        ctb = 0x80 | (PKT_PUBLIC_KEY & 0xf) << 2
    elif ((packetType >> 2) & 15) == PKT_SECRET_SUBKEY:
        ctb = 0x80 | (PKT_PUBLIC_SUBKEY & 0xf) << 2
    else:
        assert(0)

    # figure out how much of the size we need to skip
    if (packetType & 3) == OLD_PKT_LEN_ONE_OCTET:
        index = 2
    elif (packetType & 3) == OLD_PKT_LEN_TWO_OCTET:
        index = 3
    elif (packetType & 3) == OLD_PKT_LEN_FOUR_OCTET:
        index = 5
    else:
        raise MalformedKeyRing("Packet of indeterminate size.")

    # RFC 2440, sect. 5.5.2
    # check the packet version, we only handle version 4
    if ord(privateBlock[index]) != 4:
        return ''

    # get the key data
    buf = privateBlock[index:index + 6]
    index += 5

    # get the algorithm type
    algType = ord(privateBlock[index])
    index += 1
    if algType in PK_ALGO_ALL_RSA:
        numMPI = 2
    elif algType in PK_ALGO_ALL_ELGAMAL:
        numMPI = 3
    elif algType == PK_ALGO_DSA:
        numMPI = 4
    else:
        # unhandled algorithm
        return ''

    # parse the MPIs from the key block
    for i in range(0, numMPI):
        mLen = ((ord(privateBlock[index]) * 256 +
                 ord(privateBlock[index + 1])) + 7) // 8 + 2
        buf = buf + privateBlock[index:index + mLen]
        index += mLen

    # calculate the new key length, record the size in the ctb
    # see RFC 1991 for the low end bit definitions
    bufLen = len(buf)
    if bufLen > 65535:
        # 4-byte packet-length field
        ctb |= 2
        sizeBytes = 4
    elif bufLen > 255:
        # 2-byte packet length field
        ctb |= 1
        sizeBytes = 2
    else:
        # 1 byte packet-length field (no changes to ctb needed)
        sizeBytes = 1

    # prepare the size octets
    sizeBuf=''
    for i in range(1, sizeBytes + 1):
        sizeBuf += chr((bufLen >> ((sizeBytes - i) << 3)) & 0xff)

    # complete the new key packet
    return (chr(ctb) + sizeBuf + buf)

#turn the current key into a form usable for keyId's and self signatures
def getHashableKeyData(keyRing):
    # see RFC 2440 11.2 - Key IDs and Fingerprints
    keyPoint = keyRing.tell()
    keyBlock=readBlockType(keyRing)
    if not (keyBlock & 128):
        raise MalformedKeyRing("Not an OpenPGP packet.")
    keyRing.seek(keyPoint)
    if ((keyBlock == -1) or (keyBlock & 64)
        or (((keyBlock >> 2) & 15) not in PKT_ALL_KEYS)):
        return ''
    seekNextPacket(keyRing)
    dataSize = keyRing.tell() - keyPoint
    keyRing.seek(keyPoint)
    data = keyRing.read(dataSize)
    keyRing.seek(keyPoint)
    # convert private keys to a public key
    if ((keyBlock >> 2) & 15) in PKT_ALL_SECRET:
        data = convertPrivateKey(data)
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
    keyBlock = ord(data[0])
    # Translate 1 byte length blocks to two byte length blocks
    if not (keyBlock & 1):
        data = chr(keyBlock|1) + chr(0) + data[1:]
    # promote subkeys to main keys
    # 0x99 is the ctb for a public key packet with two byte length
    data = chr(0x99) + data[1:]
    return data

def getKeyId(keyRing):
    pkt = newPacketFromStream(keyRing, start = -1)
    assert pkt is not None
    return pkt.getKeyId()

def getSignatureTuple(keyRing):
    startPoint = keyRing.tell()
    blockType = readBlockType(keyRing)
    #reading the block size skips the length octets
    readBlockSize(keyRing, blockType)
    if ord(keyRing.read(1)) != 4:
        raise IncompatibleKey("Must be a V4 signature")
    keyRing.seek(1, SEEK_CUR)
    algType = ord(keyRing.read(1))
    keyRing.seek(1, SEEK_CUR)
    for repeat in range(2):
        hashLen = ord(keyRing.read(1)) * 256 + ord(keyRing.read(1))
        keyRing.seek(hashLen, SEEK_CUR)
    keyRing.seek(2, SEEK_CUR)
    #now we're pointing at the MPIs
    if algType in PK_ALGO_ALL_RSA:
        numMPI = 1
    elif algType in PK_ALGO_ALL_ELGAMAL:
        numMPI = 2
    elif algType == PK_ALGO_DSA:
        numMPI = 2
    else:
        raise IncompatibleKey("Signature is signed by an algorithm we don't know")
    mpiList = []
    for i in range(numMPI):
        mpiList.append(readMPI(keyRing))
    keyRing.seek(startPoint)
    return tuple(mpiList)


def finalizeSelfSig(data, keyRing, fingerprint, mainKey):
    # XXX FIXME misa next to convert
    # find the self signature
    intKeyId = fingerprintToInternalKeyId(fingerprint)
    while (intKeyId != getSigId(keyRing)):
        seekNextSignature(keyRing)
    # we now point to the self signature.
    # get the actual signature Tuple
    dig_sig = getSignatureTuple(keyRing)
    # append the hashable portion of the self signature
    # and record what kind of hash alorithm to use while we're at it.
    hashBlock = readBlockType(keyRing)
    # reading the block size skips the length octets
    readBlockSize(keyRing, hashBlock)
    hashData = keyRing.read(6)
    if ord(hashData[0]) != 4:
        raise InvalidKey('Self signature is not a V4 signature')
    pubAlg = ord(hashData[2])
    hashAlg = ord(hashData[3])
    hashLen = ord(hashData[4]) * 256 + ord(hashData[5])
    hashData += keyRing.read(hashLen)
    data += hashData
    # then append the trailer
    dataLen = len(hashData)
    data += chr(4) + chr(0xFF) + chr((dataLen // 0x1000000) & 0xFF) + \
            chr((dataLen // 0x10000) & 0xFF) + \
            chr((dataLen // 0x100) & 0xFF) + chr(dataLen & 0xFF)
    hashAlgList = [ None, md5, sha]
    hashFunc = hashAlgList[hashAlg]
    hashFunc = hashFunc.new()
    hashFunc.update(data)
    sigString = hashFunc.digest()
    # if this is an RSA signature, it needs to properly padded
    # RFC 2440 5.2.2 and RFC 2313 10.1.2
    if pubAlg in PK_ALGO_ALL_RSA:
        #hashPads from RFC2440 section 5.2.2
        hashPads = [ '', '\x000 0\x0c\x06\x08*\x86H\x86\xf7\r\x02\x05\x05\x00\x04\x10', '\x000!0\t\x06\x05+\x0e\x03\x02\x1a\x05\x00\x04\x14' ]
        padLen = (len(hex(mainKey.n)) - 5 - 2 * (len(sigString) + len(hashPads[hashAlg]))) // 2 -1
        sigString = chr(1) + chr(0xFF) * padLen + hashPads[hashAlg] + sigString
    if not mainKey.verify(sigString, dig_sig):
        raise BadSelfSignature("Key: %s failed self signature check"% fingerprint)

def seekKeyById(keyId, keyRing):
    pass

def seekParentKey(keyId, keyRing):
    seekKeyById(keyId, keyRing)
    blockType = readBlockType(keyRing)
    if blockType != -1:
        keyRing.seek(-1, SEEK_CUR)
    if (blockType >> 2) & 15 in PKT_MAIN_KEYS:
        # key in question is a main key, no need to seek
        return
    limit = keyRing.tell()
    keyRing.seek(0)
    mainKeyPoint = 0
    while keyRing.tell() < limit:
        blockType = readBlockType(keyRing)
        if blockType != -1:
            keyRing.seek(-1, SEEK_CUR)
        if (blockType >> 2) & 15 in PKT_MAIN_KEYS:
            mainKeyPoint = keyRing.tell()
        seekNextKey(keyRing)
    keyRing.seek(mainKeyPoint)

# parse self signatures to find timestamp(s) of key expiration.
# also seek out any revocation timestamps.
# we don't need to actually verify these signatures. see verifySelfSignatures()
def findEndOfLife(keyId, keyRing):
    parentRevoked = False
    parentExpire = 0
    expireTimestamp = 0
    revocTimestamp = 0
    startPoint = keyRing.tell()
    seekKeyById(keyId, keyRing)
    keyPoint = keyRing.tell()
    fingerprint = getKeyId(keyRing)
    intKeyId = fingerprintToInternalKeyId(fingerprint)
    blockType = readBlockType(keyRing)
    if blockType != -1:
        keyRing.seek(-1, SEEK_CUR)
    if (blockType >> 2) & 15 in PKT_SUB_KEYS:
        seekParentKey(keyId, keyRing)
        fingerprint = getKeyId(keyRing)
        parentRevoked, parentExpire = findEndOfLife(fingerprint, keyRing)
        intKeyId = fingerprintToInternalKeyId(fingerprint)
    seekNextKey(keyRing)
    limit = keyRing.tell()
    keyRing.seek(keyPoint)
    seekNextSignature(keyRing)
    while (keyRing.tell() < limit):
        while (keyRing.tell() < limit) and (intKeyId != getSigId(keyRing)):
            seekNextSignature(keyRing)
        if keyRing.tell() >= limit:
            break
        sigPoint = keyRing.tell()
        #we found a self signature, parse it for the info we want
        blockType = readBlockType(keyRing)
        # reading the block size skips the length octets
        readBlockSize(keyRing, blockType & 3)
        if (ord(keyRing.read(1)) != 4):
            raise IncompatibleKey("Not a V4 signature")
        sigType = ord(keyRing.read(1))
        #skip ahead to the hashed subpackets
        keyRing.seek(2, SEEK_CUR)
        subLim = ord(keyRing.read(1)) * 256 + ord(keyRing.read(1)) + keyRing.tell()
        # if the self signature is a cert or revocation we care.
        # other self signatures are of no use.
        if sigType in SIG_CERTS:
            # parse this self cert to see if key expires.
            # do not assume packet will even be present!
            # we're ultimately looking for the least stringent expiration
            eTimestamp = 0
            cTimestamp = 0
            while keyRing.tell() < subLim:
                subLen = ord(keyRing.read(1))
                subType = ord(keyRing.read(1))
                if subType == SIG_SUBPKT_KEY_EXPIRE:
                    eTimestamp = 0
                    for i in range(subLen-1):
                        eTimestamp = eTimestamp * 256 + ord(keyRing.read(1))
                    keyRing.seek(-1 * (subLen - 1), SEEK_CUR)
                elif subType == SIG_SUBPKT_CREATION:
                    cTimestamp = 0
                    for i in range(subLen-1):
                        cTimestamp = cTimestamp * 256 + ord(keyRing.read(1))
                    keyRing.seek(-1 * (subLen - 1), SEEK_CUR)
                keyRing.seek(subLen - 1, SEEK_CUR)
            # if there's no expiration, DON'T COMPUTE this, otherwise
            # it will appear as if the key expired the very moment
            # it was created.
            if eTimestamp:
                timestamp = eTimestamp + cTimestamp
                expireTimestamp = max(expireTimestamp, timestamp)
        elif sigType in SIG_KEY_REVOCS:
            # parse this revocation to look for the creation timestamp
            # we're ultimately looking for the most stringent revocation
            while keyRing.tell() < subLim:
                subLen = ord(keyRing.read(1))
                subType = ord(keyRing.read(1))
                if subType == SIG_SUBPKT_CREATION:
                    timestamp = 0
                    for i in range(subLen-1):
                        timestamp = timestamp * 256 + ord(keyRing.read(1))
                    if revocTimestamp:
                        revocTimestamp = min(expireTimestamp, timestamp)
                    else:
                        revocTimestamp = timestamp
                keyRing.seek(subLen - 1, SEEK_CUR)
        keyRing.seek(sigPoint)
        seekNextSignature(keyRing)
    keyRing.seek(startPoint)
    # return minimum non-zero value of the three expirations
    # unless they're ALL zero. 8-)
    if not (revocTimestamp or expireTimestamp or parentExpire):
        return False, 0
    # make no assumptions about how big a timestamp is.
    timestamp = max(revocTimestamp, expireTimestamp, parentExpire)
    if revocTimestamp:
        timestamp = min(timestamp, revocTimestamp)
    if expireTimestamp:
        timestamp = min(timestamp, expireTimestamp)
    if parentExpire:
        timestamp = min(timestamp, parentExpire)
    return (revocTimestamp != 0) and (not parentRevoked), timestamp
    
# it might seem counterproductive to re-create the key within this function,
# but alas, we don't always need the key associated with the keyId we're
# trying to verify (think subkeys)
# if you play with this chunk of code be careful to not use high-level
# functions lest you cause inadverdent recursion.
def verifySelfSignatures(keyId, keyRing):
    #seek to key in question.
    startPoint = keyRing.tell()
    keyRing.seek(0, SEEK_END)
    limit = keyRing.tell()
    keyRing.seek(0)
    while (keyId not in getKeyId(keyRing)) and keyRing.tell() < limit:
        seekNextKey(keyRing)
    if keyRing.tell() == limit:
        raise KeyNotFound(keyId)
    # get the key type
    blockType = readBlockType(keyRing)
    #for this case we need to point to the beginning of the key
    if blockType != -1:
        keyRing.seek(-1, SEEK_CUR)
    # main keys and sub keys get hashed differently:
    if ((blockType >> 2) & 15) in PKT_MAIN_KEYS:
        # we'll be verifying a userid certification signature
        # create an instance of the main key
        fingerprint = getKeyId(keyRing)
        mainKey = makeKey(getGPGKeyTuple(keyId,keyRing))
        mainKeyData = getHashableKeyData(keyRing)
        #find the next key, so we can loop thru all the UserIDs
        mainKeyPoint = keyRing.tell()
        seekNextKey(keyRing)
        limit = keyRing.tell()
        keyRing.seek(mainKeyPoint)
        # before we go to UIDs and signatures, check all direct key signatures
        # find first non-sig packet. that will be the limit
        seekNextPacket(keyRing)
        sigStart = keyRing.tell()
        packetType = readBlockType(keyRing)
        if packetType != -1:
            keyRing.seek(-1, SEEK_CUR)
        while (packetType >> 2) & 15 == PKT_SIG:
            seekNextPacket(keyRing)
            packetType = readBlockType(keyRing)
            if packetType != -1:
                keyRing.seek(-1, SEEK_CUR)
        limit = keyRing.tell()
        keyRing.seek(sigStart)
        intKeyId = fingerprintToInternalKeyId(fingerprint)
        while keyRing.tell() < limit:
            if intKeyId == getSigId(keyRing):
                sigPoint = keyRing.tell()
                try:
                    finalizeSelfSig(mainKeyData, keyRing, fingerprint, mainKey)
                except:
                    keyRing.seek(startPoint)
                    raise
                keyRing.seek(sigPoint)
            seekNextPacket(keyRing)
        keyRing.seek(mainKeyPoint)
        numUids = 0
        # FIXME: this while loop is too delicate. do not assume there is a
        # self signed signature for the userid packet.
        while keyRing.tell() < limit:
            # find the next userId packet
            data = mainKeyData
            packetType = readBlockType(keyRing)
            if packetType != -1:
                keyRing.seek(-1, SEEK_CUR)
            while (((packetType >> 2) & 15) != PKT_USERID) and (keyRing.tell() < limit):
                seekNextPacket(keyRing)
                packetType = readBlockType(keyRing)
                if packetType != -1:
                    keyRing.seek(-1, SEEK_CUR)
            if ((packetType >> 2) & 15) != PKT_USERID:
                break
            else:
                numUids += 1
            # append the primary userId
            userIdStart = keyRing.tell()
            seekNextPacket(keyRing)
            userIdLen = keyRing.tell() - userIdStart
            keyRing.seek(userIdStart)
            # reading the entire user info block puts us at the first signature
            userData = keyRing.read(userIdLen)
            # described in RFC 2440 5.2.4 computing signatures
            # we now need to mangle the userData to be V4 sig compliant
            lenType = ord(userData[0]) & 3
            if lenType == 3:
                keyRing.seek(startPoint)
                raise MalformedKeyRing("Can't read packet of indeterminate length")
            if lenType == 2:
                userData = chr(0xB4) + userData[1:]
            elif lenType == 1:
                userData = chr(0xB4) + chr(0)*2 + userData[1:]
            else:
                userData = chr(0xB4) + chr(0)*3 +userData[1:]
            data += userData
            try:
                finalizeSelfSig(data, keyRing, fingerprint, mainKey)
            except:
                keyRing.seek(startPoint)
                raise
            #seek the next packet after the UID we just comptued
            keyRing.seek(userIdStart)
            seekNextPacket(keyRing)
        if not numUids:
            raise MalformedKeyRing('Key %s has no user ids' %fingerprint)
    else:
        # the key was a subkey: we'll be verifying a subkey binding signature
        # record where we are because we need to come back to this point
        subKeyPoint = keyRing.tell()
        # seek the main key associated with this subkey
        keyRing.seek(0)
        mainKeyPoint = 0
        while keyRing.tell() < subKeyPoint:
            seekNextKey(keyRing)
            blockType = readBlockType(keyRing)
            if blockType != -1:
                keyRing.seek(-1, SEEK_CUR)
            if (blockType>>2)&15 in PKT_MAIN_KEYS:
                mainKeyPoint = keyRing.tell()
        keyRing.seek(mainKeyPoint)
        fingerprint = getKeyId(keyRing)
        # since this is a subkey, let's go ahead and make sure the
        # main key is valid before we continue
        verifySelfSignatures(fingerprint, keyRing)
        # create an instance of the main key
        mainKey = makeKey(getGPGKeyTuple(fingerprint,keyRing))
        # use gethashableKeyData to get main key. set data to that
        data = getHashableKeyData(keyRing)
        # seek back to the subkey and use getHashableKeyData to get subkey
        keyRing.seek(subKeyPoint)
        data += getHashableKeyData(keyRing)
        seekNextSignature(keyRing)
        sigPoint = keyRing.tell()
        seekNextKey(keyRing)
        limit = keyRing.tell()
        keyRing.seek(sigPoint)
        #make no assumptions about how many signatures follow subkey
        intKeyId = fingerprintToInternalKeyId(fingerprint)
        while keyRing.tell() < limit:
            if intKeyId == getSigId(keyRing):
                sigPoint = keyRing.tell()
                try:
                    finalizeSelfSig(data, keyRing, fingerprint, mainKey)
                except:
                    keyRing.seek(startPoint)
                    raise
                keyRing.seek(sigPoint)
            seekNextSignature(keyRing)
    keyRing.seek(startPoint)

# find the next OpenPGP packet regardless of type.
def seekNextPacket(keyRing):
    packetType=readBlockType(keyRing)
    dataSize = readBlockSize(keyRing, packetType)
    keyRing.seek(dataSize, SEEK_CUR)

def seekNextKey(keyRing):
    done = 0
    while not done:
        seekNextPacket(keyRing)
        packetType = readBlockType(keyRing)
        if packetType != -1:
            keyRing.seek(-1, SEEK_CUR)
        if ((packetType == -1)
            or ((not (packetType & 64))
                and (((packetType >> 2) & 15) in PKT_ALL_KEYS))):
            done = 1

def seekNextSignature(keyRing):
    done = 0
    while not done:
        seekNextPacket(keyRing)
        packetType = readBlockType(keyRing)
        if packetType != -1:
            keyRing.seek(-1,SEEK_CUR)
        if ((packetType == -1)
            or ((not (packetType&64))
                and (((packetType >> 2) & 15) == PKT_SIG))):
            done = 1

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

def getSigId(keyRing):
    startPoint = keyRing.tell()
    blockType = readBlockType(keyRing)
    if (blockType >> 2) & 15 != PKT_SIG:
        #block is not a signature. it has no sigId
        keyRing.seek(startPoint, SEEK_SET)
        return ''
    readBlockSize(keyRing, blockType)
    assert (ord(keyRing.read(1)) == 4)
    keyRing.seek(3, SEEK_CUR)
    hashedLen = ord(keyRing.read(1)) * 256 + ord(keyRing.read(1))
    # hashedLen plus two to skip len of unhashed data.
    keyRing.seek(hashedLen + 2, SEEK_CUR)
    done = 0
    while not done:
        subLen = ord(keyRing.read(1))
        if ord(keyRing.read(1)) == 16:
            done = 1
        else:
            keyRing.seek(subLen - 1, SEEK_CUR)
    data = keyRing.read(subLen - 1)
    keyRing.seek(startPoint)
    return data

def assertSigningKey(keyId,keyRing):
    startPoint = keyRing.tell()
    keyRing.seek(0, SEEK_END)
    limit = keyRing.tell()
    if limit == 0:
        # no keys in a zero length file
        keyRing.seek(startPoint)
        raise KeyNotFound(keyId, "Couldn't open keyring")
    keyRing.seek(0, SEEK_SET)
    while (keyRing.tell() < limit) and (keyId not in getKeyId(keyRing)):
        seekNextKey(keyRing)
    if keyRing.tell() >= limit:
        keyRing.seek(startPoint)
        raise KeyNotFound(keyId)
    # keyring now points to the beginning of the key we wanted
    # find self signature of this key
    keyStart = keyRing.tell()
    seekNextKey(keyRing)
    keyLim = keyRing.tell()
    keyRing.seek(keyStart)
    fingerprint = getKeyId(keyRing)
    intKeyId = fingerprintToInternalKeyId(fingerprint)
    # first search for the public key algortihm octet. if the key is really
    # old, this might be the only hint that it's legal to use this key to
    # make digital signatures.
    blockType = readBlockType(keyRing)
    readBlockSize(keyRing, blockType)
    if (ord(keyRing.read(1)) != 4):
        raise IncompatibleKey("Can only use V4 keys")
    keyRing.seek(4, SEEK_CUR)
    if ord(keyRing.read(1)) in (PK_ALGO_RSA_SIGN_ONLY, PK_ALGO_DSA):
        # the public key algorithm octet satisfies this test. no more checks required
        keyRing.seek(startPoint)
        return
    # now, if the key we are looking at is a subkey, then we need to go
    # back and find the keyId of the parent key, since we'll need that
    # to find the subkey binding signature
    if (blockType >> 2) & 15 in PKT_SUB_KEYS:
        keyRing.seek(0)
        mainKeyStart = 0
        while keyRing.tell() != keyStart:
            mainBlock = readBlockType(keyRing)
            if mainBlock != -1:
                keyRing.seek(-1, SEEK_CUR)
            if ((mainBlock >> 2) & 15) in PKT_MAIN_KEYS:
                mainKeyStart = keyRing.tell()
            seekNextKey(keyRing)
        keyRing.seek(mainKeyStart)
        fingerprint = getKeyId(keyRing)
        intKeyId = fingerprintToInternalKeyId(fingerprint)
    # return to beginning of key so we can skip chunks.
    keyRing.seek(keyStart)
    keyFlagsFound = 0
    while (not keyFlagsFound) and (keyRing.tell() < keyLim):
        seekNextSignature(keyRing)
        while (intKeyId != getSigId(keyRing)):
            seekNextSignature(keyRing)
        # we now point to the self signature. now find the Key Flags subpacket
        # remember where we are in case we didn't find what we need.
        sigStart = keyRing.tell()
        blockType = readBlockType(keyRing)
        readBlockSize(keyRing, blockType)
        if (ord(keyRing.read(1)) != 4):
            raise IncompatibleKey("Can only use V4 keys")
        keyRing.seek(3, SEEK_CUR)
        subLim = ord(keyRing.read(1)) * 256 + ord(keyRing.read(1)) + keyRing.tell()
        done = 0
        while (not done) and (keyRing.tell() < subLim):
            subLen = ord(keyRing.read(1))
            subType = ord(keyRing.read(1))
            if (subType != 27):
                keyRing.seek(subLen - 1, SEEK_CUR)
            else:
                keyFlagsFound = 1
                done = 1
        if not keyFlagsFound:
            keyRing.seek(sigStart)
    if not keyFlagsFound:
        keyRing.seek(startPoint)
        raise IncompatibleKey("Key %s has no key flags block. Can't determine suitabilty for use as a signature key"% fingerprint)
    Flags = ord(keyRing.read(1))
    keyRing.seek(startPoint)
    if not (Flags & 2):
        raise IncompatibleKey('Key %s is not a signing key.'% fingerprint)

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

def readMPI(keyRing):
    MPIlen=(ord(keyRing.read(1)) * 256 + ord(keyRing.read(1)) + 7 ) // 8
    r=0L
    for i in range(MPIlen):
        r = r * 256 + ord(keyRing.read(1))
    return r

def readBlockSize(keyRing, packetType):
    if packetType == -1:
        return 0
    # check if packet is old or new style
    dataSize = -1
    if not packetType & 64:
        # RFC 2440 4.2.1 - Old-Format Packet Lengths
        if (packetType & 3) == OLD_PKT_LEN_ONE_OCTET:
            sizeLen = 1
        elif (packetType & 3) == OLD_PKT_LEN_TWO_OCTET:
            sizeLen = 2
        elif (packetType & 3) == OLD_PKT_LEN_FOUR_OCTET:
            sizeLen = 4
        else:
            raise MalformedKeyRing("Can't get size of packet of indeterminate length.")
    else:
        # RFC 2440 4.2.2 - New-Format Packet Lengths
        octet=ord(keyRing.read(1))
        if octet < 192:
            sizeLen=1
            keyRing.seek(-1, SEEK_CUR)
        elif octet < 224:
            dataSize = (octet - 192 ) * 256 + \
                       ord(keyRing.read(1)) + 192
        elif octet < 255:
            dataSize = 1 << (octet & 0x1f)
        else:
            sizeLen=4
    # if we have not already calculated datasize, calculate it now
    if dataSize == -1:
        dataSize = 0
        for i in range(0, sizeLen):
            dataSize = (dataSize * 256) + ord(keyRing.read(1))
    return dataSize

def getGPGKeyTuple(keyId, keyRing, secret=0, passPhrase=''):
    startPoint = keyRing.tell()
    keyRing.seek(0, SEEK_END)
    limit = keyRing.tell()
    if limit == 0:
        # empty file, there can be no keys in it
        raise KeyNotFound(keyId)
    if secret:
        assertSigningKey(keyId, keyRing)
    keyRing.seek(0)
    while (keyId not in getKeyId(keyRing)):
        seekNextKey(keyRing)
        if keyRing.tell() == limit:
            raise KeyNotFound(keyId)
    startLoc=keyRing.tell()
    seekNextPacket(keyRing)
    limit = keyRing.tell()
    keyRing.seek(startLoc)
    packetType=ord(keyRing.read(1))
    if secret and (not ((packetType>>2) & 1)):
        raise IncompatibleKey("Can't get private key from public keyring")
    # reading the block size skips the length octets
    readBlockSize(keyRing, packetType)
    if ord(keyRing.read(1)) != 4:
        raise MalformedKeyRing("Can only read V4 packets")
    keyRing.seek(4, SEEK_CUR)
    keyType = ord(keyRing.read(1))
    if keyType in PK_ALGO_ALL_RSA:
        # do RSA stuff
        # n e
        n = readMPI(keyRing)
        e = readMPI(keyRing)
        if secret:
            privateMPIs = decryptPrivateKey(keyRing, limit, 4, passPhrase)
            r = (n, e, privateMPIs[0], privateMPIs[1],
                 privateMPIs[2], privateMPIs[3])
        else:
            r = (n, e)
    elif keyType in (PK_ALGO_DSA,):
        p = readMPI(keyRing)
        q = readMPI(keyRing)
        g = readMPI(keyRing)
        y = readMPI(keyRing)
        if secret:
            privateMPIs=decryptPrivateKey(keyRing, limit, 1, passPhrase)
            r = (y, g, p, q, privateMPIs[0])
        else:
            r = (y, g, p, q)
    elif keyType in PK_ALGO_ALL_ELGAMAL:
        raise MalformedKeyRing("Can't use El-Gamal keys in current version")
        p = readMPI(keyRing)
        g = readMPI(keyRing)
        y = readMPI(keyRing)
        if secret:
            privateMPIs = decryptPrivateKey(keyRing, limit, 1, passPhrase)
            r = (y, g, p, privateMPIs[0])
        else:
            r = (p, g, y)
    else:
        raise MalformedKeyRing("Wrong key type")
    keyRing.seek(startPoint)
    return r

def makeKey(keyTuple):
    # public lengths: rsa=2, dsa=4, elgamal=3
    # private lengths: rsa=6 dsa=5 elgamal=4
    if len(keyTuple) in (2, 6):
        return RSA.construct(keyTuple)
    if len(keyTuple) in (4, 5):
        return DSA.construct(keyTuple)

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
    verifySelfSignatures(keyId, keyRing)
    key = makeKey(getGPGKeyTuple(keyId, keyRing, 0, ''))
    keyRing.close()
    return key

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
    key =  makeKey(getGPGKeyTuple(keyId, keyRing, 1, passPhrase))
    keyRing.close()
    return key

def getPublicKeyFromString(keyId, data):
    keyRing = StringIO(data)
    key = makeKey(getGPGKeyTuple(keyId, keyRing, 0, ''))
    keyRing.close()
    return key

def getKeyEndOfLifeFromString(keyId, data):
    keyRing = util.ExtendedStringIO(data)
    return _getKeyEndOfLife(keyId, keyRing)

def getUserIdsFromString(keyId, data):
    keyRing = StringIO(data)
    seekKeyById(keyId, keyRing)
    startPoint = keyRing.tell()
    seekNextKey(keyRing)
    limit = keyRing.tell()
    keyRing.seek(startPoint)
    r = []
    while keyRing.tell() < limit:
        blockType = readBlockType(keyRing)
        if blockType != -1:
            keyRing.seek(-1, SEEK_CUR)
        if (blockType >> 2) & 15 == PKT_USERID:
            uidStart = keyRing.tell()
            keyRing.seek(1, SEEK_CUR)
            uidLen = readBlockSize(keyRing, blockType & 3)
            r.append(keyRing.read(uidLen))
            keyRing.seek(uidStart)
        seekNextPacket(keyRing)
    keyRing.close()
    return r

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
    checksum = int2bytes(*data[-2:])
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

def decryptPrivateKey(keyRing, limit, numMPIs, passPhrase):
    hashes = ('Unknown', md5, sha, RIPEMD, 'Double Width SHA',
              'MD2', 'Tiger/192', 'HAVAL-5-160')
    ciphers = ('Unknown', 'IDEA', DES3, CAST, Blowfish, 'SAFER-SK128',
               'DES/SK', AES, AES, AES)
    keySizes = (0, 0, 192, 128, 128, 0, 0, 128, 192, 256)
    legalCiphers = (2, 3, 4, 7, 8, 9)

    encryptType = readBlockType(keyRing)

    if encryptType == ENCRYPTION_TYPE_UNENCRYPTED:
        mpiList = []
        for i in range(0,numMPIs):
            mpiList.append(readMPI(keyRing))
        return mpiList

    if encryptType in (ENCRYPTION_TYPE_SHA1_CHECK,
                       ENCRYPTION_TYPE_S2K_SPECIFIED):
        algType=readBlockType(keyRing)
        if algType not in legalCiphers:
            if algType > len(ciphers) - 1:
                algType = 0
            raise IncompatibleKey('Cipher: %s unusable' %ciphers[algType])
        cipherAlg = ciphers[algType]
        s2kType = readBlockType(keyRing)
        hashType = readBlockType(keyRing)
        if hashType > len(hashes) - 1:
            hashType = 0
        hashAlg = hashes[hashType]
        if isinstance(hashAlg, str):
            raise IncompatibleKey('Hash algortihm %s is not implemented. '
                                  'Key not readable' %hashes[hashType])
        # RFC 2440 3.6.1.1
        keySize = keySizes[algType]
        if not s2kType:
            key = simpleS2K(passPhrase, hashAlg, keySize)
        elif s2kType == 1:
            salt = keyRing.read(8)
            key = saltedS2K(passPhrase, hashAlg, keySize, salt)
        elif s2kType == 3:
            salt = keyRing.read(8)
            count = ord(keyRing.read(1))
            key = iteratedS2K(passPhrase,hashAlg, keySize, salt, count)
        data = keyRing.read(limit - keyRing.tell())
        if algType > 6:
            cipherBlockSize = 16
        else:
            cipherBlockSize = 8
        cipher = cipherAlg.new(key,1)
        FR = data[:cipherBlockSize]
        data = data[cipherBlockSize:]
        FRE = cipher.encrypt(FR)
        unenc = xorStr(FRE, data[:cipherBlockSize])
        i = 0
        while i + cipherBlockSize < len(data):
            FR=data[i:i + cipherBlockSize]
            i += cipherBlockSize
            FRE = cipher.encrypt(FR)
            unenc += xorStr(FRE, data[i:i + cipherBlockSize])
        if encryptType == ENCRYPTION_TYPE_S2K_SPECIFIED:
            check = verifyRFC2440Checksum(unenc)
        else:
            check = verifySHAChecksum(unenc)
        if not check:
            raise BadPassPhrase('Pass phrase incorrect')
        data = unenc
        index = 0
        r = []
        for count in range(numMPIs):
            MPIlen = (ord(data[index]) * 256 + ord(data[index+1]) + 7 ) // 8
            index += 2
            MPI = 0L
            for i in range(MPIlen):
                MPI = MPI * 256 + ord(data[index])
                index += 1
            r.append(MPI)
        return r
    raise MalformedKeyRing("Can't decrypt key. unkown string-to-key "
                           "specifier: %i" %encryptType)

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
    origRing = StringIO(origKey)
    newRing = StringIO(newKey)
    fingerprint = getKeyId(origRing)
    if fingerprint != getKeyId(newRing):
        origRing.close()
        newRing.close()
        raise IncompatibleKey("Attempting to replace key %s with a different key is not allowed" %fingerprint)
    origKeyIds = []
    newKeyIds = []
    # make a list of keyIds from the original key
    origRing.seek(0, SEEK_END)
    limit = origRing.tell()
    origRing.seek(0)
    while origRing.tell() < limit:
        origKeyIds.append(getKeyId(origRing))
        seekNextKey(origRing)
    # make a list of keyIds from the new key
    newRing.seek(0, SEEK_END)
    limit = newRing.tell()
    newRing.seek(0)
    while newRing.tell() < limit:
        newKeyIds.append(getKeyId(newRing))
        seekNextKey(newRing)
    # ensure no keys were lost
    origRing.seek(0)
    newRing.seek(0)
    for keyId in origKeyIds:
        if keyId not in newKeyIds:
            origRing.close()
            newRing.close()
            raise IncompatibleKey("Attempting to remove a subkey from key %s is not allowed" %fingerprint)
    # for the main key and all subkeys in the original key:
    # loop thru all the revocations and ensure the new key contains at least
    # those revocations
    origRing.seek(0, SEEK_END)
    limit = origRing.tell()
    origRing.seek(0)
    seekNextSignature(origRing)
    while origRing.tell() < limit:
        # ensure sig is in fact a revocation.
        sigStartPoint = origRing.tell()
        blockType = readBlockType(origRing)
        try:
            readBlockSize(origRing, blockType)
        except:
            origRing.close()
            newRing.close()
            raise
        if ord(origRing.read(1)) != 4:
            origRing.close()
            newRing.close()
            raise IncompatibleKey("Only V4 signatures allowed")
        sigType = ord(origRing.read(1))
        origRing.seek(sigStartPoint)
        # if it is a revocation, read in the entire revocation packet
        if sigType in (SIG_TYPE_KEY_REVOC, SIG_TYPE_SUBKEY_REVOC, SIG_TYPE_CERT_REVOC):
            seekNextPacket(origRing)
            packetLength = origRing.tell() - sigStartPoint
            origRing.seek(sigStartPoint)
            revocPacket = origRing.read(packetLength)
            origRing.seek(sigStartPoint)
            # use substring matching to ensure revocation is still in new key
            if revocPacket not in newKey:
                origRing.close()
                newRing.close()
                raise IncompatibleKey("Removing a revocation from key %s is not allowed" %fingerprint)
        # seek to next signature
        seekNextSignature(origRing)
    origRing.close()
    newRing.close()

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
            # Assume an ExtendedFile
            assert hasattr(message, "pread"), "Not an ExtendedFile"
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
        for pkt in self.iterPackets():
            if pkt.tag in PKT_ALL_KEYS:
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
            if pkt.tag in PKT_MAIN_KEYS:
                if keyId.upper() in pkt.getKeyId():
                    # This is a main key and it has the keyId we need
                    return pkt
                mainKey = pkt
            elif pkt.tag in PKT_SUB_KEYS:
                if keyId.upper() in pkt.getKeyId():
                    # This is a subkey, return the main key
                    assert mainKey is not None
                    return mainKey
            try:
                pkt = pkt.next()
            except StopIteration:
                break

        return None

class PacketTypeDispatcher(object):
    _registry = {}

    @staticmethod
    def addPacketType(klass):
        PacketTypeDispatcher._registry[klass.tag] = klass

    @staticmethod
    def getClass(tag):
        return PacketTypeDispatcher._registry.get(tag, PGP_Packet)

def seekKeyById(keyRing, keyId):
    pass

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
            raise Exception("First bit not 1")

        if first & 0x40:
            print "new header"
            newStyle = True
            self._newHeader(first)
        else:
            newStyle = False
            self._oldHeader(first)

        _bodyStream = util.SeekableNestedFile(self._f.file,
                     self.bodyLength, self._f.start + self.headerLength)
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
        rest = self._f.read()
        if len(rest) != blLen:
            raise Exception("Unable to read %s bytes" % blLen)

        bbytes = [ ord(x) for x in rest ]
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

        body1 = self._f.read(1)
        if len(body1) < 1:
            raise Exception("Need to read at least one more byte")
        body1 = ord(body1)

        if body1 & 0xC0 == 0:
            # 4.2.2.1. One-Octet Lengths (less than 192)
            self.headerLength = 2
            self.bodyLength = body1
            return

        if body1 & 0xE0 == 0:
            # 4.2.2.2. Two-Octet Lengths (between 192 and 223):
            self.headerLength = 3
            self._f.__init__(self._f.file, self.headerLength, self._f.start)
            self._f.seek(2)

            body2 = self._f.read(1)

            if len(body2) != 1:
                raise Exception("Unable to read 1 more byte")

            body2 = ord(body2)
            self.bodyLength = len2bytes(body1, body2)
            return

        if body1 & 0xFF:
            # 4.2.2.3. Five-Octet Lengths (exactly 255)
            self.headerLength = 5

            self._f.__init__(self._f.file, self.headerLength, self._f.start)
            self._f.seek(2)

            rest = self._f.read(4)
            if len(rest) != 4:
                raise Exception("Unable to read 4 bytes")
            body2 = ord(rest[0])
            body3 = ord(rest[1])
            body4 = ord(rest[2])
            body5 = ord(rest[3])
            self.bodyLength = int4bytes(body2, body3, body4, body5)
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

    def validate(self):
        """To be overridden by various subclasses"""
        pass

    def _getHeaderLength(self, minHeaderLen = 2):
        # bsrepr is the body size representation
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
            raise NotImplementedError
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
        self.reset()
        while 1:
            buf = self._bodyStream.read(self.BUFFER_SIZE)
            if not buf:
                break
            stream.write(buf)

    def write(self, stream):
        self.writeHeader(stream)
        self.writeBody(stream)

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
            buf = src.read(self.BUFFER_SIZE)
            if not buf:
                break
            dst.write(buf)

    @staticmethod
    def _updateHash(hashObj, stream):
        """Update the hash object with data from the stream"""
        while 1:
            buf = stream.read(self.BUFFER_SIZE)
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

    def getUserIds(self):
        # Start with a key of some sort
        assert self.tag in PKT_ALL_KEYS
        pkt = self
        while 1:
            try:
                pkt = pkt.next()
            except StopIteration:
                break
            if pkt.tag in PKT_ALL_KEYS:
                # We got to the next key, stop
                break
            if pkt.tag == PKT_USERID:
                yield pkt.readBody()

    def getBodyStream(self):
        return self._bodyStream

    def _iterSubPackets(self, limitTags):
        """Iterate over the packets following this packet, until we reach a
        packet of the specified type as the limit"""
        pkt = self.next()
        while not pkt.isEmpty() and pkt.tag not in limitTags:
            yield pkt
            pkt = pkt.next()

class PGP_Packet(PGP_BasePacket):
    """Anonymous PGP packet"""
    __slots__ = ['tag']
    def setTag(self, tag):
        self.tag = tag

class PGP_BaseKeySig(PGP_BasePacket):
    """Base class for keys and signatures"""

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

        r = 0L
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
            raise InvalidBodyError("Invalid signature type %s" % sigType)
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
            self.checkStreamLength(dataf, 8)
            self.signerKeyId = self._readBin(dataf, 8)
            return binSeqToString(self.signerKeyId)

    def decodeSigSubpackets(self, fobj):
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
            padLen = (len(hex(mainKey.n)) - 5 - 2 * (len(sigString) + len(hashPads[hashAlg]))) // 2 -1
            sigString = chr(1) + chr(0xFF) * padLen + hashPads[hashAlg] + sigString

        if not mainKey.verify(sigString, digSig):
            raise BadSelfSignature(None)

PacketTypeDispatcher.addPacketType(PGP_Signature)

class PGP_UserID(PGP_BasePacket):
    __slots__ = ['id']
    tag = PKT_USERID

    def validate(self):
        self.resetBody()
        self.id = self.readBody()

    def toString(self):
        return self.id

PacketTypeDispatcher.addPacketType(PGP_UserID)

class PGP_Key(PGP_BaseKeySig):
    __slots__ = ['_parsed', 'version', 'createdTimestamp', 'pubKeyAlg',
                 'mpiFile', 'mpiLen', 'daysValid']
    # Base class for public/secret keys/subkeys
    tag = None

    def validate(self):
        self.version = self.createdTimestamp = self.pubKeyAlg = None
        self.mpiFile = self.mpiLen = None
        self.daysValid = None
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
        self.createdTimestamp = len4bytes(*self._readBin(self._bodyStream, 4))

        # daysValid
        data = self.readBin(2)
        self.daysValid = int2bytes(*data)

        # Public key algorithm
        self.pubKeyAlg, = self.readBin(1)

        # Record current position in body
        mpiStart = self._bodyStream.tell()
        # Read and discard 2 MPIs
        self._readCountMPIs(self._bodyStream, count, discard = True)
        self.mpiLen = self._bodyStream.tell() - mpiStart
        self.mpiFile = util.SeekableNestedFile(self._bodyStream, self.mpiLen,
            start = mpiStart)

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

        return m.hexdigest().upper()

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
            # Cheat a little, poke into the NestedFile to get to the parent
            # file
            newMsg = PGP_Message(self._bodyStream.file, start = 0)
            pkt = newMsg.seekParentKey(self.getKeyId())
            parentRevoked, parentExpire = pkt.getEndOfLife()

        expireTimestamp = revocTimestamp = 0

        # Get our key ID
        keyId = self.getKeyId()
        intKeyId = fingerprintToInternalKeyId(keyId)

        # Iterate over signatures, look for one whose ID matches our own ID
        for pkt in self.iterSignatures():
            if intKeyId != pkt.getSigId():
                continue
            if pkt.version != 4:
                raise IncompatibleKey("Not a V4 signature")
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

    def iterSubPackets(self):
        # Stop at another main key (pub or sec)
        return self._iterSubPackets(PKT_MAIN_KEYS)

    def iterSignatures(self):
        return (x for x in self._iterSubPackets(PKT_ALL_KEYS)
                if x.tag == PKT_SIG)

    def assertSigningKey(self):
        # Find self signature of this key
        # first search for the public key algortihm octet. if the key is really
        # old, this might be the only hint that it's legal to use this key to
        # make digital signatures.
        if self._parsed is False:
            self.parse()

        if sig.pubKeyAlg in (PK_ALGO_RSA_SIGN_ONLY, PK_ALGO_DSA):
            # the public key algorithm octet satisfies this test. no more
            # checks required.
            return True

        keyId = self.getKeyId()

        # If it's a subkey, look for the master key
        if self.tag in PKT_SUB_KEYS:
            # Cheat a little, poke into the NestedFile to get to the parent
            # file
            newMsg = PGP_Message(self._bodyStream.file, start = 0)
            pkt = newMsg.seekParentKey(keyId)
            return pkt.assertSigningKey()

        intKeyId = fingerprintToInternalKeyId(fingerprint)
        # Look for a self signature
        for pkt in self.iterSignatures():
            if intKeyId != pkt.getSigId():
                continue
            # We know it's a ver4 packet, otherwise getSigId would have failed
            for spktType, dataf in pkt.decodeSigSubpackets(pkt.hashedFile):
                if spktType == SIG_SUBPKT_KEY_FLAGS:
                    break
                # RFC 2440, sect. 5.2.3.20
                foct, = self._readBin(dataf, 1)
                if foct & 0x02:
                    return True
        # No subpacket or no key flags
        raise IncompatibleKey('Key %s is not a signing key.'% intKeyId)

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

class PGP_PublicKey(PGP_Key):
    tag = PKT_PUBLIC_KEY
    pubTag = PKT_PUBLIC_KEY

    def toPublicKey(self, minHeaderLen = 2):
        return newPacket(self.pubTag, self._bodyStream,
                         minHeaderLen = minHeaderLen)

class PGP_SecretKey(PGP_Key):
    __slots__ = ['s2k', 'symmEncAlg', 's2kType', 'hashAlg', 'salt',
                 'count', 'initialVector', 'encMpiFile']
    tag = PKT_SECRET_KEY
    pubTag = PKT_PUBLIC_KEY

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


class PGP_PublicSubKey(PGP_PublicKey):
    tag = PKT_PUBLIC_SUBKEY
    # Subkeys are promoted to main keys
    pubTag = PKT_PUBLIC_KEY

    def iterSubPackets(self):
        # Stop at another key
        return self._iterSubPackets(PKT_ALL_KEYS)

class PGP_SecretSubKey(PGP_SecretKey):
    tag = PKT_SECRET_SUBKEY
    # Subkeys are promoted to main keys
    pubTag = PKT_PUBLIC_KEY

    def iterSubPackets(self):
        # Stop at another key
        return self._iterSubPackets(PKT_ALL_KEYS)

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
    return PGP_PacketFromStream().read(stream, start = start)

def len2bytes(v1, v2):
    """Return the packet body length when represented on 2 bytes"""
    return (v1 - 192) << 8 + v2 + 192

def len4bytes(v1, v2, v3, v4):
    """Return the packet body length when represented on 4 bytes"""
    return (v1 << 24) | (v2 << 16) | (v3 << 8) | v4

def int2bytes(v1, v2):
    return (v1 << 8) + v2

def int4bytes(v1, v2, v3, v4):
    return len4bytes(v1, v2, v3, v4)

class KeyId(object):
    """Class to store a Key Identifier"""
    __slots__ = ['id']
    def __init__(self, keyId = None, hexKeyId = None):
        if hexKeyId:
            self.id = self.fromHex(hexKeyId)
        else:
            self.id = keyId

    def fromHex(self, hexKeyId):
        assert len(hexKeyId) == 16, "Hex key ID should be 16 bytes"
        return "".join(chr(int(hexKeyId[i:i+2], 16))
                        for i in range(0, len(hexKeyId), 2))

    def __str__(self):
        if self.id is None:
            return ''
        return ''.join('%02x' % ord(c) for c in self.id).lower()
