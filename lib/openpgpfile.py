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
import sha
import md5
from Crypto.Cipher import AES
from Crypto.Cipher import DES3
from Crypto.Cipher import Blowfish
from Crypto.Cipher import CAST
from Crypto.PublicKey import RSA
from Crypto.PublicKey import DSA
from string import upper


class MalformedKeyRing:
    def __str__(self):
        return error
    def __init__(self,reason="Malformed Key Ring"):
        error = "Malformed Key Ring: %s"% reason

class IncompatibleKey:
    def __str__(self):
        return error
    def __init__(self,reason="Incompatible Key"):
        error = "Incompatible Key: %s"% reason

#def getHexString(buf):
#    r=''
#    for i in range(0, len(buf)):
#        r += ( "%02X"% ord(buf[i]))
#    return r

def getBlockType(keyRing):
    r=keyRing.read(1)
    if r != '':
        return ord(r)
    else:
        return -1

def convertPrivateKey(privateBlock):
    if not len(privateBlock):
        return ''
    packetType = ord(privateBlock[0])
    if not (packetType & 128):
        raise MalformedKeyRing("Not an OpenPGP packet.")
    if packetType & 64:
        return ''
    if ((packetType>>2)&15) in [ 6, 14 ]:
        return privateBlock
    if ((packetType>>2)&15) not in [ 5, 7 ]:
        return ''
    blockSize=0
    if ((packetType>>2)&15) == 5:
        newPacketType=152
    else:
        newPacketType=184
    if not (packetType & 3):
        index = 2
    elif (packetType & 3) == 1:
        index = 3
    elif (packetType & 3) == 2:
        index = 5
    else:
        raise MalformedKeyRing("Packet of indeterminate size.")
    if ord(privateBlock[index]) != 4:
        return ''
    buf = privateBlock[ index : index + 6 ]
    index += 5
    algType = ord(privateBlock[ index ])
    index += 1
    if algType in [ 1, 2, 3 ]:
        numMPI = 2
    elif algType in [ 16, 20 ]:
        numMPI = 3
    elif algType == 17:
        numMPI = 4
    else:
        return ''
    for i in range(0, numMPI):
        mLen = ((ord(privateBlock[index]) * 256 + ord(privateBlock[index+1]))+7)/8 + 2
        buf = buf + privateBlock[ index : index + mLen ]
        index += mLen
    bufLen = len(buf)
    if bufLen > 65535:
        newPacketType |= 2
        sizeBytes = 4
    elif bufLen > 255:
        newPacketType |= 1
        sizeBytes = 2
    else:
        sizeBytes = 1
    sizeBuf=''
    for i in range(1,sizeBytes+1):
        sizeBuf += chr((bufLen>>((sizeBytes-i)<<3))&0xff)
    return (chr(newPacketType)+sizeBuf+buf)

def getKeyId(keyRing):
    keyBlock=getBlockType(keyRing)
    if not (keyBlock & 128):
        raise MalformedKeyRing("Not an OpenPGP packet.")
    if (keyBlock == -1) or \
           (keyBlock & 64) or \
           (((keyBlock>>2)&15) not in [ 5, 6, 7, 14]):
        return ''
    if (keyBlock&3) == 2:
        dataSize = ord(keyRing.read(1))<<24 + \
                   ord(keyRing.read(1))<<16 + \
                   ord(keyRing.read(1))<<8 + \
                   ord(keyRing.read(1)) + 5
        keyRing.seek(-5, 1)
    elif (keyBlock&3) == 1:
        dataSize = ord(keyRing.read(1))*256+ord(keyRing.read(1)) + 3
        keyRing.seek(-3, 1)
    elif not (keyBlock&3):
        dataSize = ord(keyRing.read(1)) + 2
        keyRing.seek(-2, 1)
    else:
        raise MalformedKeyRing("Can't parse key of indeterminate size.")
    data=keyRing.read(dataSize)
    keyRing.seek(-1*dataSize,1)
    if ((keyBlock>>2)&15) in [ 5, 7]:
        data=convertPrivateKey(data)
    keyBlock=ord(data[0])
    if not(keyBlock & 1):
        data = chr(keyBlock|1) + chr (0) + data[1:]
    if keyBlock in [ 0xb9, 0x9d ]:
        data = chr(0x99) + data[1:]
    m=sha.new()
    m.update(data)
    return m.hexdigest().upper()

def seekNextKey(keyRing):
    done=0
    packetType=getBlockType(keyRing)
    if packetType == -1:
        return
    while not done:
        special = 0
        dataSize=0
        if not packetType & 64:
            if not (packetType & 3):
                sizeLen = 1
            elif (packetType & 3) == 1:
                sizeLen = 2
            elif (packetType & 3) == 2:
                sizeLen = 4
            else:
                raise MalformedKeyRing("Can't seek past packet of indeterminate length.")
        else:
            octet=ord(keyRing.read(1))
            if octet < 192:
                sizeLen=1
                keyRing.seek(-1,1)
            elif octet < 224:
                special = 1
                dataSize = (ord(keyRing.read(1)) - 192 ) * 256 + \
                          ord(keyRing.read(1)) + 192
            elif octet < 255:
                special = 1
                dataSize = 1 << (ord(keyRing.read(1)) & 0x1f)
            else:
                sizeLen=4
        if not special:
            for i in range(0,sizeLen):
                dataSize = (dataSize << 8) + ord(keyRing.read(1))
        keyRing.seek(dataSize,1)
        packetType=getBlockType(keyRing)
        if (packetType == -1) or ((not (packetType&64)) and (((packetType>>2)&15) in [ 5, 6, 7, 14 ])):
            done=1
            if packetType != -1:
                keyRing.seek(-1,1)

def simpleS2K(passPhrase, hash, keySize):
    r=''
    iter=0
    while(len(r)<((keySize+7)/8)):
        d=hash.new(iter*chr(0))
        #buf = iter*chr(0)
        #buf += passPhrase
        d.update(passPhrase)
        r += d.digest()
        iter += 1
    return r[:(keySize+7)/8]

def saltedS2K(passPhrase, hash, keySize, salt):
    r=''
    iter=0
    while(len(r)<((keySize+7)/8)):
        d=hash.new()
        buf = iter*chr(0)
        buf += salt + passPhrase
        d.update(buf)
        r += d.digest()
        iter += 1
    return r[:(keySize+7)/8]

def iteratedS2K(passPhrase, hash, keySize, salt, count):
    r=''
    iter=0
    count=(16 + (count&15)) << ((count>>4) + 6)
    buf=salt+passPhrase
    while(len(r)<((keySize+7)/8)):
        d=hash.new()
        d.update(iter*chr(0))
        total=0
        while (count-total) > len(buf):
            d.update(buf)
            total+=len(buf)
        if total:
            d.update(buf[:count-total])
        else:
            d.update(buf)
        r += d.digest()
        iter +=1
    return r[:(keySize+7)/8]

def readMPI(keyRing):
    MPIlen=(ord(keyRing.read(1)) * 256 + ord(keyRing.read(1)) + 7 ) / 8
    r=0L
    for i in range(0,MPIlen):
        r=r*256 + ord(keyRing.read(1))
    return r

def readBlockSize(keyRing,sizeType):
    if not sizeType:
        return ord(keyRing.read(1))
    elif sizeType == 1:
        return ord(keyRing.read(1)) * 256 + ord(keyRing.read(1))
    elif sizeType == 2:
        return ord(keyRing.read(1)) << 24 + \
               ord(keyRing.read(1)) << 16 + \
               ord(keyRing.read(1)) << 8 + \
               ord(keyRing.read(1))
    else:
        raise MalformedPacekt("Can't get size of pacekt of indeterminate length")

def getGPGKeyTuple(keyId, secret=0, passPhrase='', keyFile=''):
    if keyFile == '':
        if secret:
            keyFile=os.environ['HOME'] + '/.gnupg/secring.gpg'
        else:
            keyFile=os.environ['HOME'] + '/.gnupg/pubring.gpg'
    keyRing=open(keyFile)
    keyRing.seek(0,2)
    limit=keyRing.tell()
    keyRing.seek(0)
    while (keyId not in getKeyId(keyRing)):
        seekNextKey(keyRing)
        if keyRing.tell() == limit:
            return ()
    startLoc=keyRing.tell()
    packetType=ord(keyRing.read(1))
    if secret and (not ((packetType>>2) & 1)):
        raise IncompatibleKey("Can't get private key from public keyring!")
    limit = readBlockSize(keyRing, packetType & 3) + (packetType & 3) + 1 + startLoc
    if ord(keyRing.read(1)) != 4:
        raise MalformedKeyRing("Can only read V4 packets")
    keyRing.seek(4,1)
    keyType=ord(keyRing.read(1))
    if keyType in [ 1, 2, 3]:
        #do RSA stuff
        #n e
        n=readMPI(keyRing)
        e=readMPI(keyRing)
        if secret:
            privateMPIs=decryptPrivateKey(keyRing, limit, 4, passPhrase)
            r = (n, e, privateMPIs[0], privateMPIs[1],
                 privateMPIs[2], privateMPIs[3])
        else:
            r = (n, e)
    elif keyType in [ 17 ]:
        p=readMPI(keyRing)
        q=readMPI(keyRing)
        g=readMPI(keyRing)
        y=readMPI(keyRing)
        if secret:
            privateMPIs=decryptPrivateKey(keyRing, limit, 1, passPhrase)
            r=(y,g,p,q,privateMPIs[0])
        else:
            r=(y,g,p,q)
    elif keyType in [ 16 ]:
        raise MalformedKeyRing("Can't use El-Gamal keys in current version")
        p=readMPI(keyRing)
        g=readMPI(keyRing)
        y=readMPI(keyRing)
        if secret:
            privateMPIs=decryptPrivateKey(keyRing, limit, 1, passPhrase)
            r=(y,g,p,privateMPIs[0])
        else:
            r=(p,g,y)
    else:
        raise MalformedKeyRing("Wrong key type")
    keyRing.close()
    return r

def makeKey(keyTuple):
    #public lengths: rsa=2, dsa=4, elgamal=3
    #private lengths: rsa=6 dsa=5 elgamal=4
    if len(keyTuple) in [ 2, 6 ]:
        return RSA.construct(keyTuple)
    if len(keyTuple) in [ 4, 5 ]:
        return DSA.construct(keyTuple)

def getPublicKey(keyId, keyFile=''):
    if keyFile:
        keyTuple = getGPGKeyTuple(keyId, 0, '', keyFile)
    else:
        keyTuple = getGPGKeyTuple(keyId)
    return makeKey(keyTuple)

def getPrivateKey(keyId,passPhrase='', keyFile=''):
    if keyFile:
        keyTuple = getGPGKeyTuple(keyId, 1, passPhrase, keyFile)
    else:
        keyTuple = getGPGKeyTuple(keyId, 1, passPhrase)
    return makeKey(keyTuple)

def getFingerprint( keyId, keyFile=''):
    if keyFile == '':
        keyFile=os.environ['HOME'] + '/.gnupg/pubring.gpg'
    keyRing=open(keyFile)
    keyRing.seek(0,2)
    limit = keyRing.tell()
    keyRing.seek(0)
    while (keyRing.tell() < limit) and (keyId not in getKeyId(keyRing)):
        seekNextKey(keyRing)
    return getKeyId( keyRing )

def verifyRFC2440Checksum(data):
    if len(data)<2:
        return 0
    checksum=ord(data[-2:-1]) * 256 + ord (data[-1:])
    runningCount=0
    for i in range(0,len(data)-2):
        runningCount+=ord(data[i])
        runningCount &= 0xffff
    return (runningCount == checksum)

def verifySHAChecksum(data):
    if len(data)<20:
        return 0
    m=sha.new()
    m.update(data[:-20])
    return m.digest() == data[-20:]

#Triple-DES isn't working... says incorrect password...
#AES works fine in 128 bit mode and breaks in 192 and 256
def decryptPrivateKey(keyRing, limit, numMPIs, passPhrase):
    hashes = [ 'Unknown', md5, sha, 'RIPE-MD/160', 'Double Width SHA', 'MD2', 'Tiger/192', 'HAVAL-5-160']
    ciphers = [ 'Unknown', 'IDEA', DES3, CAST, Blowfish, 'SAFER-SK128', 'DES/SK', AES, AES, AES]
    keySizes = [ 0, 0, 192, 128, 128, 0, 0, 128, 192, 256]
    legalCiphers = [ 2, 3, 4, 7, 8, 9 ]
    encryptType=getBlockType(keyRing)
    if not encryptType:
        mpiList = []
        for i in range(0,numMPIs):
            mpiList.append(readMPI(keyRing))
        return mpiList
    if encryptType in [ 0xfe, 0xff ]:
        algType=getBlockType(keyRing)
        if algType not in legalCiphers:
            if algType>9:
                algType=0
            raise IncompatibleKey('Cipher: %s unusable'% ciphers[algType])
        cipherAlg=ciphers[algType]
        s2kType=getBlockType(keyRing)
        hashType=getBlockType(keyRing)
        if hashType in [ 1, 2 ]:
            hashAlg=hashes[hashType]
        else:
            if hashType > 7:
                hashType=0
            raise IncompatibileKey('Hash algortihm %s is not implemented. \
            Key not readable'% hashes[hashType])
        if not s2kType:
            key=simpleS2K(passPhrase, hashAlg, 128)
        elif s2kType == 1:
            salt=keyRing.read(8)
            key=saltedS2K(passPhrase, hashAlg, 128, salt)
        elif s2kType == 3:
            salt=keyRing.read(8)
            count=ord(keyRing.read(1))
            key=iteratedS2K(passPhrase,hashAlg,128, salt,count)
        data = keyRing.read(limit-keyRing.tell()+1)
        if algType>6:
            cipherBlockSize=16
        else:
            cipherBlockSize=8
        cipher = cipherAlg.new(key,1)
        FR=data[:cipherBlockSize]
        data=data[cipherBlockSize:]
        FRE=cipher.encrypt(FR)
        unenc=xorStr(FRE,data[:cipherBlockSize])
        i=0
        while (i+cipherBlockSize)<len(data):
            FR=data[i:i+cipherBlockSize]
            i+=cipherBlockSize
            FRE=cipher.encrypt(FR)
            unenc+=xorStr(FRE,data[i:i+cipherBlockSize])

        if encryptType==0xff:
            check = verifyRFC2440Checksum(unenc)
        else:
            check = verifySHAChecksum(unenc)
        if not check:
            raise IncompatibleKey("Password incorrect")
        data=unenc
        index = 0
        r = []
        for count in range(0,numMPIs):
            MPIlen = (ord(data[index]) * 256 + ord(data[index+1]) + 7 ) / 8
            index += 2
            MPI = 0L
            for i in range(0,MPIlen):
                MPI = MPI * 256 + ord(data[index])
                index += 1
            r.append(MPI)
        return r
    raise MalformedKeyRing("Can't decrypt key. unkown S2K specifier: %i"% encryptType)

def xorStr(str1, str2):
    r=''
    for i in range(0,min(len(str1),len(str2))):
        r+=chr(ord(str1[i])^ord(str2[i]))
    return r

