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

import StringIO
import base64

import sqlite3
from lib import openpgpfile, openpgpkey

class OpenPGPKeyTable:
    def __init__(self, db):
        openpgpkey.keyCache.setKeyTable(self)
        openpgpkey.keyCache.setSource(openpgpkey._KC_SRC_DB)
        self.db = db
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "PGPKeys" not in tables:
            cu.execute("""CREATE TABLE PGPKeys(keyId INTEGER,
                                               userId INTEGER,
                                               fingerprint STRING(40) UNIQUE,
                                               pgpKey BINARY,
                                               PRIMARY KEY(keyId))""")
        if "PGPFingerprints" not in tables:
            cu.execute("""CREATE TABLE PGPFingerprints(
                                             keyId INTEGER,
                                             fingerprint STRING(40),
                                             PRIMARY KEY(fingerprint))""")
        db.commit()

    def getFingerprint(self, keyId):
        cu = self.db.cursor()
        r = cu.execute('SELECT fingerprint FROM PGPFingerprints '
                       'WHERE fingerprint LIKE "%' + keyId + '%"')
        keyList = r.fetchall()
        if (len(keyList) != 1):
            raise openpgpkey.KeyNotFound(keyId)
        return keyList[0][0]

    def _getFingerprints(self, keyRing):
        r = []
        keyRing.seek(0,2)
        limit = keyRing.tell()
        keyRing.seek(0)
        while (keyRing.tell() < limit):
            r.append(openpgpfile.getKeyId(keyRing))
            openpgpfile.seekNextKey(keyRing)
        return r

    def addNewAsciiKey(self, userId, asciiData):
        data = StringIO.StringIO(asciiData)
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
        self.addNewKey(userId, base64.b64decode(buf))

    def _countKeys(self, keyRing):
        keyType = openpgpfile.getBlockType(keyRing)
        keyRing.seek(-1,1)
        if (keyType >> 2) & 15 != 6:
            raise IncompatibleKey('Key must be a public key')
        keyCount = 0
        start = keyRing.tell()
        keyRing.seek(0, 2)
        limit = keyRing.tell()
        keyRing.seek(start)
        while keyRing.tell() < limit:
            keyType = openpgpfile.getBlockType(keyRing)
            keyRing.seek(-1,1)
            if (keyType >> 2) & 15 in [ 5, 6 ]:
                keyCount+=1
            openpgpfile.seekNextKey(keyRing)
        keyRing.seek(start)
        return keyCount

    def addNewKey(self, userId, pgpKeyData):
        cu = self.db.cursor()
        r = cu.execute('SELECT IFNULL(MAX(keyId),0) + 1 FROM PGPKeys')
        keyId = r.fetchone()[0]
        keyRing = StringIO.StringIO(pgpKeyData)
        if(self._countKeys(keyRing) != 1):
            raise IncompatibleKey('Submit only one key at a time.')
        mainFingerprint = openpgpfile.getKeyId(keyRing)
        try:
            cu.execute('INSERT INTO PGPKeys VALUES(?, ?, ?, ?)',
                       (keyId, userId, mainFingerprint, pgpKeyData))
        except sqlite3.ProgrammingError:
            # FIXME: make a new error for this
            raise
        keyFingerprints = self._getFingerprints(keyRing)
        for fingerprint in keyFingerprints:
            cu.execute('INSERT INTO PGPFingerprints VALUES(?, ?)',
                       (keyId, fingerprint))
        self.db.commit()
        keyRing.close()

    def deleteKey(self, keyId):
        fingerprint = self.getFingerprint(keyId)
        cu = self.db.cursor()
        r = cu.execute('SELECT keyId FROM PGPFingerprints '
                       'WHERE fingerprint=?', (fingerprint,))
        keyId=r.fetchone()[0]
        cu.execute('DELETE FROM PGPFingerprints WHERE keyId=?', (keyId,))
        cu.execute('DELETE FROM PGPKeys WHERE keyId=?', (keyId,))
        self.db.commit()

    def getPGPKeyData(self, keyId):
        cu = self.db.cursor()
        r = cu.execute("""SELECT pgpKey FROM PGPKeys
                             LEFT JOIN PGPFingerprints ON
                               PGPKeys.keyId=PGPFingerprints.keyId
                          WHERE PGPFingerprints.fingerprint like "%%%s%%"
                          """ 
                       %keyId)
        return r.fetchone()[0]

    def getUsersMainKeys(self, userId):
        cu = self.db.cursor()
        r = cu.execute('SELECT fingerprint FROM PGPKeys '
                       'WHERE userId=?', (userId,))
        return [ x[0] for x in r.fetchall() ]

    def getSubkeys(self, fingerprint):
        cu = self.db.cursor()
        r = cu.execute("""
                SELECT PGPFingerprints.fingerprint
                    FROM PGPFingerprints LEFT JOIN PGPKeys USING(keyid)
                WHERE PGPKeys.fingerprint=?
                     AND PGPFingerprints.fingerprint != PGPKeys.fingerprint""",
                       (fingerprint,))
        return [ x[0] for x in r.fetchall() ]
