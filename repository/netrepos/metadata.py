#
# Copyright (c) 2004 Specifix, Inc.
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

from local import idtable
import versions
import time

class MDClass:
    SHORT_DESC = 0
    LONG_DESC = 1
    URL = 2
    LICENSE = 3
    CATEGORY = 4

class MetadataTable:
    def __init__(self, db):
        self.db = db

        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Metadata' not in tables:
            cu.execute("""
                CREATE TABLE Metadata(metadataId INTEGER PRIMARY KEY,
                                      itemId INT,
                                      versionId INT,
                                      branchId INT,
                                      timeStamp INT)""")

        if 'MetadataItems' not in tables:
            cu.execute("""
                CREATE TABLE MetadataItems(metadataId INT,
                                           class INT,
                                           data STR,
                                           language STR)""")

    def add(self, itemId, versionId, branchId, shortDesc, longDesc,
            urls, licenses, categories, language="C"):
        cu = self.db.cursor()

        if language == "C":
            cu.execute("""
                INSERT INTO Metadata (itemId, versionId, branchId, timestamp)
                VALUES(?, ?, ?, ?)""", itemId, versionId, branchId, time.time())
            mdId = cu.lastrowid
        else:
            cu.execute("""
                SELECT metadataId FROM Metadata
                WHERE itemId=? AND versionId=? AND branchId=?""",
                itemId, versionId, branchId)
            mdId = cu.fetchone()[0]

        for mdClass, data in (MDClass.SHORT_DESC, [shortDesc]),\
                             (MDClass.LONG_DESC, [longDesc]),\
                             (MDClass.URL, urls),\
                             (MDClass.LICENSE, licenses),\
                             (MDClass.CATEGORY, categories):
            for d in data:
                cu.execute("""
                    INSERT INTO MetadataItems (metadataId, class, data, language)
                    VALUES(?, ?, ?, ?)""", mdId, mdClass, d, language)
        return mdId

    def get(self, itemId, versionId, branchId, language="C"):
        cu = self.db.cursor()

        cu.execute("SELECT metadataId FROM Metadata WHERE itemId=? AND versionId=? AND branchId=?",
                   itemId, versionId, branchId)
        metadataId = cu.fetchone()
        if metadataId:
            metadataId = metadataId[0]
        else:
            return None
            
        cu.execute("""SELECT class, data FROM MetadataItems
                      WHERE metadataId=? and (language=?
                            OR class IN (?, ?, ?))""",
                   metadataId, language, MDClass.URL,
                                         MDClass.LICENSE,
                                         MDClass.CATEGORY)

        # create a dictionary of metadata classes
        # each key points to a list of metadata items
        items = {}
        for mdClass, data in cu:
            if not items.has_key(mdClass):
                items[mdClass] = []
            items[mdClass].append(data)

        return items

    def getLatestVersion(self, itemId, branchId):
        cu = self.db.cursor()
        cu.execute("""SELECT Versions.version FROM Versions
                      JOIN Metadata ON Metadata.versionId=Versions.versionId
                      JOIN Branches ON Metadata.branchId=Branches.branchId
                      WHERE Metadata.itemId=? AND Metadata.branchId=?
                      ORDER BY Metadata.timeStamp DESC LIMIT 1""", itemId, branchId)

        item = cu.fetchone()
        if item:
            return item[0]
        else:
            return None
