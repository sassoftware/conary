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

from fmtroves import TroveCategories, LicenseCategories

import urllib
import urlparse
import xml.dom.minidom
import xml.parsers.expat
import metadata
from urllib2 import urlopen
from httplib import HTTPConnection

class MDClass:
    SHORT_DESC = 0
    LONG_DESC = 1
    URL = 2
    LICENSE = 3
    CATEGORY = 4

    # mapping from enum id to real name
    className = {SHORT_DESC: "shortDesc",
                 LONG_DESC:  "longDesc",
                 URL:        "url",
                 LICENSE:    "license",
                 CATEGORY:   "category"}
                  
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

        # XXX should I be calling commit here?
        self.db.commit()
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
           
        # URL, LICENSE, and CATEGORY are not translated
        cu.execute("""SELECT class, data FROM MetadataItems
                      WHERE metadataId=? and (language=?
                            OR class IN (?, ?, ?))""",
                   metadataId, language, MDClass.URL,
                                         MDClass.LICENSE,
                                         MDClass.CATEGORY)

        # create a dictionary of metadata classes
        # each key points to a list of metadata items
                 
        items = {}
        for className in MDClass.className.values():
            items[className] = []
            
        for mdClass, data in cu:
            className = MDClass.className[mdClass]
            items[className].append(data)

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

def resolveUrl(url):
    """Follows a redirect one level and returns the location of the HTTP 302 redirect"""
    url = urlparse.urlparse(url)
    connection = HTTPConnection(url[1])
    connection.request("GET", url[2])
    request = connection.getresponse()
    if request.status == 302: # header "Found:", might need more here
        realUrl = request.getheader("Location")
    else:
        realUrl = urlparse.urlunparse(url)
    return realUrl

def fetchFreshmeat(troveName):
    url = urlopen('http://freshmeat.net/projects-xml/%s/%s.xml' % (troveName, troveName))

    doc = xml.dom.minidom.parse(url)
    metadata = {}
    
    shortDesc = doc.getElementsByTagName("desc_short")[0]
    if shortDesc.childNodes:
        metadata["shortDesc"] = [shortDesc.childNodes[0].data]

    longDesc = doc.getElementsByTagName("desc_full")[0]
    if longDesc.childNodes:
        metadata["longDesc"] = [longDesc.childNodes[0].data]
    
    metadata["url"] = []
    urlHomepage = doc.getElementsByTagName("url_homepage")[0]
    if urlHomepage.childNodes:
        metadata["url"].append(resolveUrl(urlHomepage.childNodes[0].data))
    metadata["url"].append("http://freshmeat.net/projects/%s/" % troveName)
    
    metadata["license"] = []
    metadata["category"] = []

    for node in doc.getElementsByTagName("trove_id"):
        id = node.childNodes[0].data
        if id in LicenseCategories:
            name = LicenseCategories[id]
            metadata["license"].append(name)
        else:
            name = TroveCategories[id]
            if name.startswith('Topic ::'):
                metadata["category"].append(name)

    return metadata
