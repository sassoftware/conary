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
import metadata
import textwrap
import time
import urlparse
import versions
import xml.dom.minidom
import xml.parsers.expat

from lib import log
from local import idtable
from fmtroves import TroveCategories, LicenseCategories
from httplib import HTTPConnection
from urllib2 import urlopen

class MDClass:
    (SHORT_DESC, LONG_DESC,
     URL, LICENSE, CATEGORY,
     SOURCE) = range(6)

    # mapping from enum id to real name
    className = {SHORT_DESC: "shortDesc",
                 LONG_DESC:  "longDesc",
                 URL:        "url",
                 LICENSE:    "license",
                 CATEGORY:   "category",
                 SOURCE:     "source"}
    
    (STRING, LIST) = range(2)
    
    types = {SHORT_DESC:    STRING,
             LONG_DESC:     STRING,
             URL:           LIST,
             LICENSE:       LIST,
             CATEGORY:      LIST,
             SOURCE:        STRING}
    
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
            urls, licenses, categories, source="", language="C"):
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
                             (MDClass.CATEGORY, categories),\
                             (MDClass.SOURCE, [source]):
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
        for mdClass, className in MDClass.className.items():
            classType = MDClass.types[mdClass]
            
            if classType == MDClass.STRING:
                items[className] = ""
            elif classType == MDClass.LIST:
                items[className] = []
            else:
                items[className] = None
        
        for mdClass, data in cu:
            className = MDClass.className[mdClass]
            classType = MDClass.types[mdClass]
            
            if classType == MDClass.STRING:
                items[className] = data
            elif classType == MDClass.LIST:
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

class Metadata:
    shortDesc = ""
    longDesc = ""
    urls = []
    licenses = []
    categories = []
    language = "C"
    version = None
    source = "local"

    def __init__(self, md):
        if md:
            self.shortDesc = md["shortDesc"]
            self.longDesc = md["longDesc"]
            self.urls = md["url"]
            self.licenses = md["license"]
            self.categories = md["category"]
            if "version" in md:
                self.version = md["version"]
            if "source" in md and md["source"]:
                self.source = md["source"]
            if "language" in md:
                self.language = md["language"]

    def freeze(self):
        return {"shortDesc": self.shortDesc,
                "longDesc":  self.longDesc,
                "url":       self.urls,
                "license":   self.licenses,
                "category":  self.categories,
                "version":   self.version,
                "source":    self.source,
                "language":  self.language}

    def getShortDesc(self):
        return self.shortDesc

    def getLongDesc(self):
        return self.longDesc

    def getUrls(self):
        return self.urls

    def getLicenses(self):
        return self.licenses

    def getCategories(self):
        return self.categories
    
    def getVersion(self):
        return self.version

    def getSource(self):
        return self.source
        
    def getLanguage(self):
        return self.language

class NoFreshmeatRecord(xml.parsers.expat.ExpatError):
    pass

def fetchFreshmeat(troveName):
    url = urlopen('http://freshmeat.net/projects-xml/%s/%s.xml' % (troveName, troveName))

    try:
        doc = xml.dom.minidom.parse(url)
        metadata = {}
        
        shortDesc = doc.getElementsByTagName("desc_short")[0]
        if shortDesc.childNodes:
            metadata["shortDesc"] = shortDesc.childNodes[0].data

        longDesc = doc.getElementsByTagName("desc_full")[0]
        if longDesc.childNodes:
            metadata["longDesc"] = longDesc.childNodes[0].data
        
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

        metadata["source"] = "freshmeat"
        metadata["language"] = "C"
        return Metadata(metadata)
    except xml.parsers.expat.ExpatError:
        raise NoFreshmeatRecord

def showDetails(repos, cfg, db, troveName, branchStr=None):
    sourceName = troveName + ":source"

    branch = None
    if branchStr:
        if branchStr[0] == '/': # is a branch or version
            version = versions.VersionFromString(branchStr)

            if version.isVersion():
                branch = version.branch()
        else: # is a label
            label = versions.Label(branchStr)

            # find the first matching trove in branch of label
            leaves = repos.getTroveLeavesByLabel([sourceName], label)
            if leaves[sourceName]:
                branch = leaves[sourceName][0].branch()

        installedVers = None
    else:
        # search the local database and use the installed trove's branch first
        versionList = db.getTroveVersionList(troveName)
        if versionList:
            installedVers = [x.trailingVersion().asString() for x in versionList]
            branch = versionList[0].branch()
        else:
            # otherwise use the first installpath that has the trove
            installedVers = ["None"]
            for label in cfg.installLabelPath:
                leaves = repos.getTroveLeavesByLabel([sourceName], label)
                if leaves[sourceName]:
                    branch = leaves[sourceName][0].branch()
                    break
                    
    if not branch:
        log.error("trove not found for branch %s: %s", branchStr, troveName)
        return 0

    log.info("retrieving package details for %s on %s", troveName, branch.asString())
    md = repos.getMetadata([sourceName, branch], branch.label())

    if sourceName in md:
        md = md[sourceName]
        wrapped = textwrap.wrap(md.getLongDesc())
        wrappedDesc = "\n".join(wrapped)

        print "Name       : %-25s" % troveName,
        print "Branch     : %s" % branch.asString()
        if installedVers and len(installedVers) > 1:
            print "Versions   : %s" % ", ".join(installedVers)
        elif installedVers and len(installedVers) == 1:
            print "Version    : %s" % installedVers[0]
        print "Size       : %-25s" % str(0),
        print "Time built : %s" % "N/A"
        for l in md.getLicenses():
            print "License    : %s" % l
        for c in md.getCategories():
            print "Category   : %s" % c
        print "Summary    : %s" % md.getShortDesc()
        print "Description: \n%s" % (wrappedDesc)
    else:
        log.info("no details found for %s", troveName)

