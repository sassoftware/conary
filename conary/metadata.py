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
from httplib import HTTPConnection
from urllib2 import urlopen
import textwrap
import time
import urlparse
import xml.dom.minidom
import xml.parsers.expat

from conary import versions
from conary.dbstore import idtable
from conary.fmtroves import TroveCategories, LicenseCategories
from conary.lib import log
from conary.local import schema

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
    def __init__(self, db, schema = True):
        self.db = db
        if schema:
            schema.createMetadata(db)

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
                WHERE itemId=? AND versionId=? AND branchId=? ORDER BY timestamp DESC LIMIT 1""",
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

        cu.execute("SELECT metadataId FROM Metadata WHERE itemId=? AND versionId=? AND branchId=? ORDER BY timestamp DESC LIMIT 1",
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
        cu.execute("""SELECT
                          Versions.version
                      FROM
                          Metadata, Branches, Versions
                      WHERE
                              Metadata.itemId=? AND Metadata.branchId=?
                          AND Metadata.branchId=Branches.branchId
                          AND Metadata.versionId=Versions.versionId
                      ORDER BY
                          Metadata.timeStamp DESC LIMIT 1""",
                   (itemId, branchId))

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

def formatDetails(repos, cfg, troveName, branch, sourceTrove):
    md = repos.getMetadata([troveName, branch], branch.label())
    while not md and branch.hasParentBranch():
        lastHost = branch.getHost()
        branch = branch.parentBranch()
        while branch.hasParentBranch() and branch.getHost() == lastHost:
            branch = branch.parentBranch()

        if branch.getHost() != lastHost:
            md = repos.getMetadata([troveName, branch], branch.label())

    # check source trove for metadata
    if not md and sourceTrove:
        troveName = sourceTrove.getName()
        md = repos.getMetadata([troveName, sourceTrove.getVersion().branch()],
                               sourceTrove.getVersion().branch().label())

    if troveName in md:
        md = md[troveName]

        wrapper = textwrap.TextWrapper(initial_indent='    ',
                                       subsequent_indent='    ')
        wrapped = wrapper.wrap(md.getLongDesc())

        for l in md.getLicenses():
            yield "License   : %s" % l
        for c in md.getCategories():
            yield "Category  : %s" % c
        yield "Summary   : %s" % md.getShortDesc()
        yield "Description: "
        for line in wrapped:
            yield line
