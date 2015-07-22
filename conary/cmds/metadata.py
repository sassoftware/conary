#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import textwrap
import time
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
    def __init__(self, db, create = True):
        self.db = db
        if create:
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
        cu.execute("""
        SELECT class, data FROM MetadataItems
        WHERE metadataId=? and (language=? OR class IN (?, ?, ?))
        """, (metadataId, language,
              MDClass.URL, MDClass.LICENSE, MDClass.CATEGORY))

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

        for key, value in items.iteritems():
            if isinstance(value, list):
                items[key] = sorted(value)
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
