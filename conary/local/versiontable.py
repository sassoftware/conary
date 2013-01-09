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


from conary import versions

# XXX: this looks awfully similar to an idtable...
class VersionTable:
    """
    Maps a version to an id and timestamp pair.
    """
    noVersion = 0

    def __init__(self, db):
        self.db = db

    def addId(self, version):
        cu = self.db.cursor()
        cu.execute("INSERT INTO Versions (version) VALUES (?)",
                   version.asString())
        return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM Versions WHERE versionId=?", theId)

    def _makeVersion(self, str, timeStamps):
        ts = [ float(x) for x in timeStamps.split(":") ]
        v = versions.VersionFromString(str, timeStamps=ts)
        return v

    def getBareId(self, theId):
        """
        Gets a version object w/o setting any timestamps.
        """
        cu = self.db.cursor()
        cu.execute("""SELECT version FROM Versions
                      WHERE Versions.versionId=?""", theId)
        try:
            (s, ) = cu.next()
            return versions.VersionFromString(s)
        except StopIteration:
            raise KeyError, theId

    def has_key(self, version):
        cu = self.db.cursor()
        cu.execute("SELECT versionId FROM Versions WHERE version=?",
                   version.asString())
        return not(cu.fetchone() == None)

    def __delitem__(self, version):
        cu = self.db.cursor()
        cu.execute("DELETE FROM Versions WHERE version=?", version.asString())

    def __getitem__(self, version):
        v = self.get(version, None)
        if v == None:
            raise KeyError, version

        return v

    def get(self, version, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT versionId FROM Versions WHERE version=?",
                   version.asString())

        item = cu.fetchone()
        if item:
            return item[0]
        else:
            return defValue

    def removeUnused(self):
        # removes versions which don't have parents and aren't used
        # by any FileStreams
        cu = self.db.cursor()
        cu.execute("""
            DELETE FROM Versions WHERE versionId IN
                (SELECT versionId from Versions LEFT OUTER JOIN
                    (SELECT versionId AS fooId from Parent UNION
                     SELECT versionId AS fooId FROM FileStreams)
                ON Versions.versionId = fooId WHERE fooId is NULL)
            """)
