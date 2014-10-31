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
from conary.dbstore import idtable

class VersionTable(idtable.IdTable):
    """
    Maps a version to an id and timestamp pair.
    """

    def __init__(self, db):
        idtable.IdTable.__init__(self, db, 'Versions', 'versionId', 'version')

    def addId(self, version):
        return idtable.IdTable.addId(self, version.asString())

    def getBareId(self, theId):
        """
        Gets a version object w/o setting any timestamps.
        """
        return versions.VersionFromString(self.getId(theId))

    def has_key(self, version):
        return idtable.IdTable.has_key(self, version.asString())

    def __delitem__(self, version):
        idtable.IdTable.__delitem__(self, version.asString())

    def __getitem__(self, version):
        v = self.get(version, None)
        if v is None:
            raise KeyError(version)
        return v

    def get(self, version, defValue):
        return idtable.IdTable.get(self, version.asString(), defValue)

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
