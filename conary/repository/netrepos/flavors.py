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


from conary.deps import deps

class Flavors:
    # manages the Flavors and FlavorMap tables
    def __init__(self, db):
        self.db = db

    def createFlavorMap(self, flavorId, flavor, cu = None):
        if cu is None:
            cu = self.db.cursor()
        for depClass in flavor.getDepClasses().itervalues():
            for dep in depClass.getDeps():
                cu.execute("""INSERT INTO FlavorMap
                (flavorId, base, depClass, sense, flag)
                VALUES (?, ?, ?, ?, NULL)""",
                           flavorId, dep.name,
                           depClass.tag, deps.FLAG_SENSE_REQUIRED)
                for (flag, sense) in dep.flags.iteritems():
                    cu.execute("""INSERT INTO FlavorMap
                    (flavorId, base, depClass, sense, flag)
                    VALUES (?, ?, ?, ?, ?)""",
                               flavorId, dep.name,
                               depClass.tag, sense, flag)

    def createFlavor(self, flavor):
        cu = self.db.cursor()
        cu.execute("INSERT INTO Flavors (flavor) VALUES (?)",
                   flavor.freeze())
        flavorId = cu.lastrowid
        self.createFlavorMap(flavorId, flavor, cu)
        return flavorId

    def __getitem__(self, flavor):
        val = self.get(flavor, None)

        if val is None:
            raise KeyError, flavor

        return val

    def get(self, flavor, defValue):
        if flavor is None:
            return None

        cu = self.db.cursor()
        cu.execute("SELECT flavorId FROM Flavors WHERE flavor = ?",
                   flavor.freeze())
        item = cu.fetchone()
        if item is None:
            return defValue
        return item[0]

    def getId(self, flavorId):
        if flavorId == 0:
            return deps.Flavor()

        cu = self.db.cursor()
        cu.execute("SELECT flavor FROM Flavors WHERE flavorId = ?",
                   flavorId)
        try:
            return deps.ThawFlavor(cu.next()[0])
        except StopIteration:
            raise KeyError, flavorId
