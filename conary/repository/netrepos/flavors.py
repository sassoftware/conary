#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
