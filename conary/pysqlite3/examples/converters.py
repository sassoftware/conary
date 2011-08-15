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


import os
import sqlite

# Ok, let's define a user-defined type we can use with the SQLite database
class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    # The _quote function is currently the way a PySQLite user-defined type
    # returns its string representation to write to the database.
    def _quote(self):
        return "'%f,%f'" % (self.x, self.y)

    def __str__(self):
        return "Point(%f, %f)" % (self.x, self.y)

# The conversion callable needs to accept a string, parse it and return an
# instance of your user-defined type.
def pointConverter(s):
    x, y = s.split(",")
    return Point(float(x), float(y))

# Ensure we have an empty database
if os.path.exists("db"): os.remove("db")

cx = sqlite.connect("db", converters={"point": pointConverter})
cu = cx.cursor()
cu.execute("create table test(p point, n int)")
cu.execute("insert into test(p, n) values (%s, %s)", (Point(-3.2, 4.5), 25))

# For user-defined types, and for statements which return anything but direct
# columns, you need to use the "-- types" feature of PySQLite:
cu.execute("-- types point, int")
cu.execute("select p, n from test")
row = cu.fetchone()

print "p:", row.p       # .columnname instead of [0] is a PySQLite
print "n:", row.n       # extension to the DB-API!
cx.close()
