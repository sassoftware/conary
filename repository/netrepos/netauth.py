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
import md5
import sqlite3
from lib import log

class NetworkAuthorization:
    def check(self, authToken, write = False, label = None, trove = None):
        if label and label.getHost() != self.name:
            log.error("repository name mismatch")
            return False

        if not write and self.anonReads:
            return True

        if not authToken[0]:
            log.error("no authtoken received")
            return False

        stmt = """
            SELECT troveName FROM
               (SELECT userId as uuserId FROM Users WHERE user=? AND
                    password=?)
            JOIN Permissions ON uuserId=Permissions.userId
            LEFT OUTER JOIN TroveNames ON Permissions.troveNameId = TroveNames.troveNameId
        """
        m = md5.new()
        m.update(authToken[1])
        params = [authToken[0], m.hexdigest()]

        where = []
        if label:
            where.append(" labelId=(SELECT labelId FROM Labels WHERE " \
                            "label=?) OR labelId is Null")
            params.append(label.asString())

        if write:
            where.append("write=1")

        if where:
            stmt += "WHERE " + " AND ".join(where)

        cu = self.db.cursor()
        cu.execute(stmt, params)

        for (troveName, ) in cu:
            if not troveName or not trove:
                return True

            regExp = self.reCache.get(troveName, None)
            if regExp is None:
                regExp = re.compile(troveName)
                self.reCache[troveName] = regExp

            if regExp.match(trove):
                return True

        log.error("no permissions match for (%s, %s)" % authToken)

        return False
        
    def checkUserPass(self, authToken, label = None):
        if label and label.getHost() != self.name:
            log.error("repository name mismatch")
            return False

        stmt = "SELECT COUNT(userId) FROM Users WHERE user=? AND password=?"
        m = md5.new()
        m.update(authToken[1])
        cu = self.db.cursor()
        cu.execute(stmt, authToken[0], m.hexdigest())

        row = cu.fetchone()
        return row[0]

    def add(self, user, password, write=True):
        cu = self.db.cursor()
        
        m = md5.new()
        m.update(password)
        cu.execute("INSERT INTO Users VALUES (Null, ?, ?)", user, m.hexdigest())
        userId = cu.lastrowid

        cu.execute("INSERT INTO Permissions VALUES (?, Null, Null, ?)",
                   userId, write)
        self.db.commit()

    def iterUsers(self):
        cu = self.db.cursor()
        cu.execute("""SELECT Users.user, Users.userId, Permissions.write FROM Users
                      LEFT JOIN Permissions ON Users.userId=Permissions.userId""")
        for row in cu:
            yield row

    def __init__(self, dbpath, name, anonymousReads = False):
        self.name = name
        self.db = sqlite3.connect(dbpath)
        self.anonReads = anonymousReads
        self.reCache = {}

        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        
        if "Users" not in tables:
            cu.execute("""CREATE TABLE Users (userId INTEGER PRIMARY KEY,
                                              user STRING UNIQUE,
                                              password STRING)""")
        if "Labels" not in tables:
            cu.execute("""CREATE TABLE Labels (labelId INTEGER PRIMARY KEY,
                                               label STRING UNIQUE)""")
        if "TroveNames" not in tables:
            cu.execute("""CREATE TABLE TroveNames (troveNameId INTEGER PRIMARY KEY,
                                                   troveName STRING UNIQUE)""")
        if "Permissions" not in tables:
            cu.execute("""CREATE TABLE Permissions (userId INTEGER,
                                                    labelId INTEGER,
                                                    troveNameId INTEGER,
                                                    write INTEGER)""")
            cu.execute("""CREATE INDEX PermissionsIdx
                          ON Permissions(userId, labelId, troveNameId)""")

class InsufficientPermission(Exception):
    pass
