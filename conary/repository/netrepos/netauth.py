#
# Copyright (c) 2004-2006 rPath, Inc.
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
import os
import re
import sys

from conary.repository import errors
from conary.lib.tracelog import logMe
from conary.dbstore import sqlerrors

# FIXME: remove these compatibilty error classes later
UserAlreadyExists = errors.UserAlreadyExists
GroupAlreadyExists = errors.GroupAlreadyExists

class NetworkAuthorization:
    def __init__(self, db, name):
        self.name = name
        self.db = db
        self.reCache = {}

    def getAuthGroups(self, cu, authToken):
        logMe(3, authToken[0], authToken[2], authToken[3])
        # Find what group this user belongs to
        # anonymous users should come through as anonymous, not None
        assert(authToken[0])
        cu.execute("""
        SELECT salt, password, userGroupId
        FROM Users JOIN UserGroupMembers USING(userId)
        WHERE userName = ?
        """, authToken[0])

        groupsFromUser = [ x for x in cu ]

        if groupsFromUser:
            # each user can only appear once (by constraint), so we only
            # need to validate the password once
            if not self.checkPassword(cu.frombinary(groupsFromUser[0][0]),
                                      groupsFromUser[0][1],
                                      authToken[1]):
                raise errors.InsufficientPermission

            groupsFromUser = [ x[2] for x in groupsFromUser ]

        if authToken[2] is not None:
            # look up entitlements
            cu.execute("""
            SELECT userGroupId
            FROM EntitlementGroups JOIN Entitlements USING(entGroupId)
            WHERE
            entGroup=? AND entitlement=?
            """, authToken[2], authToken[3])
            groupsFromUser += [ x[0] for x in cu ]

        return groupsFromUser

    def check(self, authToken, write = False, admin = False, label = None,
              trove = None, mirror = False):
        logMe(2, authToken[0], authToken[1], authToken[2], write, admin,
              label, trove)

        if label and label.getHost() != self.name:
            raise errors.RepositoryMismatch

        if not authToken[0]:
            return False

        cu = self.db.cursor()

        try:
            groupIds = self.getAuthGroups(cu, authToken)
        except errors.InsufficientPermission:
            return False

        if mirror:
            # admin access includes mirror access
            cu.execute("""
                SELECT userGroupId FROM 
                    UserGroups JOIN Permissions USING (userGroupId)
                    WHERE
                        userGroupId IN (%s) AND
                        (canMirror =1 OR admin = 1)
                """ % ",".join("%d" % x for x in groupIds))
            if not cu.fetchall():
                return False

        stmt = """
        select Items.item
        from Permissions join items using (itemId)
        """
        params = []
        where = []
        if len(groupIds):
            where.append("Permissions.userGroupId IN (%s)" %
                     ",".join("%d" % x for x in groupIds))
        if label:
            where.append("""
            (
            Permissions.labelId = 0 OR
            Permissions.labelId in
                ( select labelId from Labels where Labels.label = ? )
            )
            """)
            params.append(label.asString())

        if write:
            where.append("Permissions.canWrite=1")

        if admin:
            where.append("Permissions.admin=1")

        if where:
            stmt += "WHERE " + " AND ".join(where)

        logMe(3, stmt, params)
        cu.execute(stmt, params)

        for (troveName,) in cu:
            if troveName=='ALL' or not trove:
                regExp = None
            else:
                regExp = self.reCache.get(troveName, None)
                if regExp is None:
                    regExp = re.compile(troveName)
                    self.reCache[troveName] = regExp

            if not regExp or regExp.match(trove):
                return True

        return False

    def checkTrove(self, pattern, trove):
        if pattern=='ALL':
            return True

        regExp = self.reCache.get(pattern, None)
        if regExp is None:
            regExp = re.compile(pattern)
            self.reCache[trove] = regExp

        if regExp.match(trove):
            return True

        return False

    def checkPassword(self, salt, password, challenge):
        m = md5.new()
        m.update(salt)
        m.update(challenge)

        return m.hexdigest() == password

    def checkUserPass(self, authToken, label = None):
        logMe(2, authToken[0], label)
        if label and label.getHost() != self.name:
            raise errors.RepositoryMismatch

        cu = self.db.cursor()

        stmt = "SELECT salt, password FROM Users WHERE userName=?"
        cu.execute(stmt, authToken[0])

        for (salt, password) in cu:
            m = md5.new()
            m.update(salt)
            m.update(authToken[1])
            if m.hexdigest() == password:
                return True

        return False

    def checkIsFullAdmin(self, user, password):
        logMe(3, user)
        cu = self.db.cursor()
        cu.execute("""
        SELECT salt, password
        FROM Users as U
        JOIN UserGroupMembers as UGM USING(userId)
        JOIN Permissions as P USING(userGroupId)
        WHERE U.userName = ? and P.admin = 1
        """, user)
        for (salt, cryptPassword) in cu:
            if not self.checkPassword(salt, cryptPassword, password):
                return False
            return True
        return False

    def addAcl(self, userGroup, trovePattern, label, write, capped, admin):
        cu = self.db.cursor()

        if write:
            write = 1
        else:
            write = 0

        if capped:
            capped = 1
        else:
            capped = 0

        if admin:
            admin = 1
        else:
            admin = 0

        # XXX This functionality is available in the TroveStore class
        #     refactor so that the code is not in two places
        if trovePattern:
            cu.execute("SELECT * FROM Items WHERE item=?", trovePattern)
            itemId = cu.fetchone()
            if itemId:
                itemId = itemId[0]
            else:
                cu.execute("INSERT INTO Items (itemId, item) VALUES(NULL, ?)",
                           trovePattern)
                itemId = cu.lastrowid
        else:
            itemId = 0

        if label:
            cu.execute("SELECT * FROM Labels WHERE label=?", label)
            labelId = cu.fetchone()
            if labelId:
                labelId = labelId[0]
            else:
                cu.execute("INSERT INTO Labels (labelId, label) VALUES(NULL, ?)", label)
                labelId = cu.lastrowid
        else:
            labelId = 0

        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
                   userGroup)
        userGroupId = cu.next()[0]

        try:
            cu.execute("""
            INSERT INTO Permissions
                (userGroupId, labelId, itemId, canWrite, capped, admin)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (userGroupId, labelId, itemId, write, capped, admin))
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.PermissionAlreadyExists, "labelId: '%s', itemId: '%s'" % (labelId, itemId)

        self.db.commit()

    def editAcl(self, userGroup, oldTroveId, oldLabelId, troveId, labelId,
            write, capped, admin):

        cu = self.db.cursor()

        userGroupId = self.getGroupIdByName(userGroup)

        if write:
            write = 1
        else:
            write = 0

        if capped:
            capped = 1
        else:
            capped = 0

        if admin:
            admin = 1
        else:
            admin = 0

        try:
            cu.execute("""
            UPDATE Permissions
            SET labelId = ?, itemId = ?, canWrite = ?, capped = ?, admin = ?
            WHERE userGroupId=? AND labelId=? AND itemId=?""",
                       labelId, troveId, write, capped, admin,
                       userGroupId, oldLabelId, oldTroveId)
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.PermissionAlreadyExists, "labelId: '%s', itemId: '%s'" % (labelId, itemId)

        self.db.commit()

    def deleteAcl(self, userGroupId, labelId, itemId):
        cu = self.db.cursor()

        stmt = """DELETE FROM Permissions
                  WHERE userGroupId=? AND
                        (labelId=? OR (labelId IS NULL AND ? IS NULL)) AND
                        (itemId=? OR (itemId IS NULL AND ? IS NULL))"""

        cu.execute(stmt, userGroupId, labelId, labelId, itemId, itemId)
        self.db.commit()

    def _uniqueUser(self, cu, user):
        """
        Returns True if the username is unique.  Raises UserAlreadyExists
        if it is not unique
        """
        cu.execute("""
            SELECT COUNT(userId)
            FROM Users WHERE LOWER(userName)=LOWER(?)
        """, user)
        if cu.next()[0]:
            raise errors.UserAlreadyExists, 'user: %s' % user
        return True

    def _uniqueUserGroup(self, cu, usergroup):
        """
        Returns True if the username is unique.  Raises UserAlreadyExists
        if it is not unique
        """
        cu.execute("""
            SELECT COUNT(userGroupId)
            FROM UserGroups WHERE LOWER(UserGroup)=LOWER(?)
        """, usergroup)
        if cu.next()[0]:
            raise errors.GroupAlreadyExists, 'usergroup: %s' % usergroup
        return True

    def addUser(self, user, password):
        salt = os.urandom(4)

        m = md5.new()
        m.update(salt)
        m.update(password)
        return self.addUserByMD5(user, salt, m.hexdigest())

    def setMirror(self, userGroup, canMirror):
        cu = self.db.cursor()
        cu.execute("update userGroups set canMirror=? where userGroup=?",
                   canMirror, userGroup)
        self.db.commit()

    def addUserByMD5(self, user, salt, password):

        #Insert the UserGroup first, but since usergroups can be added
        #and deleted at will, and sqlite uses a MAX(id)+1 approach to
        #sequencing, use max(userId, userGroupId)+1 so that userId and
        #userGroupId can be in sync.  This will leave lots of holes, and
        #will probably need to be changed if conary moves to another db.
        cu = self.db.transaction()

        #Check to make sure the user is unique in both the user and UserGroup
        #tables
        self._uniqueUserGroup(cu, user)
        self._uniqueUser(cu, user)

        # FIXME: race condition - fix it using sequences once they are
        # provisioned
        cu.execute("""
        SELECT MAX(maxId)+1 FROM (
            SELECT COALESCE(MAX(userId),0) as maxId FROM Users
            UNION
            SELECT COALESCE(MAX(userGroupId),0) as maxId FROM UserGroups
        ) as MaxList
        """)
        ugid = cu.fetchone()[0]
        # XXX: ahhh, how we miss real sequences...
        try:
            cu.execute("INSERT INTO UserGroups (userGroupId, userGroup) "
                       "VALUES (?, ?)",
                       (ugid, user))
        except sqlerrors.ColumnNotUnique:
            raise errors.GroupAlreadyExists, 'group: %s' % user
        try:
            cu.execute("INSERT INTO Users (userId, userName, salt, password) "
                       "VALUES (?, ?, ?, ?)",
                       (ugid, user, cu.binary(salt), cu.binary(password)))
        except sqlerrors.ColumnNotUnique:
            raise errors.UserAlreadyExists, 'user: %s' % user
        logMe(3, "salt", salt, len(salt),
              "dbsalt", cu.execute("select salt from Users where userId = ?", ugid).fetchone()[0])
        cu.execute("INSERT INTO UserGroupMembers (userGroupId, userId) "
                   "VALUES (?, ?)",
                   (ugid, ugid))
        self.db.commit()
        return ugid

    def deleteUserByName(self, user, commit = True):
        cu = self.db.cursor()

        sql = "SELECT userId FROM Users WHERE userName=?"
        cu.execute(sql, user)
        try:
            userId = cu.next()[0]
        except StopIteration:
            raise errors.UserNotFound(user)

        return self.deleteUser(userId, user, commit)

    def deleteUserById(self, userId, commit = True):
        cu = self.db.cursor()

        sql = "SELECT userName FROM Users WHERE userId=?"
        cu.execute(sql, userId)
        try:
            user = cu.next()[0]
        except StopIteration:
            raise errors.UserNotFound(user)

        return self.deleteUser(userId, user, commit)

    def deleteUser(self, userId, user, commit = True):
        # Need to do a lot of stuff:
        # UserGroups, Users, and all ACLs

        cu = self.db.cursor()

        try:
            #First delete the user from all the groups
            sql = "DELETE from UserGroupMembers WHERE userId=?"
            cu.execute(sql, userId)


            #Then delete the UserGroup created with the name of that user
            try:
                self.deleteGroup(user, False)
            except StopIteration, e:
                # Ignore the StopIteration error as it means there was no
                # Group matching that name.  Probably because the group
                # was deleted beforehand
                pass

            #Now delete the user-self
            sql = "DELETE from Users WHERE userId=?"
            cu.execute(sql, userId)

            if commit:
                self.db.commit()
        except Exception, e:
            if commit:
                self.db.rollback()

            raise e
        return True

    def changePassword(self, user, newPassword):
        cu = self.db.cursor()

        salt = os.urandom(4)

        m = md5.new()
        m.update(salt)
        m.update(newPassword)
        password = m.hexdigest()

        cu.execute("UPDATE Users SET password=?, salt=? WHERE userName=?",
                   cu.binary(password), cu.binary(salt), user)
        self.db.commit()

    def getUserGroups(self, user):
        cu = self.db.cursor()
        cu.execute("""SELECT UserGroups.userGroup
                      FROM UserGroups, Users, UserGroupMembers
                      WHERE UserGroups.userGroupId = UserGroupMembers.userGroupId AND
                            UserGroupMembers.userId = Users.userId AND
                            Users.userName = ?""", user)

        return [row[0] for row in cu]

    def iterUsers(self):
        cu = self.db.cursor()
        cu.execute("SELECT userId, userName FROM Users")

        for row in cu:
            yield row

    def iterGroups(self):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId, userGroup FROM UserGroups")

        for row in cu:
            yield row

    def iterGroupsByUserId(self, userId):
        cu = self.db.cursor()
        cu.execute("""SELECT UserGroups.userGroupId, UserGroups.userGroup
                      FROM UserGroups INNER JOIN UserGroupMembers ON
                      UserGroups.userGroupId = UserGroupMembers.userGroupId
                      WHERE UserGroupMembers.userId=?""", userId)

        for row in cu:
            yield row

    def iterGroupMembers(self, userGroupId):
        cu = self.db.cursor()
        cu.execute("""SELECT Users.userName FROM UserGroupMembers, Users
                      WHERE Users.userId = UserGroupMembers.userId AND
                      UserGroupMembers.userGroupId=?""", userGroupId)

        for row in cu:
            yield row[0]

    def iterPermsByGroupId(self, userGroupId):
        cu = self.db.cursor()
        cu.execute("""SELECT Permissions.labelId, Labels.label,
                             PerItems.itemId, PerItems.item,
                             canwrite, capped, admin
                      FROM Permissions
                      LEFT OUTER JOIN Items AS PerItems ON
                          PerItems.itemId = Permissions.itemId
                      LEFT OUTER JOIN Labels ON
                          Permissions.labelId = Labels.labelId
                      WHERE userGroupId=?""", userGroupId)

        for row in cu:
            yield row

    def getGroupNameById(self, userGroupId):
        cu = self.db.cursor()

        cu.execute("SELECT userGroup from UserGroups WHERE userGroupId=?",
            userGroupId)

        return cu.next()[0]

    def getGroupIdByName(self, userGroupName):
        cu = self.db.cursor()

        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
            userGroupName)

        return cu.next()[0]

    def getUserIdByName(self, userName):
        cu = self.db.cursor()

        cu.execute("SELECT userId FROM Users WHERE userName=?",
                   userName)

        return cu.next()[0]

    def addGroup(self, userGroupName):
        cu = self.db.cursor()

        #Check to make sure the group is unique
        self._uniqueUserGroup(cu, userGroupName)

        try:
            cu.execute("INSERT INTO UserGroups (userGroup) VALUES (?)", userGroupName)
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.GroupAlreadyExists, "group: %s" % userGroupName
        self.db.commit()

        return cu.lastrowid

    def renameGroup(self, userGroupId, userGroupName):
        cu = self.db.cursor()

        #See if we're actually going to do any work:
        currentGroupName = self.getGroupNameById(userGroupId)
        if currentGroupName != userGroupName:
            if currentGroupName.lower() != userGroupName.lower():
                #Check to make sure the group is unique
                self._uniqueUserGroup(cu, userGroupName)
            #else we're just changing case.

            try:
                cu.execute("UPDATE UserGroups SET userGroup=? WHERE userGroupId=?", userGroupName, userGroupId)
            except sqlerrors.ColumnNotUnique:
                self.db.rollback()
                raise errors.GroupAlreadyExists, "group: %s" % userGroupName

            self.db.commit()

    def updateGroupMembers(self, userGroupId, members):
        #Do this in a transaction
        cu = self.db.cursor()

        #First drop all the current members
        cu.execute ("DELETE FROM UserGroupMembers WHERE userGroupId=?", userGroupId)

        #now add the new members
        for userId in members:
            self.addGroupMember(userGroupId, userId, False)

        self.db.commit()

    def addGroupMember(self, userGroupId, userId, commit = True):
        cu = self.db.cursor()

        cu.execute("INSERT INTO UserGroupMembers (userGroupId, userId) VALUES(?, ?)",
                   (userGroupId, userId))
        if commit:
            self.db.commit()

    def deleteGroup(self, userGroupName, commit = True):
        return self.deleteGroupById(self.getGroupIdByName(userGroupName), commit)

    def deleteGroupById(self, userGroupId, commit = True):
        cu = self.db.cursor()
        cu.execute("DELETE FROM Permissions WHERE userGroupId=?", userGroupId)
        cu.execute("DELETE FROM UserGroupMembers WHERE userGroupId=?", userGroupId)
        cu.execute("DELETE FROM UserGroups WHERE userGroupId=?", userGroupId)

        #Note, there could be a user left behind with no associated group
        #if the group being deleted was created with a user.  This user is not
        #deleted because it is possible for this user to be a member of
        #another group.
        if commit:
            self.db.commit()

    def iterItems(self):
        cu = self.db.cursor()

        cu.execute("SELECT itemId, item FROM Items")
        for row in cu:
            yield row

    def iterLabels(self):
        cu = self.db.cursor()

        cu.execute("SELECT labelId, label FROM Labels")
        for row in cu:
            yield row

    def __checkEntitlementOwner(self, cu, userName, entGroup):
        """
        Raises an error or returns the group Id.
        """
        # verify that the user has permission to change this entitlement
        # group
        cu.execute("""
                SELECT entGroupId, admin
                    FROM Users JOIN UserGroupMembers ON
                        UserGroupMembers.userId = Users.userId
                    LEFT OUTER JOIN Permissions ON
                        UserGroupMembers.userGroupId = Permissions.userGroupId
                    LEFT OUTER JOIN EntitlementOwners ON
                        UserGroupMembers.userGroupId = \
                                EntitlementOwners.ownerGroupId
                    WHERE
                        Users.userName = ?
                        AND
                          (EntitlementOwners.ownerGroupId IS NOT NULL OR
                           Permissions.admin = 1)
                """, userName)

        isAdmin = False
        entGroupsEditable = []
        for (entGroupId, rowIsAdmin) in cu:
            if entGroupId is not None:
                entGroupsEditable.append(entGroupId)
            isAdmin = isAdmin or rowIsAdmin

        cu.execute("SELECT entGroupId FROM EntitlementGroups WHERE "
                   "entGroup = ?", entGroup)
        entGroupIds = [ x[0] for x in cu ]

        if len(entGroupIds) == 1:
            entGroupId = entGroupIds[0]
        else:
            assert(not entGroupIds)
            entGroupId = -1

        if isAdmin:
            if not entGroupIds:
                raise errors.UnknownEntitlementGroup

            return entGroupId
        elif entGroupId in entGroupsEditable:
            return entGroupId

        raise errors.InsufficientPermission

    def addEntitlement(self, authToken, entGroup, entitlement):
        cu = self.db.cursor()

        # validate the password
        if len(entitlement) > 64 or not self.checkUserPass(authToken):
            return errors.InsufficientPermission

        entGroupId = self.__checkEntitlementOwner(cu, authToken[0], entGroup)

        # check for duplicates
        cu.execute("""
                SELECT COUNT(*) FROM Entitlements WHERE
                    entGroupId = ? AND entitlement = ?
                """, entGroupId, entitlement)
        count = cu.next()[0]
        if count:
            raise UserAlreadyExists

        cu.execute("INSERT INTO Entitlements (entGroupId, entitlement) VALUES (?, ?)",
                   (entGroupId, entitlement))

        self.db.commit()

    def addEntitlementGroup(self, authToken, entGroup, userGroup):
        cu = self.db.cursor()

        if not self.check(authToken, admin = True):
            raise errors.InsufficientPermission

        # check for duplicate
        cu.execute("SELECT COUNT(*) FROM EntitlementGroups WHERE "
                   "entGroup = ?", entGroup)
        if cu.next()[0]:
            raise errors.GroupAlreadyExists

        cu.execute("SELECT userGroupId FROM userGroups WHERE userGroup=?",
                   userGroup)
        l = [ x for x in cu ]
        if not l:
            raise errors.GroupNotFound

        assert(len(l) == 1)
        userGroupId = l[0][0]

        cu.execute("INSERT INTO EntitlementGroups (entGroupId, entGroup, userGroupId) "
                   "VALUES (NULL, ?, ?)",
                   (entGroup, userGroupId))

        self.db.commit()

    def addEntitlementOwnerAcl(self, authToken, userGroup, entGroup):
        """
        Gives the userGroup ownership permission for the entGroup entitlement
        set.
        """
        if not self.check(authToken, admin = True):
            raise errors.InsufficientPermission

        cu = self.db.cursor()

        entGroupId = cu.execute("SELECT entGroupId FROM entitlementGroups "
                                "WHERE entGroup = ?", entGroup).next()[0]
        userGroupId = cu.execute("SELECT userGroupId FROM userGroups "
                                 "WHERE userGroup = ?", userGroup).next()[0]

        cu.execute("INSERT INTO EntitlementOwners (entGroupId, ownerGroupId) "
                   "VALUES (?, ?)",
                   (entGroupId, userGroupId))

    def iterEntitlements(self, authToken, entGroup):
        # validate the password
        if not self.checkUserPass(authToken):
            return errors.InsufficientPermission

        cu = self.db.cursor()

        entGroupId = self.__checkEntitlementOwner(cu, authToken[0], entGroup)

        cu.execute("SELECT entitlement FROM Entitlements WHERE "
                   "entGroupId = ?", entGroupId)

        return [ x[0] for x in cu ]

