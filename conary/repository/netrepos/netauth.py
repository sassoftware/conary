#
# Copyright (c) 2004-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import md5
import os
import re

from conary.repository import errors
from conary.lib import tracelog
from conary.dbstore import sqlerrors

# FIXME: remove these compatibilty error classes later
UserAlreadyExists = errors.UserAlreadyExists
GroupAlreadyExists = errors.GroupAlreadyExists

class UserAuthorization:

    def addUserByMD5(self, cu, user, salt, password, ugid):
        try:
            cu.execute("INSERT INTO Users (userName, salt, password) "
                       "VALUES (?, ?, ?)",
                       (user, cu.binary(salt), cu.binary(password)))
            uid = cu.lastrowid
        except sqlerrors.ColumnNotUnique:
            raise errors.UserAlreadyExists, 'user: %s' % user

        # make sure we don't conflict with another entry based on case; this
        # avoids races from other processes adding case differentiated
        # duplicates
        cu.execute("""
            SELECT COUNT(userId)
            FROM Users WHERE LOWER(userName)=LOWER(?)
        """, user)
        if cu.next()[0] > 1:
            raise errors.UserAlreadyExists, 'user: %s' % user

        cu.execute("INSERT INTO UserGroupMembers (userGroupId, userId) "
                   "VALUES (?, ?)",
                   (ugid, uid))

        return uid

    def checkPassword(self, salt, password, challenge):
        m = md5.new()
        m.update(salt)
        m.update(challenge)
        return m.hexdigest() == password

    def checkUserPass(self, cu, authToken):
        cu.execute("SELECT salt, password FROM Users WHERE userName=?", 
                   authToken[0])

        for (salt, password) in cu:
            m = md5.new()
            m.update(salt)
            m.update(authToken[1])
            if m.hexdigest() == password:
                return True

        return False

    def getAuthorizedGroups(self, cu, user, password):
        cu.execute("""
        SELECT salt, password, userGroup FROM Users 
        JOIN UserGroupMembers USING(userId)
        JOIN UserGroups USING (userGroupId)
        WHERE userName = ?
        """, user)

        groupsFromUser = [ x for x in cu ]

        if groupsFromUser:
            # each user can only appear once (by constraint), so we only
            # need to validate the password once
            if not self.checkPassword(cu.frombinary(groupsFromUser[0][0]),
                                      groupsFromUser[0][1],
                                      password):
                raise errors.InsufficientPermission

            groupsFromUser = set(x[2] for x in groupsFromUser)
        else:
            return set()

        return groupsFromUser

    def getGroupsByUser(self, user):
        cu = self.db.cursor()
        cu.execute("""SELECT userGroup FROM Users
                        JOIN UserGroupMembers USING (userId)
                        JOIN UserGroups USING (userGroupId)
                        WHERE Users.userName = ?""", user)

        return [ x[0] for x in cu ]

    def getUserList(self):
        cu = self.db.cursor()
        cu.execute("SELECT userName FROM Users")
        return [ x[0] for x in cu ]

    def __init__(self, db):
        self.db = db

class EntitlementAuthorization:

    def getAuthorizedGroups(self, cu, entitlementGroup, entitlement):
        # look up entitlements
        cu.execute("""
        SELECT userGroup FROM EntitlementGroups
        JOIN Entitlements USING (entGroupId)
        JOIN UserGroups ON
            EntitlementGroups.userGroupId = UserGroups.userGroupId
        WHERE
        entGroup=? AND entitlement=?
        """, entitlementGroup, entitlement)

        return set(x[0] for x in cu)

class NetworkAuthorization:
    def __init__(self, db, name, log = None):
        self.name = name
        self.db = db
        self.reCache = {}
        self.log = log or tracelog.getLog(None)
        self.userAuth = UserAuthorization(self.db)
        self.entitlementAuth = EntitlementAuthorization()

    def getAuthGroups(self, cu, authToken):
        self.log(3, authToken[0], authToken[2], authToken[3])
        # Find what group this user belongs to
        # anonymous users should come through as anonymous, not None
        assert(authToken[0])
        groupsFromUser = self.userAuth.getAuthorizedGroups(cu, authToken[0],
                                                           authToken[1])
        if authToken[2] is not None:
            groupsFromEntitlement = \
                  self.entitlementAuth.getAuthorizedGroups(cu, authToken[2],
                                                           authToken[3])
            groupsFromUser.update(groupsFromEntitlement)

        # We have lists of symbolic names; get lists of group ids
        cu.execute("SELECT userGroupId FROM UserGroups WHERE "
                   "userGroup IN (%s)" %
                        ",".join("'%s'" % x for x in groupsFromUser) )

        return [ x[0] for x in cu ]

    def check(self, authToken, write = False, admin = False, label = None,
              trove = None, mirror = False):
        self.log(3, authToken[0],
                 "entitlement=%s write=%s admin=%s label=%s trove=%s mirror=%s" %(
            authToken[2], int(bool(write)), int(bool(admin)), label, trove, int(bool(mirror))))

        if label and label.getHost() != self.name:
            raise errors.RepositoryMismatch(self.name, label.getHost())

        if not authToken[0]:
            return False

        cu = self.db.cursor()

        try:
            groupIds = self.getAuthGroups(cu, authToken)
        except errors.InsufficientPermission:
            return False

        if len(groupIds) < 1:
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

        self.log(4, stmt, params)
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

    def addAcl(self, userGroup, trovePattern, label, write, capped, admin):
        self.log(3, userGroup, trovePattern, label, write, admin)
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

        userGroupId = self.getGroupIdByName(userGroup)

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

    def addUser(self, user, password):
        self.log(3, user)

        salt = os.urandom(4)
        m = md5.new()
        m.update(salt)
        m.update(password)

        return self.addUserByMD5(user, salt, m.hexdigest())

        return ugid

    def setMirror(self, userGroup, canMirror):
        self.log(3, userGroup, canMirror)
        cu = self.db.transaction()
        cu.execute("update userGroups set canMirror=? where userGroup=?",
                   canMirror, userGroup)
        self.db.commit()

    def addUserByMD5(self, user, salt, password):
        self.log(3, user)
        cu = self.db.transaction()

        ugid = self._addGroup(cu, user)
        uid = self.userAuth.addUserByMD5(cu, user, salt, password, ugid)

        self.db.commit()

        return uid

    def deleteUserByName(self, user, commit = True):
        self.log(3, user)
        cu = self.db.cursor()
        sql = "SELECT userId FROM Users WHERE userName=?"
        cu.execute(sql, user)
        try:
            userId = cu.next()[0]
        except StopIteration:
            raise errors.UserNotFound(user)
        return self.deleteUser(userId, user, commit)

    def deleteUser(self, userId, user, commit = True):
        # Need to do a lot of stuff:
        # UserGroups, Users, and all ACLs
        self.log(3, userId, user)
        cu = self.db.cursor()

        try:
            #First delete the user from all the groups
            sql = "DELETE from UserGroupMembers WHERE userId=?"
            cu.execute(sql, userId)


            #Then delete the UserGroup created with the name of that user
            try:
                self.deleteGroup(user, False)
            except errors.GroupNotFound, e:
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
        self.log(3, user)
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

    def iterGroups(self):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId, userGroup FROM UserGroups")
        for row in cu:
            yield row

    def iterGroupMembers(self, userGroupId):
        cu = self.db.cursor()
        cu.execute("""SELECT Users.userName FROM UserGroupMembers, Users
                      WHERE Users.userId = UserGroupMembers.userId AND
                      UserGroupMembers.userGroupId=?""", userGroupId)
        for row in cu:
            yield row[0]

    def iterPermsByGroup(self, userGroupName):
        cu = self.db.cursor()
        cu.execute("""SELECT Permissions.labelId, Labels.label,
                             PerItems.itemId, PerItems.item,
                             canwrite, capped, admin
                      FROM UserGroups
                      JOIN Permissions USING (userGroupId)
                      LEFT OUTER JOIN Items AS PerItems ON
                          PerItems.itemId = Permissions.itemId
                      LEFT OUTER JOIN Labels ON
                          Permissions.labelId = Labels.labelId
                      WHERE userGroup=?""", userGroupName)
        for row in cu:
            yield row

    def getGroupIdByName(self, userGroupName):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
            userGroupName)

        try:
            return cu.next()[0]
        except:
            raise errors.GroupNotFound

    def getUserIdByName(self, userName):
        cu = self.db.cursor()
        cu.execute("SELECT userId FROM Users WHERE userName=?",
                   userName)
        return cu.next()[0]

    def _addGroup(self, cu, userGroupName):
        cu = self.db.transaction()
        try:
            cu.execute("INSERT INTO UserGroups (userGroup) VALUES (?)", 
                       userGroupName)
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.GroupAlreadyExists, "group: %s" % userGroupName

        # check for case insensitive user conflicts -- avoids race with
        # other adders on case-differentiated names
        cu.execute("""
            SELECT COUNT(userGroupId)
            FROM UserGroups WHERE LOWER(UserGroup)=LOWER(?)
        """, userGroupName)
        if cu.next()[0] > 1:
            raise errors.GroupAlreadyExists, 'usergroup: %s' % userGroupName

        return cu.lastrowid

    def addGroup(self, userGroupName):
        cu = self.db.transaction()
        ugid = self._addGroup(cu, userGroupName)
        self.db.commit()
        return ugid;

    def renameGroup(self, currentGroupName, userGroupName):
        cu = self.db.cursor()
        #See if we're actually going to do any work:

        try:
            userGroupId = self.getGroupIdByName(currentGroupName)
        except errors.GroupNotFound:
            return

        if currentGroupName != userGroupName:
            try:
                cu.execute("UPDATE UserGroups SET userGroup=? WHERE userGroupId=?", userGroupName, userGroupId)
            except sqlerrors.ColumnNotUnique:
                self.db.rollback()
                raise errors.GroupAlreadyExists, "group: %s" % userGroupName

            # check for case-differentiated duplicates
            cu.execute("""
                SELECT COUNT(userGroupId)
                FROM UserGroups WHERE LOWER(UserGroup)=LOWER(?)
            """, userGroupName)
            if cu.next()[0] > 1:
                raise errors.GroupAlreadyExists, 'usergroup: %s' % userGroupName

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
        if not self.userAuth.checkUserPass(cu, authToken):
            raise errors.InsufficientPermission
        self.log(2, "entGroup=%s entitlement=%s" % (entGroup, entitlement))

        if len(entitlement) > 64:
            raise errors.InvalidEntitlement

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
        self.log(2, "entGroup=%s userGroup=%s" % (entGroup, userGroup))

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
        self.log(2, "userGroup=%s entGroup=%s" % (userGroup, entGroup))
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
        cu = self.db.cursor()
        if not self.userAuth.checkUserPass(cu, authToken):
            return errors.InsufficientPermission
        entGroupId = self.__checkEntitlementOwner(cu, authToken[0], entGroup)
        cu.execute("SELECT entitlement FROM Entitlements WHERE "
                   "entGroupId = ?", entGroupId)
        return [ x[0] for x in cu ]

