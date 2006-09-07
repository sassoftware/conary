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
import time
import urllib, urllib2
import xml

from conary import conarycfg
from conary.repository import errors
from conary.lib import sha1helper, tracelog
from conary.dbstore import sqlerrors

# FIXME: remove these compatibilty error classes later
UserAlreadyExists = errors.UserAlreadyExists
GroupAlreadyExists = errors.GroupAlreadyExists

class UserAuthorization:
    def __init__(self, db, pwCheckUrl = None, cacheTimeout = None):
        self.db = db
        self.pwCheckUrl = pwCheckUrl
        self.cacheTimeout = cacheTimeout
        self.pwCache = {}


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

    def changePassword(self, cu, user, salt, password):
        if self.pwCheckUrl:
            raise errors.CannotChangePassword

        cu.execute("UPDATE Users SET password=?, salt=? WHERE userName=?",
                   cu.binary(password), cu.binary(salt), user)

    def _checkPassword(self, user, salt, password, challenge):
        if self.cacheTimeout:
            cacheEntry = sha1helper.sha1String("%s%s" % (user, challenge))
            timeout = self.pwCache.get(cacheEntry, None)
            if timeout is not None and timeout < time.time():
                return True

        if self.pwCheckUrl:
            try:
                url = "%s?user=%s;password=%s" \
                        % (self.pwCheckUrl, urllib.quote(user),
                           urllib.quote(challenge))
                f = urllib2.urlopen(url)
                xmlResponse = f.read()
            except:
                return False

            p = PasswordCheckParser()
            p.parse(xmlResponse)

            isValid = p.validPassword()
        else:
            m = md5.new()
            m.update(salt)
            m.update(challenge)
            isValid = m.hexdigest() == password

        if isValid and self.cacheTimeout:
            # cacheEntry is still around from above
            self.pwCache[cacheEntry] = time.time() + self.cacheTimeout

        return isValid

    def deleteUser(self, cu, user):
        userId = self.getUserIdByName(user)

        # First delete the user from all the groups
        sql = "DELETE from UserGroupMembers WHERE userId=?"
        cu.execute(sql, userId)

        # Now delete the user itself
        sql = "DELETE from Users WHERE userId=?"
        cu.execute(sql, userId)

    def getAuthorizedGroups(self, cu, user, password):
        cu.execute("""
        SELECT salt, password, userGroupId FROM Users
        JOIN UserGroupMembers USING(userId)
        WHERE userName = ?
        """, user)

        groupsFromUser = [ x for x in cu ]

        if groupsFromUser:
            # each user can only appear once (by constraint), so we only
            # need to validate the password once
            if not self._checkPassword(user,
                                      cu.frombinary(groupsFromUser[0][0]),
                                      groupsFromUser[0][1],
                                      password):
                return set()

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

    def getUserIdByName(self, userName):
        cu = self.db.cursor()

        cu.execute("SELECT userId FROM Users WHERE userName=?", userName)
        try:
            return cu.next()[0]
        except:
            raise errors.UserNotFound(userName)

    def getUserList(self):
        cu = self.db.cursor()
        cu.execute("SELECT userName FROM Users")
        return [ x[0] for x in cu ]


class EntitlementAuthorization:
    def __init__(self, entCheckUrl = None, cacheTimeout = None):
        self.entCheckUrl = entCheckUrl
        self.cacheTimeout = cacheTimeout
        self.cache = {}

    def getAuthorizedGroups(self, cu, serverName, entitlementGroup, entitlement):
        if self.cacheTimeout:
            cacheEntry = sha1helper.sha1String("%s%s%s" % (
                serverName, entitlementGroup, entitlement))
            userGroupIds, timeout = self.cache.get(cacheEntry, (None, None))
            if timeout is not None and (timeout < time.time()):
                return userGroupIds

        if self.entCheckUrl:
            try:
                url = "%s?server=%s;class=%s;key=%s" \
                        % (self.entCheckUrl, urllib.quote(serverName),
                           urllib.quote(entitlementGroup),
                           urllib.quote(entitlement))
                f = urllib2.urlopen(url)
                xmlResponse = f.read()
            except Exception, e:
                return set()

            p = conarycfg.EntitlementParser()

            try:
                p.parse(xmlResponse)
            except:
                return set()

            if p['server'] != serverName:
                return set()

            entitlementGroup = p['class']
            entitlement = p['key']

        # look up entitlements
        cu.execute("""
        SELECT userGroupId FROM EntitlementGroups
        JOIN Entitlements USING (entGroupId)
        WHERE entGroup=? AND entitlement=?
        """, entitlementGroup, entitlement)

        userGroupIds = set(x[0] for x in cu)
        if self.cacheTimeout:
            # cacheEntry is still set from the cache check above
            self.cache[cacheEntry] = (userGroupIds,
                                      time.time() + self.cacheTimeout)

        return userGroupIds


class NetworkAuthorization:
    def __init__(self, db, serverNameList, cacheTimeout = None, log = None,
                 passwordURL = None, entCheckURL = None):
        """
        @param cacheTimeout: Timeout, in seconds, for authorization cache
        entries. If None, no cache is used.
        @type cacheTimeout: int
        @param passwordURL: URL base to use for an http get request to
        externally validate user passwords. When this is specified, the
        passwords int the local database are ignored, and the changePassword()
        call is disabled.
        @param entCheckURL: URL base for mapping an entitlement received
        over the network to an entitlement to check for in the database.
        """
        self.serverNameList = serverNameList
        self.db = db
        self.log = log or tracelog.getLog(None)
        self.userAuth = UserAuthorization(
            self.db, passwordURL, cacheTimeout = cacheTimeout)
        self.entitlementAuth = EntitlementAuthorization(
            cacheTimeout = cacheTimeout, entCheckUrl = entCheckURL)

    def getAuthGroups(self, cu, authToken):
        self.log(3, authToken[0], authToken[2], authToken[3])
        # Find what group this user belongs to
        # anonymous users should come through as anonymous, not None
        assert(authToken[0])

        # we need a hashable tuple, a list won't work
        authToken = tuple(authToken)

        groupSet = self.userAuth.getAuthorizedGroups(cu, authToken[0],
                                                           authToken[1])
        if authToken[2] is not None:
            for serverName in self.serverNameList:
                groupsFromEntitlement = self.entitlementAuth.getAuthorizedGroups(
                    cu, serverName, authToken[2], authToken[3])
                groupSet.update(groupsFromEntitlement)

        return groupSet

    def check(self, authToken, write = False, admin = False, label = None,
              trove = None, mirror = False):
        self.log(3, authToken[0],
                 "entitlement=%s write=%s admin=%s label=%s trove=%s mirror=%s" %(
            authToken[2], int(bool(write)), int(bool(admin)), label, trove, int(bool(mirror))))

        if label and label.getHost() not in self.serverNameList:
            raise errors.RepositoryMismatch(self.serverNameList, label.getHost())

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

        for (pattern,) in cu:
            if self.checkTrove(pattern, trove):
                return True

        return False

    def checkTrove(self, pattern, trove):
        if pattern == 'ALL' or trove is None:
            return True
        regExp = re.compile(pattern + '$')
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

        userGroupId = self._getGroupIdByName(userGroup)

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

        userGroupId = self._getGroupIdByName(userGroup)

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

    def deleteAcl(self, userGroup, label, item):
        cu = self.db.cursor()

        userGroupId = self._getGroupIdByName(userGroup)

        if label is None:
            labelId = 0
        else:
            labelId = cu.execute("SELECT labelId FROM Labels WHERE label=?", 
                                 label).next()[0]

        if item is None:
            itemId = 0
        else:
            itemId = cu.execute("SELECT itemId FROM Items WHERE item=?", 
                                item).next()[0]

        stmt = """DELETE FROM Permissions
                  WHERE userGroupId=? AND labelId=? AND itemId=?"""

        cu.execute(stmt, userGroupId, labelId, itemId)
        self.db.commit()

    def addUser(self, user, password):
        self.log(3, user)

        salt = os.urandom(4)
        m = md5.new()
        m.update(salt)
        m.update(password)

        self.addUserByMD5(user, salt, m.hexdigest())

    def groupCanMirror(self, userGroup):
        cu = self.db.cursor()
        cu.execute("SELECT canMirror FROM UserGroups WHERE userGroup=?",
                   userGroup)
        try:
            canMirror = cu.next()[0]
        except:
            raise errors.GroupNotFound

        return canMirror != 0

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

    def deleteUserByName(self, user):
        self.log(3, user)

        cu = self.db.cursor()

        # delete the UserGroup created with the name of that user
        try:
            self.deleteGroup(user, False)
        except errors.GroupNotFound, e:
            pass

        self.userAuth.deleteUser(cu, user)

        self.db.commit()

    def changePassword(self, user, newPassword):
        self.log(3, user)
        salt = os.urandom(4)
        m = md5.new()
        m.update(salt)
        m.update(newPassword)

        cu = self.db.cursor()
        self.userAuth.changePassword(cu, user, salt, m.hexdigest())
        self.db.commit()

    def getUserGroups(self, user):
        cu = self.db.cursor()
        cu.execute("""SELECT UserGroups.userGroup
                      FROM UserGroups, Users, UserGroupMembers
                      WHERE UserGroups.userGroupId = UserGroupMembers.userGroupId AND
                            UserGroupMembers.userId = Users.userId AND
                            Users.userName = ?""", user)
        return [row[0] for row in cu]

    def getGroupList(self):
        cu = self.db.cursor()
        cu.execute("SELECT userGroup FROM UserGroups")
        return [ x[0] for x in cu ]

    def getGroupMembers(self, userGroup):
        cu = self.db.cursor()
        cu.execute("""SELECT Users.userName FROM UserGroups
                            JOIN UserGroupMembers USING (userGroupId)
                            JOIN Users USING (userId)
                            WHERE userGroup = ? """, userGroup)
        return [ x[0] for x in cu ]

    def iterPermsByGroup(self, userGroupName):
        cu = self.db.cursor()
        cu.execute("""SELECT Labels.label,
                             PerItems.item,
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

    def _getGroupIdByName(self, userGroupName):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
            userGroupName)

        try:
            return cu.next()[0]
        except:
            raise errors.GroupNotFound

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
        return ugid

    def renameGroup(self, currentGroupName, userGroupName):
        cu = self.db.cursor()
        #See if we're actually going to do any work:

        try:
            userGroupId = self._getGroupIdByName(currentGroupName)
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

    def updateGroupMembers(self, userGroup, members):
        #Do this in a transaction
        cu = self.db.cursor()
        userGroupId = self._getGroupIdByName(userGroup)

        #First drop all the current members
        cu.execute ("DELETE FROM UserGroupMembers WHERE userGroupId=?", userGroupId)
        #now add the new members
        for userId in members:
            self.addGroupMember(userGroup, userId, False)
        self.db.commit()

    def addGroupMember(self, userGroup, userName, commit = True):
        cu = self.db.cursor()
        # we do this in multiple select to let us generate the proper 
        # exceptions when the names don't xist
        userGroupId = self._getGroupIdByName(userGroup)
        userId = self.userAuth.getUserIdByName(userName)

        cu.execute("""INSERT INTO UserGroupMembers (userGroupId, userId)
                        VALUES (?, ?)""", userGroupId, userId)

        if commit:
            self.db.commit()

    def deleteGroup(self, userGroupName, commit = True):
        self.deleteGroupById(self._getGroupIdByName(userGroupName), 
                                    commit)

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

    def getItemList(self):
        cu = self.db.cursor()
        cu.execute("SELECT item FROM Items")
        return [ x[0] for x in cu ]

    def getLabelList(self):
        cu = self.db.cursor()
        cu.execute("SELECT label FROM Labels")
        return [ x[0] for x in cu ]

    def __checkEntitlementOwner(self, cu, authGroupIds, entGroup):
        """
        Raises an error or returns the group Id.
        """
        if not authGroupIds:
            raise errors.InsufficientPermission

        # verify that the user has permission to change this entitlement
        # group
        cu.execute("""
            SELECT entGroupId FROM EntitlementGroups 
                JOIN EntitlementOwners USING (entGroupId)
                WHERE 
                    ownerGroupId IN (%s)
                  AND
                    entGroup = ?
        """ % ",".join(str(x) for x in authGroupIds), entGroup)

        entGroupIdList = [ x[0] for x in cu ]
        if entGroupIdList:
            assert(max(entGroupIdList) == min(entGroupIdList))
            return entGroupIdList[0]

        # admins can do everything
        cu.execute("""
                SELECT COUNT(*)
                    FROM Permissions
                    WHERE
                        Permissions.userGroupId IN (%s)
                      AND
                       Permissions.admin = 1
        """ % ",".join(str(x) for x in authGroupIds))

        if cu.next()[0] == 0:
            raise errors.InsufficientPermission

        cu.execute("SELECT entGroupId FROM EntitlementGroups WHERE "
                   "entGroup = ?", entGroup)
        entGroupIds = [ x[0] for x in cu ]

        if len(entGroupIds) == 1:
            entGroupId = entGroupIds[0]
        else:
            assert(not entGroupIds)
            entGroupId = -1

        return entGroupId

    def deleteEntitlementGroup(self, authToken, entGroup):
        cu = self.db.cursor()
        if not self.check(authToken, admin = True):
            raise errors.InsufficientPermission

        try:
            entGroupId = cu.execute("SELECT entGroupId FROM entitlementGroups "
                                    "WHERE entGroup = ?", entGroup).next()[0]
        except StopIteration:
            raise errors.UnknownEntitlementGroup

        cu.execute("DELETE FROM Entitlements WHERE entGroupId=?",
                   entGroupId)
        cu.execute("DELETE FROM EntitlementOwners WHERE entGroupId=?",
                   entGroupId)
        cu.execute("DELETE FROM EntitlementGroups WHERE entGroupId=?",
                   entGroupId)
        self.db.commit()

    def addEntitlement(self, authToken, entGroup, entitlement):
        cu = self.db.cursor()
        # validate the password

        authGroupIds = self.getAuthGroups(cu, authToken)
        self.log(2, "entGroup=%s entitlement=%s" % (entGroup, entitlement))

        if len(entitlement) > 64:
            raise errors.InvalidEntitlement

        entGroupId = self.__checkEntitlementOwner(cu, authGroupIds, entGroup)

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

    def deleteEntitlement(self, authToken, entGroup, entitlement):
        cu = self.db.cursor()
        # validate the password

        authGroupIds = self.getAuthGroups(cu, authToken)
        self.log(2, "entGroup=%s entitlement=%s" % (entGroup, entitlement))

        if len(entitlement) > 64:
            raise errors.InvalidEntitlement

        entGroupId = self.__checkEntitlementOwner(cu, authGroupIds, entGroup)

        # if the entitlement doesn't exist, return an error
        cu.execute("""
                SELECT COUNT(*) FROM Entitlements WHERE
                    entGroupId = ? AND entitlement = ?
                """, entGroupId, entitlement)
        count = cu.next()[0]
        if not count:
            raise errors.InvalidEntitlement

        cu.execute("DELETE FROM Entitlements WHERE entGroupId=? AND "
                   "entitlement=?", (entGroupId, entitlement))

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

    def getEntitlementPermGroup(self, authToken, entGroup):
        """
        Returns the user group which controls the permissions for a group.
        """
        if not self.check(authToken, admin = True):
            raise errors.InsufficientPermission

        cu = self.db.cursor()
        cu.execute("""SELECT userGroup FROM EntitlementGroups
                        JOIN UserGroups USING (userGroupId)
                        WHERE entGroup = ?""", entGroup)
        try:
            return cu.next()[0]
        except:
            return None

    def getEntitlementOwnerAcl(self, authToken, entGroup):
        """
        Returns the user group which owns the entitlement group
        """
        if not self.check(authToken, admin = True):
            raise errors.InsufficientPermission

        cu = self.db.cursor()
        cu.execute("""SELECT userGroup FROM EntitlementGroups
                        JOIN EntitlementOwners USING (entGroupId)
                        JOIN UserGroups ON
                            UserGroups.userGroupId =
                                EntitlementOwners.ownerGroupId
                        WHERE entGroup = ?""", entGroup)
        try:
            return cu.next()[0]
        except:
            return None

    def _getIds(self, cu, entGroup, userGroup):
        cu.execute("SELECT entGroupId FROM entitlementGroups "
                   "WHERE entGroup = ?", entGroup)
        try:
            entGroupId = cu.next()[0]
        except StopIteration:
            raise errors.UnknownEntitlementGroup
        cu.execute("SELECT userGroupId FROM userGroups "
                   "WHERE userGroup = ?", userGroup)
        try:
            userGroupId = cu.next()[0]
        except StopIteration:
            raise errors.GroupNotFound
        return entGroupId, userGroupId

    def addEntitlementOwnerAcl(self, authToken, userGroup, entGroup):
        """
        Gives the userGroup ownership permission for the entGroup entitlement
        set.
        """
        if not self.check(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, "userGroup=%s entGroup=%s" % (userGroup, entGroup))
        cu = self.db.cursor()
        entGroupId, userGroupId = self._getIds(cu, entGroup, userGroup)
        cu.execute("INSERT INTO EntitlementOwners (entGroupId, ownerGroupId) "
                   "VALUES (?, ?)",
                   (entGroupId, userGroupId))
        self.db.commit()

    def deleteEntitlementOwnerAcl(self, authToken, userGroup, entGroup):
        if not self.check(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, "userGroup=%s entGroup=%s" % (userGroup, entGroup))
        cu = self.db.cursor()
        entGroupId, userGroupId = self._getIds(cu, entGroup, userGroup)
        cu.execute("DELETE FROM EntitlementOwners WHERE "
                   "entGroupId=? AND ownerGroupId=?",
                   entGroupId, userGroupId)
        self.db.commit()

    def iterEntitlements(self, authToken, entGroup):
        # validate the password
        cu = self.db.cursor()

        authGroupIds = self.getAuthGroups(cu, authToken)
        entGroupId = self.__checkEntitlementOwner(cu, authGroupIds, entGroup)
        cu.execute("SELECT entitlement FROM Entitlements WHERE "
                   "entGroupId = ?", entGroupId)

        return [ x[0] for x in cu ]

    def listEntitlementGroups(self, authToken):
        cu = self.db.cursor()

        if self.check(authToken, admin = True):
            # admins can see everything
            cu.execute("SELECT entGroup FROM EntitlementGroups")
        else:
            authGroupIds = self.getAuthGroups(cu, authToken)
            if not authGroupIds:
                return []

            cu.execute("""SELECT entGroup FROM EntitlementOwners
                            JOIN EntitlementGroups USING (entGroupId)
                            WHERE ownerGroupId IN (%s)""" % 
                       ",".join([ "%d" % x for x in authGroupIds ]))

        return [ x[0] for x in cu ]

class PasswordCheckParser(dict):

    def StartElementHandler(self, name, attrs):
        if name not in [ 'auth' ]:
            raise SyntaxError

        val = attrs.get('valid', None)

        self.valid = (val == '1' or str(val).lower() == 'true')

    def EndElementHandler(self, name):
        pass

    def CharacterDataHandler(self, data):
        if data:
            self.valid = False

    def parse(self, s):
        return self.p.Parse(s)

    def validPassword(self):
        return self.valid

    def __init__(self):
        self.p = xml.parsers.expat.ParserCreate()
        self.p.StartElementHandler = self.StartElementHandler
        self.p.EndElementHandler = self.EndElementHandler
        self.p.CharacterDataHandler = self.CharacterDataHandler
        self.valid = False
        dict.__init__(self)

