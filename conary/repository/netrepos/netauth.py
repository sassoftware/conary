#
# Copyright (c) 2004-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import itertools
import md5
import os
import time
import urllib, urllib2
import xml

from conary import conarycfg
from conary.repository import errors
from conary.lib import sha1helper, tracelog
from conary.dbstore import sqlerrors
from conary.repository.netrepos import items, versionops, accessmap

# FIXME: remove these compatibilty error classes later
UserAlreadyExists = errors.UserAlreadyExists
GroupAlreadyExists = errors.GroupAlreadyExists

MAX_ENTITLEMENT_LENGTH = 255

nameCharacterSet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-'

class UserAuthorization:
    def __init__(self, db, pwCheckUrl = None, cacheTimeout = None):
        self.db = db
        self.pwCheckUrl = pwCheckUrl
        self.cacheTimeout = cacheTimeout
        self.pwCache = {}


    def addUserByMD5(self, cu, user, salt, password):
        for letter in user:
            if letter not in nameCharacterSet:
                raise errors.InvalidName(user)
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
        cu.execute("SELECT userId FROM Users WHERE LOWER(userName)=LOWER(?)",
                   user)
        if len(cu.fetchall()) > 1:
            raise errors.UserAlreadyExists, 'user: %s' % user

        return uid

    def changePassword(self, cu, user, salt, password):
        if self.pwCheckUrl:
            raise errors.CannotChangePassword

        cu.execute("UPDATE Users SET password=?, salt=? WHERE userName=?",
                   cu.binary(password), cu.binary(salt), user)

    def _checkPassword(self, user, salt, password, challenge, remoteIp = None):
        if self.cacheTimeout:
            cacheEntry = sha1helper.sha1String("%s%s" % (user, challenge))
            timeout = self.pwCache.get(cacheEntry, None)
            if timeout is not None and time.time() < timeout:
                return True

        if self.pwCheckUrl:
            try:
                url = "%s?user=%s;password=%s" \
                        % (self.pwCheckUrl, urllib.quote(user),
                           urllib.quote(challenge))

                if remoteIp is not None:
                    url += ';remote_ip=%s' % urllib.quote(remoteIp)

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

    def getAuthorizedRoles(self, cu, user, password, allowAnonymous = True,
                           remoteIp = None):
        """
        Given a user and password, return the list of roles that are
        authorized via these credentials
        """

        cu.execute("""
        SELECT Users.salt, Users.password, UserGroupMembers.userGroupId,
               Users.userName, UserGroups.canMirror
        FROM Users
        JOIN UserGroupMembers USING(userId)
        JOIN UserGroups USING(userGroupId)
        WHERE Users.userName = ? OR Users.userName = 'anonymous'
        """, user)
        result = [ x for x in cu ]
        if not result:
            return set()

        # each user can only appear once (by constraint), so we only
        # need to validate the password once. we don't validate the
        # password for 'anonymous'. Using a bad password still allows
        # anonymous access
        userPasswords = [ x for x in result if x[3] != 'anonymous' ]
        # mirror users do not have an anonymous fallback
        if userPasswords and userPasswords[0][4] == 1:
            allowAnonymous = False
        if not allowAnonymous:
            result = userPasswords
        if userPasswords and not self._checkPassword(
                                        user,
                                        cu.frombinary(userPasswords[0][0]),
                                        userPasswords[0][1],
                                        password, remoteIp):
            result = [ x for x in result if x[3] == 'anonymous' ]

        return set(x[2] for x in result)

    def getRolesByUser(self, user):
        cu = self.db.cursor()
        cu.execute("""SELECT userGroup FROM Users
                        JOIN UserGroupMembers USING (userId)
                        JOIN UserGroups USING (userGroupId)
                        WHERE Users.userName = ?""", user)
        return [ x[0] for x in cu ]

    def getUserIdByName(self, userName):
        cu = self.db.cursor()

        cu.execute("SELECT userId FROM Users WHERE userName=?", userName)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
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

    def getAuthorizedRoles(self, cu, serverName, remoteIp,
                           entitlementClass, entitlement):
        """
        Given an entitlement, return the list of roles that the
        credentials authorize.
        """
        cacheEntry = sha1helper.sha1String("%s%s%s" % (
            serverName, entitlementClass, entitlement))
        roleIds, timeout, autoRetry = \
                self.cache.get(cacheEntry, (None, None, None))
        if (timeout is not None) and time.time() < timeout:
            return roleIds
        elif (timeout is not None):
            del self.cache[cacheEntry]
            if autoRetry is not True:
                raise errors.EntitlementTimeout([entitlement])

        if self.entCheckUrl:
            if entitlementClass is not None:
                url = "%s?server=%s;class=%s;key=%s" \
                        % (self.entCheckUrl, urllib.quote(serverName),
                           urllib.quote(entitlementClass),
                           urllib.quote(entitlement))
            else:
                url = "%s?server=%s;key=%s" \
                        % (self.entCheckUrl, urllib.quote(serverName),
                           urllib.quote(entitlement))

            if remoteIp is not None:
                url += ';remote_ip=%s' % urllib.quote(remoteIp)

            try:
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

            entitlementClass = p['class']
            entitlement = p['key']
            entitlementRetry = p['retry']
            if p['timeout'] is None:
                entitlementTimeout = self.cacheTimeout
            else:
                entitlementTimeout = p['timeout']

            if entitlementTimeout is None:
                entitlementTimeout = -1

        # look up entitlements
        cu.execute("""
        SELECT userGroupId FROM Entitlements
        JOIN EntitlementAccessMap USING (entGroupId)
        WHERE entitlement=?
        """, entitlement)

        roleIds = set(x[0] for x in cu)

        if self.entCheckUrl:
            # cacheEntry is still set from the cache check above
            self.cache[cacheEntry] = (roleIds,
                                      time.time() + entitlementTimeout,
                                      entitlementRetry)

        return roleIds

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
        self.items = items.Items(db)
        self.ri = accessmap.RoleInstances(db)

    def getAuthRoles(self, cu, authToken, allowAnonymous = True):
        self.log(4, authToken[0], authToken[2])
        # Find what role(s) this user belongs to
        # anonymous users should come through as anonymous, not None
        assert(authToken[0])

        # we need a hashable tuple, a list won't work
        authToken = tuple(authToken)

        if type(authToken[2]) is not list:
            # this code is for compatibility with old callers who
            # form up an old (user, pass, entclass, entkey) authToken.
            # rBuilder is one such caller.
            entList = []
            entClass = authToken[2]
            entKey = authToken[3]
            if entClass is not None and entKey is not None:
                entList.append((entClass, entKey))
            remoteIp = None
        elif len(authToken) == 3:
            entList = authToken[2]
            remoteIp = None
        else:
            entList = authToken[2]
            remoteIp = authToken[3]

        roleSet = self.userAuth.getAuthorizedRoles(
            cu, authToken[0], authToken[1],
            allowAnonymous = allowAnonymous,
            remoteIp = remoteIp)

        timedOut = []
        for entClass, entKey in entList:
            # XXX serverName is passed only for compatibility with the server
            # and entitlement class based entitlement design; it's only used
            # here during external authentication (used by some rPath
            # customers)
            try:
                rolesFromEntitlement = \
                    self.entitlementAuth.getAuthorizedRoles(
                        cu, self.serverNameList[0], remoteIp,
                        entClass, entKey)
                roleSet.update(rolesFromEntitlement)
            except errors.EntitlementTimeout, e:
                timedOut += e.getEntitlements()

        if timedOut:
            raise errors.EntitlementTimeout(timedOut)

        return roleSet

    # a faster way for batch checking access to a list of troves
    def batchCheck(self, authToken, troveTupList, write = False, remove = False):
        # troveTupList is a list of (name, VFS) tuples
        self.log(3, authToken[0], "entitlements=%s write=%s remove=%s" %(
            authToken[2], int(bool(write)), int(bool(remove))),
                 troveTupList)
        checkDict = {}
        # troveTupList can actually be an iterator, so we need to keep
        # a list of the trove names we're dealing with
        troveList = []
        # first check that we handle all the labels we're asked about
        for i, (n, v) in enumerate(troveTupList):
            label = v.branch().label()
            if label.getHost() not in self.serverNameList:
                raise errors.RepositoryMismatch(self.serverNameList, label.getHost())
            l = checkDict.setdefault(label.asString(), set())
            troveList.append(n)
            l.add(i)
        # default to all failing
        retlist = [ False ] * len(troveList)
        if not authToken[0]:
            return retlist
        # check groupIds. this is the same as the self.check() function
        cu = self.db.cursor()
        try:
            groupIds = self.getAuthRoles(cu, authToken)
        except errors.InsufficientPermission:
            return retlist
        if not len(groupIds):
            return retlist
        # build the query statement for permissions check
        stmt = """
        select Items.item
        from Permissions join Items using (itemId)
        """
        where = []
        where.append("Permissions.userGroupId IN (%s)" %
                     ",".join("%d" % x for x in groupIds))
        if write:
            where.append("Permissions.canWrite=1")
        if remove:
            where.append("Permissions.canRemove=1")
        if len(checkDict):
            where.append("""(
            Permissions.labelId = 0 OR
            Permissions.labelId in (select labelId from Labels where label=?)
            )""")
        stmt += "WHERE " + " AND ".join(where)
        self.log(4, stmt)
        # we need to test for each label separately in case we have
        # mutiple troves living of multiple lables with different
        # permission settings
        for label in checkDict.iterkeys():
            cu.execute(stmt, label)
            patterns = [ x[0] for x in cu ]
            for i in checkDict[label]:
                for pattern in patterns:
                    if self.checkTrove(pattern, troveList[i]):
                        retlist[i] = True
                        break
        return retlist

    # checks for group-wide permissions like admin and mirror
    def authCheck(self, authToken, admin=False, mirror=False):
        self.log(3, authToken[0],
                 "entitlements=%s admin=%s mirror=%s" %(
            authToken[2], int(bool(admin)), int(bool(mirror)) ))
        if not authToken[0]:
            return False
        cu = self.db.cursor()
        try:
            groupIds = self.getAuthRoles(cu, authToken, allowAnonymous=False)
        except errors.InsufficientPermission:
            return False
        if len(groupIds) < 1:
            return False
        cu.execute("select canMirror, admin from UserGroups "
                   "where userGroupId in (%s)" %(
            ",".join("%d" % x for x in groupIds)))
        hasAdmin = False
        hasMirror = False
        for mirrorBit, adminBit in cu.fetchall():
            if admin and adminBit:
                hasAdmin = True
            if mirror and (mirrorBit or adminBit):
                hasMirror = True
        admin = (not admin) or (admin and hasAdmin)
        mirror = (not mirror) or (mirror and hasMirror)
        return admin and mirror

    def checkPassword(self, authToken):
        cu = self.db.cursor()
        user = authToken[0]
        password = authToken[1]
        cu.execute('SELECT salt, password FROM Users WHERE userName=?', user)
        rows = cu.fetchall()
        if not len(rows):
            return False
        salt, challenge = rows[0]
        return self.userAuth._checkPassword(user, salt, challenge, password)

    # a simple call to auth.check(authToken) checks that the role
    # has an entry into the Permissions table - questionable
    # usefullness since we can't check that permission against the
    # label or the troves
    def check(self, authToken, write = False, label = None,
              trove = None, remove = False, allowAnonymous = True):
        self.log(3, authToken[0],
                 "entitlements=%s write=%s label=%s trove=%s remove=%s" %(
            authToken[2], int(bool(write)), label, trove, int(bool(remove))))

        if label and label.getHost() not in self.serverNameList:
            raise errors.RepositoryMismatch(self.serverNameList, label.getHost())

        if not authToken[0]:
            return False

        cu = self.db.cursor()

        try:
            groupIds = self.getAuthRoles(cu, authToken,
                                         allowAnonymous = allowAnonymous)
        except errors.InsufficientPermission:
            return False

        if len(groupIds) < 1:
            return False
        elif not label and not trove and not remove and not write:
            # no more checks to do -- the authentication information is valid
            return True

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

        if remove:
            where.append("Permissions.canRemove=1")

        if where:
            stmt += "WHERE " + " AND ".join(where)

        self.log(4, stmt, params)
        cu.execute(stmt, params)

        for (pattern,) in cu:
            if self.checkTrove(pattern, trove):
                return True

        return False

    def checkTrove(self, pattern, trove):
        return items.checkTrove(pattern, trove)

    def addAcl(self, role, trovePattern, label, write = False,
               remove = False):
        self.log(3, role, trovePattern, label, write, remove)
        cu = self.db.cursor()

        # these need to show up as 0/1 regardless of what we pass in
        write = int(bool(write))
        remove = int(bool(remove))

        if trovePattern:
            itemId = self.items.addPattern(trovePattern)
        else:
            itemId = 0
        # XXX This functionality is available in the TroveStore class
        #     refactor so that the code is not in two places
        if label:
            cu.execute("SELECT * FROM Labels WHERE label=?", label)
            labelId = cu.fetchone()
            if labelId:
                labelId = labelId[0]
            else:
                cu.execute("INSERT INTO Labels (label) VALUES(?)", label)
                labelId = cu.lastrowid
        else:
            labelId = 0

        roleId = self._getRoleIdByName(role)

        try:
            cu.execute("""
            INSERT INTO Permissions
            (userGroupId, labelId, itemId, canWrite, canRemove)
            VALUES (?, ?, ?, ?, ?)""", (
                roleId, labelId, itemId, write, remove))
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.PermissionAlreadyExists, "labelId: '%s', itemId: '%s'" % (labelId, itemId)
        self.ri.addPermissionId(cu.lastrowid, roleId)
        self.db.commit()

    def editAcl(self, role, oldTroveId, oldLabelId, troveId, labelId,
                write = False, canRemove = False):

        self.log(3, role, (oldTroveId, oldLabelId), (troveId, labelId),
                 write, canRemove)
        cu = self.db.cursor()

        roleId = self._getRoleIdByName(role)

        # these need to show up as 0/1 regardless of what we pass in
        write = int(bool(write))
        canRemove = int(bool(canRemove))

        try:
            cu.execute("""
            UPDATE Permissions
            SET labelId = ?, itemId = ?, canWrite = ?,
                canRemove = ?
            WHERE userGroupId=? AND labelId=? AND itemId=?""",
                       labelId, troveId, write, canRemove,
                       roleId, oldLabelId, oldTroveId)
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.PermissionAlreadyExists, "labelId: '%s', itemId: '%s'" % (labelId, troveId)

        # find out what permission we have changed and update cached
        # permission tables
        cu.execute("""
        select permissionId from Permissions
        where userGroupId = ? and labelId = ? and itemId = ?""",
                   (roleId, labelId, troveId))
        permissionId = cu.fetchone()
        if permissionId :
            permissionId = permissionId[0]
            if (oldLabelId != labelId or oldTroveId != troveId):
                # a permission has changed the itemId or the labelId...
                self.ri.updatePermissionId(permissionId, roleId)
            else: # just set the new canWrite flag
                self.ri.updateCanWrite(permissionId, roleId)
        self.db.commit()

    def deleteAcl(self, role, label, item):
        self.log(3, role, label, item)

        # check the validity of the role
        roleId = self._getRoleIdByName(role)

        if item is None: item = 'ALL'
        if label is None: label = 'ALL'

        cu = self.db.cursor()
        # lock the Permissions records we are about to delete. This is
        # a crude hack for sqlite's lack of "select for update"
        cu.execute("""
        update Permissions set canWrite=0, canRemove=0
        where userGroupId = ?
          and labelId = (select labelId from Labels where label=?)
          and itemId = (select itemId from Items where item=?)
        """, (roleId, label, item))
        cu.execute("""
        select permissionId from Permissions
        where userGroupId = ?
          and labelId = (select labelId from Labels where label=?)
          and itemId = (select itemId from Items where item=?)
        """, (roleId, label, item))
        for permissionId, in cu.fetchall():
            self.ri.deletePermissionId(permissionId, roleId)
            cu.execute("delete from Permissions where permissionId = ?",
                       permissionId)
        self.db.commit()

    def addUser(self, user, password):
        self.log(3, user)

        salt = os.urandom(4)
        m = md5.new()
        m.update(salt)
        m.update(password)

        self.addUserByMD5(user, salt, m.hexdigest())

    def roleIsAdmin(self, role):
        cu = self.db.cursor()
        cu.execute("SELECT admin FROM UserGroups WHERE userGroup=?",
                   role)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
        raise errors.RoleNotFound

    def roleCanMirror(self, role):
        cu = self.db.cursor()
        cu.execute("SELECT canMirror FROM UserGroups WHERE userGroup=?",
                   role)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
        raise errors.RoleNotFound

    def setAdmin(self, role, admin):
        self.log(3, role, admin)
        cu = self.db.transaction()
        cu.execute("UPDATE userGroups SET admin=? WHERE userGroup=?",
                   (int(bool(admin)), role))
        self.db.commit()

    def setMirror(self, role, canMirror):
        self.log(3, role, canMirror)
        cu = self.db.transaction()
        cu.execute("UPDATE userGroups SET canMirror=? WHERE userGroup=?",
                   (int(bool(canMirror)), role))
        self.db.commit()

    def _checkValidName(self, name):
        for letter in name:
            if letter not in nameCharacterSet:
                raise errors.InvalidName(name)
        
    def addUserByMD5(self, user, salt, password):
        self.log(3, user)
        self._checkValidName(user)
        cu = self.db.transaction()
        uid = self.userAuth.addUserByMD5(cu, user, salt, password)
        self.db.commit()

    def deleteUserByName(self, user):
        self.log(3, user)

        cu = self.db.cursor()

        # delete the role created with the name of that user
        try:
            self.deleteRole(user, False)
        except errors.RoleNotFound, e:
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

    def getRoles(self, user):
        cu = self.db.cursor()
        cu.execute("""SELECT UserGroups.userGroup
                      FROM UserGroups, Users, UserGroupMembers
                      WHERE UserGroups.userGroupId = UserGroupMembers.userGroupId AND
                            UserGroupMembers.userId = Users.userId AND
                            Users.userName = ?""", user)
        return [row[0] for row in cu]

    def getRoleList(self):
        cu = self.db.cursor()
        cu.execute("SELECT userGroup FROM UserGroups")
        return [ x[0] for x in cu ]

    def getRoleMembers(self, role):
        cu = self.db.cursor()
        cu.execute("""SELECT Users.userName FROM UserGroups
                            JOIN UserGroupMembers USING (userGroupId)
                            JOIN Users USING (userId)
                            WHERE userGroup = ? """, role)
        return [ x[0] for x in cu ]

    def _queryPermsByRole(self, role):
        cu = self.db.cursor()
        cu.execute("""SELECT Labels.label,
                             PerItems.item,
                             canWrite, canRemove
                      FROM UserGroups
                      JOIN Permissions USING (userGroupId)
                      LEFT OUTER JOIN Items AS PerItems ON
                          PerItems.itemId = Permissions.itemId
                      LEFT OUTER JOIN Labels ON
                          Permissions.labelId = Labels.labelId
                      WHERE userGroup=?""", role)
        return cu

    def iterPermsByRole(self, role):
        cu = self._queryPermsByRole(role)

        for row in cu:
            yield row

    def getPermsByRole(self, roleName):
        cu = self._queryPermsByRole(roleName)
        results = cu.fetchall_dict()
        # reconstruct the dictionary of values (because some
        # database engines like PostgreSQL lowercase all column names)
        l = []
        for result in results:
            d = {}
            for key in ('label', 'item', 'canWrite', 'canRemove'):
                d[key] = result[key]
            l.append(d)
        return l

    def _getRoleIdByName(self, role):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
                   role)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
        raise errors.RoleNotFound

    def _checkDuplicates(self, cu, role):
        # check for case insensitive user conflicts -- avoids race with
        # other adders on case-differentiated names
        cu.execute("SELECT userGroupId FROM UserGroups "
                   "WHERE LOWER(UserGroup)=LOWER(?)", role)
        if len(cu.fetchall()) > 1:
            # undo our insert
            self.db.rollback()
            raise errors.RoleAlreadyExists('role: %s' % role)

    def addRole(self, role):
        self.log(3, role)
        self._checkValidName(role)
        cu = self.db.transaction()
        try:
            cu.execute("INSERT INTO UserGroups (userGroup) VALUES (?)", role)
            ugid = cu.lastrowid
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.RoleAlreadyExists, "role: %s" % role
        self._checkDuplicates(cu, role)
        self.db.commit()
        return ugid

    def renameRole(self, oldRole, newRole):
        cu = self.db.cursor()
        if oldRole == newRole:
            return True
        try:
            cu.execute("UPDATE UserGroups SET userGroup=? WHERE userGroup=?",
                       (newRole, oldRole))
        except sqlerrors.ColumnNotUnique:
            self.db.rollback()
            raise errors.RoleAlreadyExists("role: %s" % newRole)
        self._checkDuplicates(cu, newRole)
        self.db.commit()
        return True

    def updateRoleMembers(self, role, members):
        #Do this in a transaction
        cu = self.db.cursor()
        roleId = self._getRoleIdByName(role)

        #First drop all the current members
        cu.execute ("DELETE FROM UserGroupMembers WHERE userGroupId=?", roleId)
        #now add the new members
        for userName in members:
            self.addRoleMember(role, userName, commit=False)
        self.db.commit()

    def addRoleMember(self, role, userName, commit = True):
        cu = self.db.cursor()
        # we do this in multiple select to let us generate the proper 
        # exceptions when the names don't xist
        roleId = self._getRoleIdByName(role)
        userId = self.userAuth.getUserIdByName(userName)

        cu.execute("""INSERT INTO UserGroupMembers (userGroupId, userId)
                        VALUES (?, ?)""", roleId, userId)

        if commit:
            self.db.commit()

    def deleteRole(self, role, commit = True):
        self.deleteRoleById(self._getRoleIdByName(role), commit)

    def deleteRoleById(self, roleId, commit = True):
        cu = self.db.cursor()
        cu.execute("DELETE FROM EntitlementAccessMap WHERE userGroupId=?",
                   roleId)
        cu.execute("DELETE FROM Permissions WHERE userGroupId=?", roleId)
        cu.execute("DELETE FROM UserGroupMembers WHERE userGroupId=?", roleId)
        cu.execute("DELETE FROM UserGroupInstancesCache WHERE userGroupId = ?",
                   roleId)
        cu.execute("DELETE FROM UserGroupTroves WHERE userGroupId = ?", roleId)
        cu.execute("DELETE FROM LatestCache WHERE userGroupId = ?", roleId)
        #Note, there could be a user left behind with no associated group
        #if the group being deleted was created with a user.  This user is not
        #deleted because it is possible for this user to be a member of
        #another group.
        cu.execute("DELETE FROM UserGroups WHERE userGroupId=?", roleId)
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

    def __checkEntitlementOwner(self, cu, roleIds, entClass):
        """
        Raises an error or returns the group Id.
        """
        if not roleIds:
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
        """ % ",".join(str(x) for x in roleIds), entClass)

        entClassIdList = [ x[0] for x in cu ]
        if entClassIdList:
            assert(max(entClassIdList) == min(entClassIdList))
            return entClassIdList[0]

        # admins can do everything
        cu.execute("select userGroupId from UserGroups "
                   "where userGroupId in (%s) "
                   "and admin = 1" % ",".join(str(x) for x in roleIds))
        if not len(cu.fetchall()):
            raise errors.InsufficientPermission

        cu.execute("SELECT entGroupId FROM EntitlementGroups WHERE "
                   "entGroup = ?", entClass)
        entClassIds = [ x[0] for x in cu ]

        if len(entClassIds) == 1:
            entClassId = entClassIds[0]
        else:
            assert(not entClassIds)
            entClassId = -1

        return entClassId

    def deleteEntitlementClass(self, authToken, entClass):
        cu = self.db.cursor()
        if not self.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission

        cu.execute("SELECT entGroupId FROM entitlementGroups "
                   "WHERE entGroup = ?", entClass)
        ret = cu.fetchall()
        # XXX: should we raise an error here or just go about it silently?
        if not len(ret):
            raise errors.UnknownEntitlementClass
        entClassId = ret[0][0]
        cu.execute("DELETE FROM EntitlementAccessMap WHERE entGroupId=?",
                   entClassId)
        cu.execute("DELETE FROM Entitlements WHERE entGroupId=?",
                   entClassId)
        cu.execute("DELETE FROM EntitlementOwners WHERE entGroupId=?",
                   entClassId)
        cu.execute("DELETE FROM EntitlementGroups WHERE entGroupId=?",
                   entClassId)
        self.db.commit()

    def addEntitlementKey(self, authToken, entClass, entKey):
        cu = self.db.cursor()
        # validate the password

        roleIds = self.getAuthRoles(cu, authToken)
        self.log(2, "entClass=%s entKey=%s" % (entClass, entKey))

        if len(entKey) > MAX_ENTITLEMENT_LENGTH:
            raise errors.InvalidEntitlement

        entClassId = self.__checkEntitlementOwner(cu, roleIds, entClass)

        if entClassId == -1:
            raise errors.UnknownEntitlementClass

        # check for duplicates
        cu.execute("SELECT * FROM Entitlements WHERE entGroupId = ? AND entitlement = ?",
                   (entClassId, entKey))
        if len(cu.fetchall()):
            raise errors.EntitlementKeyAlreadyExists

        cu.execute("INSERT INTO Entitlements (entGroupId, entitlement) VALUES (?, ?)",
                   (entClassId, entKey))

        self.db.commit()

    def deleteEntitlementKey(self, authToken, entClass, entKey):
        cu = self.db.cursor()
        # validate the password

        roleIds = self.getAuthRoles(cu, authToken)
        self.log(2, "entClass=%s entKey=%s" % (entClass, entKey))

        if len(entKey) > MAX_ENTITLEMENT_LENGTH:
            raise errors.InvalidEntitlement

        entClassId = self.__checkEntitlementOwner(cu, roleIds, entClass)

        # if the entitlement doesn't exist, return an error
        cu.execute("SELECT * FROM Entitlements WHERE entGroupId = ? AND entitlement = ?",
                   (entClassId, entKey))
        if not len(cu.fetchall()):
            raise errors.InvalidEntitlement

        cu.execute("DELETE FROM Entitlements WHERE entGroupId=? AND "
                   "entitlement=?", (entClassId, entKey))

        self.db.commit()

    def addEntitlementClass(self, authToken, entClass, role):
        """
        Adds a new entitlement class to the server, and populates it with
        an initial role
        """
        cu = self.db.cursor()
        if not self.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, "entClass=%s role=%s" % (entClass, role))

        # check for duplicate
        cu.execute("SELECT entGroupId FROM EntitlementGroups WHERE entGroup = ?",
                   entClass)
        if len(cu.fetchall()):
            raise errors.EntitlementClassAlreadyExists

        roleId = self._getRoleIdByName(role)

        cu.execute("INSERT INTO EntitlementGroups (entGroup) "
                   "VALUES (?)", entClass)
        entClassId = cu.lastrowid
        cu.execute("INSERT INTO EntitlementAccessMap (entGroupId, userGroupId) "
                   "VALUES (?, ?)", entClassId, roleId)
        self.db.commit()

    def getEntitlementClassOwner(self, authToken, entClass):
        """
        Returns the role which owns the entitlement class
        """
        if not self.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission

        cu = self.db.cursor()
        cu.execute("""
        SELECT userGroup FROM EntitlementGroups
        JOIN EntitlementOwners USING (entGroupId)
        JOIN UserGroups ON UserGroups.userGroupId = EntitlementOwners.ownerGroupId
        WHERE entGroup = ?""", entClass)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
        return None

    def _getIds(self, cu, entClass, role):
        cu.execute("SELECT entGroupId FROM entitlementGroups "
                   "WHERE entGroup = ?", entClass)
        ent = cu.fetchall()
        if not len(ent):
            raise errors.UnknownEntitlementClass

        cu.execute("SELECT userGroupId FROM userGroups "
                   "WHERE userGroup = ?", role)
        user = cu.fetchall()
        if not len(user):
            raise errors.RoleNotFound
        return ent[0][0], user[0][0]

    def addEntitlementClassOwner(self, authToken, role, entClass):
        """
        Gives the role management permission for the entitlement class.
        """
        if not self.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, "role=%s entClass=%s" % (role, entClass))
        cu = self.db.cursor()
        entClassId, roleId = self._getIds(cu, entClass, role)
        cu.execute("INSERT INTO EntitlementOwners (entGroupId, ownerGroupId) "
                   "VALUES (?, ?)",
                   (entClassId, roleId))
        self.db.commit()

    def deleteEntitlementClassOwner(self, authToken, role, entClass):
        if not self.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, "role=%s entClass=%s" % (role, entClass))
        cu = self.db.cursor()
        entClassId, roleId = self._getIds(cu, entClass, role)
        cu.execute("DELETE FROM EntitlementOwners WHERE "
                   "entGroupId=? AND ownerGroupId=?",
                   entClassId, roleId)
        self.db.commit()

    def iterEntitlementKeys(self, authToken, entClass):
        # validate the password
        cu = self.db.cursor()

        roleIds = self.getAuthRoles(cu, authToken)
        entClassId = self.__checkEntitlementOwner(cu, roleIds, entClass)
        cu.execute("SELECT entitlement FROM Entitlements WHERE "
                   "entGroupId = ?", entClassId)

        return [ x[0] for x in cu ]

    def listEntitlementClasses(self, authToken):
        cu = self.db.cursor()

        if self.authCheck(authToken, admin = True):
            # admins can see everything
            cu.execute("SELECT entGroup FROM EntitlementGroups")
        else:
            roleIds = self.getAuthRoles(cu, authToken)
            if not roleIds:
                return []

            # XXX gafton said he'd clean this up
            cu.execute("""SELECT entGroup FROM EntitlementOwners
                            JOIN EntitlementGroups USING (entGroupId)
                            WHERE ownerGroupId IN (%s)""" % 
                       ",".join([ "%d" % x for x in roleIds ]))

        return [ x[0] for x in cu ]

    def getEntitlementClassesRoles(self, authToken, classList):
        if not self.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission

        cu = self.db.cursor()

        # XXX gafton said he'd clean this up
        cu.execute("""SELECT entGroup, userGroup FROM EntitlementGroups
                        LEFT OUTER JOIN EntitlementAccessMap USING (entGroupId)
                        LEFT OUTER JOIN UserGroups USING (userGroupId)
                        WHERE entGroup IN (%s)"""
                   % ",".join([ "'%s'" % x for x in classList]))
        d = {}
        for entClass, role in cu:
            l = d.setdefault(entClass, [])
            if role is not None:
                l.append(role)

        if len(d) != len(classList):
            raise errors.RoleNotFound

        return d

    def setEntitlementClassesRoles(self, authToken, classInfo):
        """
        @param classInfo: Dictionary indexed by entitlement class, each
        entry being a list of exactly the roles that entitlement group
        should have access to.
        @type classInfo: dict
        """
        if not self.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission

        cu = self.db.cursor()

        # this would be faster with temporary tables; I doubt it matters
        # XXX gafton said he'd clean this up
        cu.execute("""SELECT entGroup, entGroupId FROM EntitlementGroups
                      WHERE entGroup IN (%s)""" % 
                   ",".join([ "'%s'" % x for x in classInfo ]))
        entClassMap = dict(x for x in cu)
        if len(entClassMap) != len(classInfo):
            raise errors.RoleNotFound

        # XXX gafton said he'd clean this up
        rolesNeeded = set(itertools.chain(*classInfo.itervalues()))
        if rolesNeeded:
            cu.execute("""SELECT userGroup, userGroupId FROM UserGroups
                              WHERE userGroup IN (%s)""" % 
                       ",".join([ "'%s'" % x for x in rolesNeeded ]))
            roleMap = dict(x for x in cu)
        else:
            roleMap = {}
        if len(roleMap) != len(rolesNeeded):
            raise errors.RoleNotFound

        # XXX gafton said he'd clean this up
        cu.execute("""DELETE FROM EntitlementAccessMap
                      WHERE entGroupId IN (%s)""" %
                   ",".join([ "%d" % x for x in entClassMap.itervalues() ]))

        for entClass, roles in classInfo.iteritems():
            for role in roles:
                cu.execute("""INSERT INTO EntitlementAccessMap
                              (entGroupId, userGroupId) VALUES (?, ?)""",
                           entClassMap[entClass], roleMap[role])

        self.db.commit()


class PasswordCheckParser(dict):

    def StartElementHandler(self, name, attrs):
        if name not in [ 'auth' ]:
            raise SyntaxError

        val = attrs.get('valid', None)

        self.valid = (val == '1' or str(val).lower() == 'true')

    def EndElementHandler(self, name):
        pass

    def CharacterDataHandler(self, data):
        if data.strip():
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

