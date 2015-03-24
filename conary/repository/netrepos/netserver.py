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


import base64
import cPickle
import errno
import itertools
import os
import re
import sys
import tempfile
import time
import types

from conary import files, trove, versions, streams
from conary.conarycfg import CfgEntitlement, CfgProxy, CfgProxyMap, CfgRepoMap, CfgUserInfo, getProxyMap
from conary.deps import deps
from conary.lib import log, tracelog, sha1helper, util
from conary.lib.cfg import ConfigFile
from conary.lib.cfgtypes import (CfgInt, CfgString, CfgPath, CfgBool, CfgList,
        CfgLineList)
from conary.repository import changeset, errors, xmlshims
from conary.repository.netrepos import fsrepos, instances, trovestore
from conary.repository.netrepos import accessmap, deptable, fingerprints
from conary.lib.openpgpfile import KeyNotFound
from conary.repository.netrepos.netauth import NetworkAuthorization
from conary.repository.netclient import TROVE_QUERY_ALL, TROVE_QUERY_PRESENT, \
                                        TROVE_QUERY_NORMAL
from conary.repository.netrepos import reposlog
from conary.repository.netrepos.repo_cfg import GlobListType, CfgContentStore
from conary import dbstore
from conary.dbstore import idtable, sqlerrors
from conary.server import schema
from conary.local import schema as depSchema
from conary.errors import InvalidRegex

# a list of the protocol versions we understand. Make sure the first
# one in the list is the lowest protocol version we support and th
# last one is the current server protocol version. Remember that range stops
# at MAX - 1
SERVER_VERSIONS = range(36, 73 + 1)

# We need to provide transitions from VALUE to KEY, we cache them as we go

# Decorators for method access

def _methodAccess(f, accessType):
    f._accessType = accessType
    return f

def accessReadOnly(f, paramList = []):
    _methodAccess(f, 'readOnly')
    return f

def accessReadWrite(f, paramList = []):
    _methodAccess(f, 'readWrite')
    return f

def requireClientProtocol(protocol):

    # the check is written in callWrapper rather than in the decorator
    # itself to make sure the access* and requireClientProtocol decorators
    # play nicely together

    def dec(f):
        f._minimumClientProtocol = protocol
        return f

    return dec

def deprecatedPermissionCall(*args, **kw):
    def f(self, *args, **kw):
        raise errors.InvalidClientVersion(
            'Conary 2.0 is required to manipulate permissions in this '
            'repository.')
    return f

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

    # lets the following exceptions pass:
    #
    # 1. Internal server error (unknown exception)
    # 2. netserver.InsufficientPermission

    # version filtering happens first. that's important for these flags
    # to make sense. it means that:
    #
    # _GET_TROVE_VERY_LATEST/_GET_TROVE_ALLOWED_FLAVOR
    #      returns all allowed flavors for the latest version of the trove
    #      which has any allowed flavor
    # _GET_TROVE_VERY_LATEST/_GET_TROVE_ALL_FLAVORS
    #      returns all flavors available for the latest version of the
    #      trove which has an allowed flavor
    # _GET_TROVE_VERY_LATEST/_GET_TROVE_BEST_FLAVOR
    #      returns the best flavor for the latest version of the trove
    #      which has at least one allowed flavor
    _GET_TROVE_ALL_VERSIONS = 1
    _GET_TROVE_VERY_LATEST  = 2         # latest of any flavor

    _GET_TROVE_NO_FLAVOR        = 1     # no flavor info is returned
    _GET_TROVE_ALL_FLAVORS      = 2     # all flavors (no scoring)
    _GET_TROVE_BEST_FLAVOR      = 3     # the best flavor for flavorFilter
    _GET_TROVE_ALLOWED_FLAVOR   = 4     # all flavors which are legal

    def __init__(self, cfg, basicUrl, db = None):
        # this is a bit of a hack to determine if we're running
        # as a standalone server or not without having to touch
        # rMake code
        self.standalone = hasattr(cfg, 'port')
        self.map = cfg.repositoryMap
        self.tmpPath = cfg.tmpDir
        self.basicUrl = basicUrl
        if isinstance(cfg.serverName, str):
            self.serverNameList = [ cfg.serverName ]
        else:
            self.serverNameList = cfg.serverName
        self.commitAction = cfg.commitAction
        self.troveStore = None
        self.logFile = cfg.logFile
        self.callLog = None
        self.requireSigs = cfg.requireSigs
        self.deadlockRetry = cfg.deadlockRetry
        self.repDB = cfg.repositoryDB
        self.contentsDir = cfg.contentsDir
        self.authCacheTimeout = cfg.authCacheTimeout
        self.externalPasswordURL = cfg.externalPasswordURL
        self.entitlementCheckURL = cfg.entitlementCheckURL
        self.readOnlyRepository = cfg.readOnlyRepository
        self.serializeCommits = cfg.serializeCommits
        self.paranoidCommits = cfg.paranoidCommits
        self.geoIpFiles = cfg.geoIpFiles
        for key in ['capsuleServerUrl', 'excludeCapsuleContents',
                'injectCapsuleContentServers']:
            if cfg[key]:
                raise RuntimeError("Capsule injection is no longer "
                        "implemented (%s)" % (key,))

        self.__delDB = False
        self.log = tracelog.getLog(None)
        if cfg.traceLog:
            (l, f) = cfg.traceLog
            self.log = tracelog.getLog(filename=f, level=l, trace=l>2)

        if self.logFile:
            self.callLog = reposlog.RepositoryCallLogger(self.logFile,
                                                         self.serverNameList)

        if not db:
            self.open()
        else:
            self.db = db
            self.open(connect = False)

        self.log(1, "url=%s" % basicUrl, "name=%s" % self.serverNameList,
              self.repDB, self.contentsDir[1])

    def __del__(self):
        # this is ugly, but for now it is the only way to break the
        # circular dep created by self.repos back to us
        self.repos.troveStore = None
        self.auth = self.ugo = None
        try:
            if self.__delDB: self.db.close()
        except:
            pass
        self.troveStore = self.repos = self.deptable = self.db = None

    def open(self, connect = True):
        self.log(3, "connect=", connect)
        if connect:
            self.db = dbstore.connect(self.repDB[1], driver = self.repDB[0])
            self.__delDB = True
        schema.checkVersion(self.db)
        schema.setupTempTables(self.db)
        depSchema.setupTempDepTables(self.db)
        self.troveStore = trovestore.TroveStore(self.db, self.log)
        self.repos = fsrepos.FilesystemRepository(
            self.serverNameList, self.troveStore, self.contentsDir,
            self.map, requireSigs = self.requireSigs,
            paranoidCommits = self.paranoidCommits)
        self.auth = NetworkAuthorization(
            self.db, self.serverNameList, log = self.log,
            cacheTimeout = self.authCacheTimeout,
            passwordURL = self.externalPasswordURL,
            entCheckURL = self.entitlementCheckURL,
            geoIpFiles=self.geoIpFiles,
            )
        self.ri = accessmap.RoleInstances(self.db)
        self.deptable = deptable.DependencyTables(self.db)
        self.log.reset()

    def reopen(self):
        self.log.reset()
        self.log(3)
        if self.db.reopen():
            # help the garbage collector with the magic from __del__
            self.repos.troveStore = None
            self.troveStore = self.repos = self.auth = self.deptable = None
            self.open(connect=False)

    def reset(self):
        """
        Free temporary resources that do not need to persist between requests,
        e.g. pooled database connections.
        """
        if self.db.poolmode:
            self.db.close()

    def close(self):
        self.db.close()
        self.log.close()
        if self.callLog:
            self.callLog.close()

    # does the actual method calling and the retry when hitting deadlocks
    def _callWrapper(self, method, authToken, orderedArgs, kwArgs):
        methodname = method.im_func.__name__
        attempt = 1
        while True:
            try:
                return method(authToken, *orderedArgs, **kwArgs)
            except sqlerrors.DatabaseLocked, e:
                # deadlock occurred; we rollback and try again
                log.error("Deadlock id %d while calling %s: %s",
                          attempt, methodname, str(e.args))
                self.log(1, "Deadlock id %d while calling %s: %s" %(
                    attempt, methodname, str(e.args)))
                if attempt < self.deadlockRetry:
                    self.db.rollback()
                    attempt += 1
                    continue
                # max number of deadlocks reached, bail out
                raise
            # not reached
            assert(0)

    def callWrapper(self, protocol, port, methodname, authToken,
                    orderedArgs, kwArgs,
                    remoteIp = None, rawUrl = None, isSecure = False,
                    systemId = None):
        """
        Returns a tuple of (Exception, result).  Exception is a Boolean
        stating whether an error occurred.
        """
        self._port = port
        self._protocol = protocol
        self._baseUrlOverride = rawUrl

        if methodname not in self.publicCalls:
            raise errors.MethodNotSupported(methodname)

        method = self.__getattribute__(methodname)

        # Repository in read-only mode?
        if method._accessType == 'readWrite' and self.readOnlyRepository:
            raise errors.ReadOnlyRepositoryError("Repository is read only")

        if (hasattr(method, '_minimumClientProtocol') and
                        method._minimumClientProtocol > orderedArgs[0]):
            raise errors.InvalidClientVersion(
                    '%s call only supports protocol versions %s '
                    'and later' % (methodname, method._minimumClientProtocol))

        # reopens the database as needed (if changed on disk or lost connection)
        self.reopen()

        exceptionOverride = None
        start = time.time()
        try:
            r = self._callWrapper(method, authToken, orderedArgs, kwArgs)
            if self.db.inTransaction(default=True):
                # Commit if someone left a transaction open (or the
                # DB doesn't have a way to tell)
                self.db.commit()
        except Exception, e:
            # on exceptions we rollback the database
            if self.db.inTransaction(default=True):
                self.db.rollback()
        else:
            if self.callLog:
                self.callLog.log(remoteIp, authToken, methodname,
                                 orderedArgs, kwArgs,
                                 latency = time.time() - start,
                                 systemId = systemId)
            return r

        if self.callLog:
            if isinstance(e, HiddenException):
                self.callLog.log(remoteIp, authToken, methodname, orderedArgs,
                                 kwArgs, exception = e.forLog,
                                 latency = time.time() - start,
                                 systemId = systemId)
                exceptionOverride = e.forReturn
            else:
                self.callLog.log(remoteIp, authToken, methodname, orderedArgs,
                                 kwArgs, exception = e,
                                 latency = time.time() - start,
                                 systemId = systemId)
        elif isinstance(e, HiddenException):
            exceptionOverride = e.forReturn

        if isinstance(e, sqlerrors.DatabaseLocked):
            exceptionOverride = errors.RepositoryLocked()

        if exceptionOverride:
            raise exceptionOverride

        raise

    def urlBase(self, urlName = True):
        if urlName and self._baseUrlOverride:
            return self._baseUrlOverride

        return self.basicUrl % { 'port' : self._port,
                                 'protocol' : self._protocol }

    def getContentsStore(self):
        return self.repos.contentsStore

    @accessReadWrite
    def addUser(self, authToken, clientVersion, user, newPassword):
        # adds a new user, with no acls. for now it requires full admin
        # rights
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], user)
        self.auth.addUser(user, newPassword)
        return True

    @accessReadWrite
    def addUserByMD5(self, authToken, clientVersion, user, salt, newPassword):
        # adds a new user, with no acls. for now it requires full admin
        # rights
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], user)
        #Base64 decode salt
        self.auth.addUserByMD5(user, base64.decodestring(salt), newPassword)
        return True

    @accessReadWrite
    @deprecatedPermissionCall
    def addAccessGroup(self, authToken, clientVersion, groupName):
        pass

    @accessReadWrite
    def addRole(self, authToken, clientVersion, role):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role)
        return self.auth.addRole(role)


    @accessReadWrite
    @deprecatedPermissionCall
    def deleteAccessGroup(self, authToken, clientVersion, groupName):
        pass

    @accessReadWrite
    def deleteRole(self, authToken, clientVersion, role):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role)
        self.auth.deleteRole(role)
        return True

    @accessReadOnly
    @deprecatedPermissionCall
    def listAccessGroups(self, authToken, clientVersion):
        pass

    @accessReadOnly
    def listRoles(self, authToken, clientVersion):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0])
        return self.auth.getRoleList()

    @accessReadWrite
    @deprecatedPermissionCall
    def updateAccessGroupMembers(self, authToken, clientVersion, groupName, members):
        pass

    @accessReadWrite
    def addRoleMember(self, authToken, clientVersion, role, username):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role, username)
        self.auth.addRoleMember(role, username)
        return True

    @accessReadOnly
    def getRoleMembers(self, authToken, clientVersion, role):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role)
        return self.auth.getRoleMembers(role)

    @accessReadWrite
    def updateRoleMembers(self, authToken, clientVersion, role, members):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role, members)
        self.auth.updateRoleMembers(role, members)
        return True

    @accessReadWrite
    def deleteUserByName(self, authToken, clientVersion, user):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], user)
        self.auth.deleteUserByName(user)
        return True

    @accessReadWrite
    @deprecatedPermissionCall
    def setUserGroupCanMirror(self, authToken, clientVersion,
                              userGroup, canMirror):
        pass

    @accessReadWrite
    def setRoleCanMirror(self, authToken, clientVersion, role, canMirror):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role, canMirror)
        self.auth.setMirror(role, canMirror)
        return True

    @accessReadOnly
    def getRoleFilters(self, authToken, clientVersion, roles):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], roles)
        ret = self.auth.getRoleFilters(roles)
        for role, flags in ret.iteritems():
            ret[role] = [self.fromFlavor(x) for x in flags]
        return ret

    @accessReadWrite
    def setRoleFilters(self, authToken, clientVersion, roleFiltersMap):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], roleFiltersMap)
        for role, flags in roleFiltersMap.iteritems():
            roleFiltersMap[role] = [self.toFlavor(x) for x in flags]
        self.auth.setRoleFilters(roleFiltersMap)
        return True

    @accessReadWrite
    @deprecatedPermissionCall
    def setUserGroupIsAdmin(self, authToken, clientVersion, userGroup, admin):
        pass

    @accessReadWrite
    def setRoleIsAdmin(self, authToken, clientVersion, role, admin):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role, admin)
        self.auth.setAdmin(role, admin)
        return True

    @accessReadOnly
    def listAcls(self, authToken, clientVersion, role):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role)
        returner = list()
        for acl in self.auth.getPermsByRole(role):
            if acl['label'] is None:
                acl['label'] = ""
            if acl['item'] is None:
                acl['item'] = ""
            returner.append(acl)
        return returner

    @accessReadWrite
    @requireClientProtocol(60)
    def addAcl(self, authToken, clientVersion, role, trovePattern,
               label, write = False, remove = False):
        if not self.auth.authCheck(authToken, admin=True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role, trovePattern, label,
                 "write=%s remove=%s" % (write, remove))
        if trovePattern == "":
            trovePattern = None
        if trovePattern:
            try:
                re.compile(trovePattern)
            except:
                raise InvalidRegex(trovePattern)

        if label == "":
            label = None
        self.auth.addAcl(role, trovePattern, label, write, remove = remove)

        return True

    @accessReadWrite
    def deleteAcl(self, authToken, clientVersion, role, trovePattern,
                  label):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role, trovePattern, label)
        if trovePattern == "":
            trovePattern = None

        if label == "":
            label = None

        self.auth.deleteAcl(role, label, trovePattern)

        return True

    @accessReadWrite
    @requireClientProtocol(60)
    def editAcl(self, authToken, clientVersion, role, oldTrovePattern,
                oldLabel, trovePattern, label, write = False,
                canRemove = False):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], role,
                 "old=%s new=%s" % ((oldTrovePattern, oldLabel),
                                    (trovePattern, label)),
                 "write=%s" % (write,))
        if trovePattern == "":
            trovePattern = "ALL"
        if trovePattern:
            try:
                re.compile(trovePattern)
            except:
                raise InvalidRegex(trovePattern)

        if label == "":
            label = "ALL"

        #Get the Ids
        troveId = self.troveStore.getItemId(trovePattern)
        oldTroveId = self.troveStore.items.get(oldTrovePattern, None)

        labelId = idtable.IdTable.get(self.troveStore.versionOps.labels, label, None)
        oldLabelId = idtable.IdTable.get(self.troveStore.versionOps.labels, oldLabel, None)

        self.auth.editAcl(role, oldTroveId, oldLabelId, troveId, labelId,
                          write, canRemove = canRemove)

        return True

    @accessReadWrite
    def changePassword(self, authToken, clientVersion, user, newPassword):
        if (not self.auth.authCheck(authToken, admin = True)
            and not self.auth.check(authToken, allowAnonymous = False)):
            raise errors.InsufficientPermission

        self.log(2, authToken[0], user)
        self.auth.changePassword(user, newPassword)
        return True

    @accessReadOnly
    @deprecatedPermissionCall
    def getUserGroups(self, authToken, clientVersion):
        pass

    @accessReadOnly
    def getRoles(self, authToken, clientVersion):
        if not self.auth.check(authToken):
            raise errors.InsufficientPermission
        self.log(2)
        r = self.auth.getRoles(authToken[0])
        return r

    @deprecatedPermissionCall
    def addEntitlement(self, authToken, clientVersion, *args):
        pass

    @accessReadWrite
    def addEntitlementKeys(self, authToken, clientVersion, entClass,
                           entKeys):
        # self.auth does its own authentication check
        for key in entKeys:
            key = self.toEntitlement(key)
            self.auth.addEntitlementKey(authToken, entClass, key)

        return True

    @accessReadWrite
    def deleteEntitlement(self, authToken, clientVersion, *args):
        raise errors.InvalidClientVersion(
            'conary 1.1.x is required to manipulate entitlements in '
            'this repository server')

    @accessReadWrite
    def deleteEntitlementKeys(self, authToken, clientVersion, entClass,
                              entKeys):
        # self.auth does its own authentication check
        for key in entKeys:
            key = self.toEntitlement(key)
            self.auth.deleteEntitlementKey(authToken, entClass, key)

        return True

    @accessReadWrite
    @deprecatedPermissionCall
    def addEntitlementGroup(self, authToken, clientVersion, entGroup,
                            userGroup):
        pass

    @accessReadWrite
    def addEntitlementClass(self, authToken, clientVersion, entClass,
                            role):
        # self.auth does its own authentication check
        self.auth.addEntitlementClass(authToken, entClass, role)
        return True

    @accessReadWrite
    @deprecatedPermissionCall
    def deleteEntitlementGroup(self, authToken, clientVersion, entGroup):
        pass

    @accessReadWrite
    def deleteEntitlementClass(self, authToken, clientVersion, entClass):
        # self.auth does its own authentication check
        self.auth.deleteEntitlementClass(authToken, entClass)
        return True

    @accessReadWrite
    @deprecatedPermissionCall
    def addEntitlementOwnerAcl(self, authToken, clientVersion, userGroup,
                               entGroup):
        pass

    @accessReadWrite
    def addEntitlementClassOwner(self, authToken, clientVersion, role,
                                 entClass):
        # self.auth does its own authentication check
        self.auth.addEntitlementClassOwner(authToken, role, entClass)
        return True

    @accessReadWrite
    @deprecatedPermissionCall
    def deleteEntitlementOwnerAcl(self, authToken, clientVersion, userGroup,
                                  entGroup):
        pass

    @accessReadWrite
    def deleteEntitlementClassOwner(self, authToken, clientVersion, role,
                                    entClass):
        # self.auth does its own authentication check
        self.auth.deleteEntitlementClassOwner(authToken, role, entClass)
        return True

    @accessReadOnly
    def listEntitlementKeys(self, authToken, clientVersion, entClass):
        # self.auth does its own authentication check
        return [ self.fromEntitlement(x) for x in
                 self.auth.iterEntitlementKeys(authToken, entClass) ]

    @accessReadOnly
    @deprecatedPermissionCall
    def listEntitlementGroups(self, authToken, clientVersion):
        pass

    @accessReadOnly
    def listEntitlementClasses(self, authToken, clientVersion):
        # self.auth does its own authentication check and restricts the
        # list of entitlements being displayed to those the user has
        # permissions to manage
        return self.auth.listEntitlementClasses(authToken)

    @accessReadOnly
    @deprecatedPermissionCall
    def getEntitlementClassAccessGroup(self, authToken, clientVersion,
                                       classList):
        pass

    @accessReadOnly
    def getEntitlementClassesRoles(self, authToken, clientVersion,
                                   classList):
        # self.auth does its own authentication check and restricts the
        # list of entitlements being displayed to the admin user
        return self.auth.getEntitlementClassesRoles(authToken, classList)

    @accessReadWrite
    @deprecatedPermissionCall
    def setEntitlementClassAccessGroup(self, authToken, clientVersion,
                                       classInfo):
        pass

    @accessReadWrite
    def setEntitlementClassesRoles(self, authToken, clientVersion,
                                   classInfo):
        # self.auth does its own authentication check and restricts the
        # list of entitlements being displayed to the admin user
        self.auth.setEntitlementClassesRoles(authToken, classInfo)
        return ""

    @accessReadWrite
    def addTroveAccess(self, authToken, clientVersion, role, troveList):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.ri.addTroveAccess(role, troveList, recursive=True)
        return ""

    @accessReadWrite
    def deleteTroveAccess(self, authToken, clientVersion, role, troveList):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        self.ri.deleteTroveAccess(role, troveList)
        return ""

    @accessReadOnly
    def listTroveAccess(self, authToken, clientVersion, role):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        ret = self.ri.listTroveAccess(role)
        # strip off the recurse flag; return just the name,version,flavor tuple
        return [ x[0] for x in ret ]

    @accessReadWrite
    def updateMetadata(self, authToken, clientVersion,
                       troveName, branch, shortDesc, longDesc,
                       urls, categories, licenses, source, language):
        branch = self.toBranch(branch)
        if not self.auth.check(authToken, write = True,
                               label = branch.label(),
                               trove = troveName):
            raise errors.InsufficientPermission
        self.log(2, troveName, branch)
        retval = self.troveStore.updateMetadata(
            troveName, branch, shortDesc, longDesc,
            urls, categories, licenses, source, language)
        return retval

    @accessReadOnly
    def getMetadata(self, authToken, clientVersion, troveList, language):
        self.log(2, "language=%s" % language, troveList)
        metadata = {}
        # XXX optimize this to one SQL query downstream
        for troveName, branch, version in troveList:
            branch = self.toBranch(branch)
            if not self.auth.check(authToken, write = False,
                                   label = branch.label(),
                                   trove = troveName):
                raise errors.InsufficientPermission
            if version:
                version = self.toVersion(version)
            else:
                version = None
            md = self.troveStore.getMetadata(troveName, branch, version, language)
            if md:
                metadata[troveName] = md.freeze()
        return metadata

    def _setupFlavorFilter(self, cu, flavorSet):
        self.log(3, flavorSet)
        schema.resetTable(cu, 'tmpFlavorMap')
        for i, flavor in enumerate(flavorSet.iterkeys()):
            flavorId = i + 1
            flavorSet[flavor] = flavorId
            if flavor is '':
                # empty flavor yields a dummy dep on a null flag
                cu.execute("""INSERT INTO tmpFlavorMap
                (flavorId, base, sense, depClass, flag)
                VALUES(?, 'use', ?, ?, NULL)""",(
                    flavorId, deps.FLAG_SENSE_REQUIRED, deps.DEP_CLASS_USE),
                           start_transaction = False)
                continue
            for depClass in self.toFlavor(flavor).getDepClasses().itervalues():
                for dep in depClass.getDeps():
                    cu.execute("""INSERT INTO tmpFlavorMap
                    (flavorId, base, sense, depClass) VALUES (?, ?, ?, ?)""", (
                        flavorId, dep.name, deps.FLAG_SENSE_REQUIRED, depClass.tag),
                               start_transaction = False)
                    for (flag, sense) in dep.flags.iteritems():
                        cu.execute("""INSERT INTO tmpFlavorMap
                        (flavorId, base, sense, depClass, flag)
                        VALUES (?, ?, ?, ?, ?)""", (
                            flavorId, dep.name, sense, depClass.tag, flag),
                                   start_transaction = False)
        self.db.analyze("tmpFlavorMap")

    def _setupTroveFilter(self, cu, troveSpecs, flavorIndices):
        self.log(3, troveSpecs, flavorIndices)
        schema.resetTable(cu, 'tmpGTVL')
        for troveName, versionDict in troveSpecs.iteritems():
            if type(versionDict) is list:
                versionDict = dict.fromkeys(versionDict, [ None ])

            for versionSpec, flavorList in versionDict.iteritems():
                if flavorList is None:
                    cu.execute("INSERT INTO tmpGTVL VALUES (?, ?, NULL)",
                               cu.encode(troveName), cu.encode(versionSpec),
                               start_transaction = False)
                else:
                    for flavorSpec in flavorList:
                        flavorId = flavorIndices.get(flavorSpec, None)
                        cu.execute("INSERT INTO tmpGTVL VALUES (?, ?, ?)",
                                cu.encode(troveName), cu.encode(versionSpec),
                                flavorId, start_transaction=False)
        self.db.analyze("tmpGTVL")

    def _latestType(self, queryType):
        return queryType

    _GTL_VERSION_TYPE_NONE = 0
    _GTL_VERSION_TYPE_LABEL = 1
    _GTL_VERSION_TYPE_VERSION = 2
    _GTL_VERSION_TYPE_BRANCH = 3

    def _getTroveList(self, authToken, clientVersion, troveSpecs,
                      versionType = _GTL_VERSION_TYPE_NONE,
                      latestFilter = _GET_TROVE_ALL_VERSIONS,
                      flavorFilter = _GET_TROVE_ALL_FLAVORS,
                      withFlavors = False,
                      troveTypes = TROVE_QUERY_PRESENT):
        self.log(3, versionType, latestFilter, flavorFilter)
        cu = self.db.cursor()
        singleVersionSpec = None
        dropTroveTable = False

        assert(versionType == self._GTL_VERSION_TYPE_NONE or
               versionType == self._GTL_VERSION_TYPE_BRANCH or
               versionType == self._GTL_VERSION_TYPE_VERSION or
               versionType == self._GTL_VERSION_TYPE_LABEL)

        # permission check first
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return {}

        flavorIndices = {}
        if troveSpecs:
            # populate flavorIndices with all of the flavor lookups we
            # need; a flavor of 0 (numeric) means "None"
            for versionDict in troveSpecs.itervalues():
                for flavorList in versionDict.itervalues():
                    if flavorList is not None:
                        flavorIndices.update({}.fromkeys(flavorList))
            if flavorIndices.has_key(0):
                del flavorIndices[0]
        if flavorIndices:
            self._setupFlavorFilter(cu, flavorIndices)

        coreQdict = {}
        coreQdict["localFlavor"] = "0"
        if not troveSpecs or (len(troveSpecs) == 1 and
                                 troveSpecs.has_key(None) and
                                 len(troveSpecs[None]) == 1 and
                                 troveSpecs[None].has_key(None)):
            # None or { None:None} case
            coreQdict["trove"] = "Items"
            assert(versionType == self._GTL_VERSION_TYPE_NONE)
        elif len(troveSpecs) == 1 and None in troveSpecs:
            if len(troveSpecs[None]) == 1:
                # no trove names, and a single version spec (multiple ones
                # are disallowed)
                coreQdict["trove"] = "Items"
                singleVersionSpec = troveSpecs[None].keys()[0]
            else:
                self._setupTroveFilter(cu, troveSpecs, flavorIndices)
                coreQdict["trove"] = "Items CROSS JOIN tmpGTVL"
                coreQdict["localFlavor"] = "tmpGTVL.flavorId"
        else:
            dropTroveTable = True
            self._setupTroveFilter(cu, troveSpecs, flavorIndices)
            coreQdict["trove"] = "tmpGTVL JOIN Items USING (item)"
            coreQdict["localFlavor"] = "tmpGTVL.flavorId"

        # FIXME: the '%s' in the next lines are wreaking havoc through
        # cached execution plans
        argDict = {}
        if singleVersionSpec:
            spec = ":spec"
            argDict["spec"] = singleVersionSpec
        else:
            spec = "tmpGTVL.versionSpec"
        if versionType == self._GTL_VERSION_TYPE_LABEL:
            coreQdict["spec"] = """JOIN LabelMap ON
            LabelMap.itemId = Nodes.itemId AND
            LabelMap.branchId = Nodes.branchId
        JOIN Labels ON
            Labels.labelId = LabelMap.labelId
            AND Labels.label = %s""" % spec
        elif versionType == self._GTL_VERSION_TYPE_BRANCH:
            coreQdict["spec"] = """JOIN Branches ON
            Branches.branchId = Nodes.branchId
            AND Branches.branch = %s""" % spec
        elif versionType == self._GTL_VERSION_TYPE_VERSION:
            coreQdict["spec"] = """JOIN Versions ON
            Nodes.versionId = Versions.versionId
            AND Versions.version = %s""" % spec
        else:
            assert(versionType == self._GTL_VERSION_TYPE_NONE)
            coreQdict["spec"] = ""

        # because we need to filter through RoleInstancesCache
        # table, we need to go through the Instances and Nodes tables
        # all the time
        where = []
        where.append(
            "ugi.userGroupId IN (%s)" % (
            ", ".join("%d" % x for x in roleIds),))
        # "leaves" == Latest ; "all" == Instances
        coreQdict["latest"] = ""
        if latestFilter != self._GET_TROVE_ALL_VERSIONS:
            coreQdict["latest"] = """JOIN LatestCache ON
            LatestCache.itemId = Nodes.itemId AND
            LatestCache.versionId = Nodes.versionId AND
            LatestCache.branchId = Nodes.branchId AND
            LatestCache.flavorId = Instances.flavorId AND
            LatestCache.userGroupId = ugi.userGroupId AND
            LatestCache.latestType = :ltype"""
            argDict["ltype"] = self._latestType(troveTypes)
        elif troveTypes != TROVE_QUERY_ALL:
            if troveTypes == TROVE_QUERY_PRESENT:
                s = "!= :ttype"
                argDict["ttype"] = trove.TROVE_TYPE_REMOVED
            else:
                assert(troveTypes == TROVE_QUERY_NORMAL)
                s = "= :ttype"
                argDict["ttype"] = trove.TROVE_TYPE_NORMAL
            where.append("Instances.isPresent = %d " % (
                instances.INSTANCE_PRESENT_NORMAL,))
            where.append("Instances.troveType %s" % (s,))
        coreQdict["where"] = """
          AND """.join(where)

        coreQuery = """
        SELECT DISTINCT
            Nodes.nodeId as nodeId,
            Instances.flavorId as flavorId,
            %(localFlavor)s as localFlavorId
        FROM %(trove)s
        JOIN Instances ON
            Items.itemId = Instances.itemId
        JOIN UserGroupInstancesCache AS ugi ON
            Instances.instanceId = ugi.instanceId
        JOIN Nodes ON
            Instances.itemId = Nodes.itemId AND
            Instances.versionId = Nodes.versionId
        %(latest)s
        %(spec)s
        WHERE %(where)s
        """ % coreQdict

        # build the outer query around the coreQuery
        mainQdict = {}

        if flavorIndices:
            assert(withFlavors)
            extraJoin = localGroup = ""
            localFlavor = "0"
            if len(flavorIndices) > 1:
                # if there is only one flavor we don't need to join based on
                # the tmpGTVL.flavorId (which is good, since it may not exist)
                extraJoin = "tmpFlavorMap.flavorId = gtlTmp.localFlavorId AND"
            if dropTroveTable:
                localFlavor = "gtlTmp.localFlavorId"
                localGroup = ", " + localFlavor

            # take the core query and compute flavor scoring
            mainQdict["core"] = """
            SELECT
                gtlTmp.nodeId as nodeId,
                gtlTmp.flavorId as flavorId,
                %(flavor)s as localFlavorId,
                SUM(coalesce(FlavorScores.value, 0)) as flavorScore
            FROM ( %(core)s ) as gtlTmp
            LEFT OUTER JOIN FlavorMap ON
                FlavorMap.flavorId = gtlTmp.flavorId
            LEFT OUTER JOIN tmpFlavorMap ON
                %(extra)s tmpFlavorMap.base = FlavorMap.base
                AND tmpFlavorMap.depClass = FlavorMap.depClass
                AND ( tmpFlavorMap.flag = FlavorMap.flag OR
                      (tmpFlavorMap.flag is NULL AND FlavorMap.flag is NULL) )
            LEFT OUTER JOIN FlavorScores ON
                FlavorScores.present = FlavorMap.sense
                AND FlavorScores.request = coalesce(tmpFlavorMap.sense, 0)
            GROUP BY gtlTmp.nodeId, gtlTmp.flavorId %(group)s
            HAVING SUM(coalesce(FlavorScores.value, 0)) > -500000
            """ % { "core" : coreQuery,
                    "extra" : extraJoin,
                    "flavor" : localFlavor,
                    "group" : localGroup}
            mainQdict["score"] = "tmpQ.flavorScore"
        else:
            assert(flavorFilter == self._GET_TROVE_ALL_FLAVORS)
            mainQdict["core"] = coreQuery
            mainQdict["score"] = "NULL"

        mainQdict["select"] = """I.item as trove,
            tmpQ.localFlavorId as localFlavorId,
            V.version as version,
            N.timeStamps as timeStamps,
            N.branchId as branchId,
            N.finalTimestamp as finalTimestamp"""
        mainQdict["flavor"] = ""
        mainQdict["joinFlavor"] = ""
        if withFlavors:
            mainQdict["joinFlavor"] = "JOIN Flavors AS F ON F.flavorId = tmpQ.flavorId"
            mainQdict["flavor"] = "F.flavor"

        # this is the Query we execute. Executing the core query as a
        # subquery forces better execution plans and reduces the
        # overall number of rows traversed.
        fullQuery = """
        SELECT
            %(select)s,
            %(flavor)s as flavor,
            %(score)s as flavorScore
        FROM ( %(core)s ) AS tmpQ
        JOIN Nodes AS N on tmpQ.nodeId = N.nodeId
        JOIN Items AS I on N.itemId = I.itemId
        JOIN Versions AS V on N.versionId = V.versionId
        %(joinFlavor)s
        ORDER BY I.item, N.finalTimestamp
        """ % mainQdict

        self.log(4, "execute query", fullQuery, argDict)
        cu.execute(fullQuery, argDict)
        self.log(4, "executed query")

        # this prevents dups that could otherwise arise from multiple
        # acl's allowing access to the same information
        allowed = set()

        troveVersions = {}

        # FIXME: Remove the ORDER BY in the sql statement above and watch it
        # CRASH and BURN. Put a "DESC" in there to return some really wrong data
        #
        # That is because the loop below is dependent on the order in
        # which this data is provided, even though it is the same
        # dataset with and without "ORDER BY" -- gafton
        for (troveName, localFlavorId, versionStr, timeStamps,
             branchId, finalTimestamp, flavor, flavorScore) in cu:
            if flavorScore is None:
                flavorScore = 0

            #self.log(4, troveName, versionStr, flavor, flavorScore, finalTimestamp)
            if (troveName, versionStr, flavor, localFlavorId) in allowed:
                continue
            allowed.add((troveName, versionStr, flavor, localFlavorId))

            if latestFilter == self._GET_TROVE_VERY_LATEST:
                d = troveVersions.setdefault(troveName, {})

                if flavorFilter == self._GET_TROVE_BEST_FLAVOR:
                    flavorIdentifier = localFlavorId
                else:
                    flavorIdentifier = flavor

                lastTimestamp, lastFlavorScore = d.get(
                        (branchId, flavorIdentifier), (0, -500000))[0:2]
                # this rule implements "later is better"; we've already
                # thrown out incompatible troves, so whatever is left
                # is at least compatible; within compatible, newer
                # wins (even if it isn't as "good" as something older)

                # FIXME: this OR-based serialization sucks.
                # if the following pairs of (score, timestamp) come in the
                # order showed, we end up picking different results.
                #  (assume GET_TROVE_BEST_FLAVOR here)
                # (1, 3), (3, 2), (2, 1)  -> (3, 2)  [WRONG]
                # (2, 1) , (3, 2), (1, 3) -> (1, 3)  [RIGHT]
                #
                # XXX: this is why the row order of the SQL result matters.
                #      We ain't doing the right thing here.
                if (flavorFilter == self._GET_TROVE_BEST_FLAVOR and
                    flavorScore > lastFlavorScore) or \
                    finalTimestamp > lastTimestamp:
                    d[(branchId, flavorIdentifier)] = \
                        (finalTimestamp, flavorScore, versionStr,
                         timeStamps, flavor)
                    #self.log(4, lastTimestamp, lastFlavorScore, d)

            elif flavorFilter == self._GET_TROVE_BEST_FLAVOR:
                assert(latestFilter == self._GET_TROVE_ALL_VERSIONS)
                assert(withFlavors)

                d = troveVersions.get(troveName, None)
                if d is None:
                    d = {}
                    troveVersions[troveName] = d

                lastTimestamp, lastFlavorScore = d.get(
                        (versionStr, localFlavorId), (0, -500000))[0:2]

                if (flavorScore > lastFlavorScore):
                    d[(versionStr, localFlavorId)] = \
                        (finalTimestamp, flavorScore, versionStr,
                         timeStamps, flavor)
            else:
                # if _GET_TROVE_ALL_VERSIONS is used, withFlavors must
                # be specified (or the various latest versions can't
                # be differentiated)
                assert(latestFilter == self._GET_TROVE_ALL_VERSIONS)
                assert(withFlavors)

                d = troveVersions.get(troveName, None)
                if d is None:
                    d = {}
                    troveVersions[troveName] = d

                version = self.versionStringToFrozen(versionStr, timeStamps)
                l = d.get(version, None)
                if l is None:
                    l = []
                    d[version] = l
                l.append(flavor)
        self.log(4, "extracted query results")

        if latestFilter == self._GET_TROVE_VERY_LATEST or \
                    flavorFilter == self._GET_TROVE_BEST_FLAVOR:
            newTroveVersions = {}
            for troveName, versionDict in troveVersions.iteritems():
                if withFlavors:
                    l = {}
                else:
                    l = []

                for (finalTimestamp, flavorScore, versionStr, timeStamps,
                     flavor) in versionDict.itervalues():
                    version = self.versionStringToFrozen(versionStr, timeStamps)
                    if withFlavors:
                        flist = l.setdefault(version, [])
                        flist.append(flavor or '')
                    else:
                        l.append(version)

                newTroveVersions[troveName] = l

            troveVersions = newTroveVersions

        self.log(4, "processed troveVersions")
        return troveVersions

    @accessReadOnly
    def troveNames(self, authToken, clientVersion, labelStr,
                   troveTypes = TROVE_QUERY_ALL):
        cu = self.db.cursor()

        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []

        if troveTypes == TROVE_QUERY_PRESENT:
            troveTypeClause = \
                'and Instances.troveType != %d' % trove.TROVE_TYPE_REMOVED
        elif troveTypes == TROVE_QUERY_NORMAL:
            troveTypeClause = \
                'and Instances.troveType = %d' % trove.TROVE_TYPE_NORMAL
        else:
            troveTypeClause = ''

        if not labelStr:
            cu.execute("""
            select Items.item from Items
            where Items.hasTrove = 1
              and exists ( select 1
                  from UserGroupInstancesCache as ugi
                  join Instances using (instanceId)
                  where ugi.userGroupId in (%s)
                    and Instances.itemId = Items.itemId
                    %s )
            """ % (",".join("%d" % x for x in roleIds),
                   troveTypeClause))
        else:
            cu.execute("""
            select Items.item from Items
            where Items.hasTrove = 1
              and exists ( select 1
                  from Labels
                  join LabelMap using (labelId)
                  join Nodes using (itemId, branchId)
                  join Instances using (itemId, versionId)
                  join UserGroupInstancesCache as ugi using (instanceId)
                  where ugi.userGroupId in (%s)
                    and Labels.label = ?
                    and LabelMap.itemId = Items.itemId
                    %s )
            """ % (",".join("%d" % x for x in roleIds), troveTypeClause),
                       labelStr)
        return [ x[0] for x in cu ]

    @accessReadOnly
    def getTroveVersionList(self, authToken, clientVersion, troveSpecs,
                            troveTypes = TROVE_QUERY_PRESENT):
        self.log(2, troveSpecs)
        troveFilter = {}
        for name, flavors in troveSpecs.iteritems():
            if len(name) == 0:
                name = None

            if type(flavors) is list:
                troveFilter[name] = { None : flavors }
            else:
                troveFilter[name] = { None : None }
        return self._getTroveList(authToken, clientVersion, troveFilter,
                                  withFlavors = True,
                                  troveTypes = troveTypes)

    @accessReadOnly
    def getTroveVersionFlavors(self, authToken, clientVersion, troveSpecs,
                               bestFlavor, troveTypes = TROVE_QUERY_PRESENT):
        self.log(2, troveSpecs)
        return self._getTroveVerInfoByVer(authToken, clientVersion, troveSpecs,
                              bestFlavor, self._GTL_VERSION_TYPE_VERSION,
                              latestFilter = self._GET_TROVE_ALL_VERSIONS,
                              troveTypes = troveTypes)

    @accessReadOnly
    def getAllTroveLeaves(self, authToken, clientVersion, troveSpecs,
                          troveTypes = TROVE_QUERY_PRESENT):
        self.log(2, troveSpecs)
        troveFilter = {}
        for name, flavors in troveSpecs.iteritems():
            if len(name) == 0:
                name = None
            if type(flavors) is list:
                troveFilter[name] = { None : flavors }
            else:
                troveFilter[name] = { None : None }
        # dispatch the more complex version to the old getTroveList
        if not troveSpecs == { '' : True }:
            return self._getTroveList(authToken, clientVersion, troveFilter,
                                  latestFilter = self._GET_TROVE_VERY_LATEST,
                                  withFlavors = True, troveTypes = troveTypes)

        # faster version for the "get-all" case
        # authenticate this user first
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return {}

        latestType = self._latestType(troveTypes)

        query = """
        select
            Items.item as trove,
            Versions.version as version,
            Flavors.flavor as flavor,
            Nodes.timeStamps as timeStamps
        from LatestCache
        join Instances using (itemId, versionId, flavorId)
        join Nodes on
            LatestCache.itemId = Nodes.itemId and
            LatestCache.branchId = Nodes.branchId and
            LatestCache.versionId = Nodes.versionId
        join Items on LatestCache.itemId = Items.itemId
        join Flavors on LatestCache.flavorId = Flavors.flavorId
        join Versions on LatestCache.versionId = Versions.versionId
        where LatestCache.userGroupId in (%s)
          and LatestCache.latestType = %d
        """ % (",".join("%d" % x for x in roleIds), latestType)
        cu.execute(query)
        ret = {}
        for (trove, version, flavor, timeStamps) in cu:
            version = self.versionStringToFrozen(version, timeStamps)
            retname = ret.setdefault(trove, {})
            flist = retname.setdefault(version, [])
            flist.append(flavor or '')
        return ret

    def _getTroveVerInfoByVer(self, authToken, clientVersion, troveSpecs,
                              bestFlavor, versionType, latestFilter,
                              troveTypes = TROVE_QUERY_PRESENT):
        self.log(3, troveSpecs)
        hasFlavors = False
        d = {}
        for (name, labels) in troveSpecs.iteritems():
            if not name:
                name = None

            d[name] = {}
            for label, flavors in labels.iteritems():
                if type(flavors) == list:
                    d[name][label] = flavors
                    hasFlavors = True
                else:
                    d[name][label] = None

        # FIXME: Usually when we want the very latest we don't want to be
        # constrained by the "best flavor". But just testing for
        # 'latestFilter!=self._GET_TROVE_VERY_LATEST' to avoid asking for
        # BEST_FLAVOR doesn't work because there are other things being keyed
        # on this in the _getTroveList function
        #
        # some MAJOR logic rework needed here...
        if bestFlavor and hasFlavors:
            flavorFilter = self._GET_TROVE_BEST_FLAVOR
        else:
            flavorFilter = self._GET_TROVE_ALL_FLAVORS
        return self._getTroveList(authToken, clientVersion, d,
                                  flavorFilter = flavorFilter,
                                  versionType = versionType,
                                  latestFilter = latestFilter,
                                  withFlavors = True, troveTypes = troveTypes)

    @accessReadOnly
    def getTroveVersionsByBranch(self, authToken, clientVersion, troveSpecs,
                                 bestFlavor, troveTypes = TROVE_QUERY_PRESENT):
        self.log(2, troveSpecs)
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_BRANCH,
                                          self._GET_TROVE_ALL_VERSIONS,
                                          troveTypes = troveTypes)

    @accessReadOnly
    def getTroveLeavesByBranch(self, authToken, clientVersion, troveSpecs,
                               bestFlavor, troveTypes = TROVE_QUERY_PRESENT):
        self.log(2, troveSpecs)
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_BRANCH,
                                          self._GET_TROVE_VERY_LATEST,
                                          troveTypes = troveTypes)

    @accessReadOnly
    def getTroveLeavesByLabel(self, authToken, clientVersion, troveSpecs,
                              bestFlavor, troveTypes = TROVE_QUERY_PRESENT):
        self.log(2, troveSpecs)
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_LABEL,
                                          self._GET_TROVE_VERY_LATEST,
                                          troveTypes = troveTypes)

    @accessReadOnly
    def getTroveVersionsByLabel(self, authToken, clientVersion, troveNameList,
                                bestFlavor, troveTypes = TROVE_QUERY_PRESENT):
        troveSpecs = troveNameList
        self.log(2, troveSpecs)
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_LABEL,
                                          self._GET_TROVE_ALL_VERSIONS,
                                          troveTypes = troveTypes)

    @accessReadOnly
    def getFileContentsFromTrove(self, authToken, clientVersion,
                                 troveName, version, flavor, pathList):
        self.log(2, troveName, version, flavor, pathList)

        trvList = self._lookupTroves(authToken, [(troveName, version, flavor)])
        for isPresent, hasAccess in trvList:
            if isPresent and not hasAccess:
                raise errors.InsufficientPermission

        pathList = [ base64.decodestring(x) for x in pathList ]
        cu = self.db.cursor()
        schema.resetTable(cu, 'tmpFilePaths')
        for row, path in enumerate(pathList):
            dirname, basename = os.path.split(path)
            cu.execute("INSERT INTO tmpFilePaths (row, dirname, basename) "
                       " VALUES (?, ?, ?)",
                       (row, dirname, basename), start_transaction=False)
        #self.db.analyze("tmpFilePaths")

        sql = '''
                SELECT row, fileId,FileVersions.version FROM
                Versions
                JOIN Flavors ON (Flavors.flavor=?)
                JOIN Items ON (Items.item=?)
                JOIN Instances ON
                    (Items.itemId = Instances.itemId AND
                     Versions.versionId = Instances.versionId AND
                     Flavors.flavorId = Instances.flavorId)
                JOIN TroveFiles USING(instanceId)
                JOIN FilePaths USING (filePathId)
                JOIN (SELECT tfp.row as row, fp.filePathId as filePathId
                       from FilePaths as fp
                       join Dirnames as d on fp.dirnameId = d.dirnameId
                       join Basenames as b on fp.basenameId = b.basenameId
                       join tmpFilePaths as tfp on
                           tfp.dirname = d.dirname and
                           tfp.basename = b.basename ) AS blah
                     USING(filePathId)
                JOIN FileStreams ON (TroveFiles.streamId=FileStreams.streamId)
                JOIN Versions AS FileVersions
                    ON (TroveFiles.versionId=FileVersions.versionId)
                WHERE Versions.version=?'''
        sql = cu.execute(sql, flavor, troveName, version)
        fileList = [None] * len(pathList)
        for row, fileId, fileVer in cu:
            fileList[row] = (fileId, fileVer)
        if None in fileList:
            missingPaths = [ pathList[idx]
                             for (idx, x) in enumerate(fileList)
                             if x is None ]
            raise errors.PathsNotFound(missingPaths)

        fileIdGen = (x[0] for x in fileList)
        rawStreams = self._getFileStreams(authToken, fileIdGen)
        rc = self._getFileContents(clientVersion, fileList, rawStreams)
        return rc

    @accessReadOnly
    def getFileContents(self, authToken, clientVersion, fileList,
                        authCheckOnly = False):
        self.log(2, "fileList", fileList)

        # We use _getFileStreams here for the permission checks.
        fileIdGen = (self.toFileId(x[0]) for x in fileList)
        rawStreams = self._getFileStreams(authToken, fileIdGen)

        if authCheckOnly:
            for stream, (encFileId, encVersion) in \
                                itertools.izip(rawStreams, fileList):
                if stream is None:
                    raise errors.FileStreamNotFound(
                                    self.toFileId(encFileId),
                                    self.toVersion(encVersion))
            return True
        return self._getFileContents(clientVersion, fileList, rawStreams)

    def _getFileContents(self, clientVersion, fileList, rawStreams):
        manifest = ManifestWriter(self.tmpPath)
        sizeList = []
        exception = None

        for stream, (encFileId, encVersion) in \
                            itertools.izip(rawStreams, fileList):
            if stream is None:
                # return an exception if we couldn't find one of
                # the streams
                exception = errors.FileStreamNotFound
            elif not files.frozenFileHasContents(stream):
                exception = errors.FileHasNoContents
            else:
                contents = files.frozenFileContentInfo(stream)
                filePath = self.repos.contentsStore.hashToPath(
                        contents.sha1())
                try:
                    size = os.stat(filePath).st_size
                    sizeList.append(size)
                    manifest.append(filePath,
                            expandedSize=size,
                            isChangeset=False,
                            preserveFile=True,
                            offset=0,
                            )
                except OSError, e:
                    if e.errno != errno.ENOENT:
                        raise
                    exception = errors.FileContentsNotFound

            if exception:
                raise exception(self.toFileId(encFileId),
                                self.toVersion(encVersion))

        name = manifest.close()
        url = os.path.join(self.urlBase(), "changeset?%s" % name)
        # client versions >= 44 use strings instead of ints for size
        # because xmlrpclib can't marshal ints > 2GiB
        if clientVersion >= 44:
            sizeList = [ str(x) for x in sizeList ]
        else:
            for size in sizeList:
                if size >= 0x80000000:
                    raise errors.InvalidClientVersion(
                        'This version of Conary does not support '
                        'downloading file contents larger than 2 '
                        'GiB.  Please install a new Conary '
                        'client.')
        return url, sizeList

    @accessReadOnly
    def getTroveLatestVersion(self, authToken, clientVersion, pkgName,
                              branchStr, troveTypes = TROVE_QUERY_PRESENT):
        self.log(2, pkgName, branchStr)
        r = self.getTroveLeavesByBranch(authToken, clientVersion,
                                { pkgName : { branchStr : None } },
                                True, troveTypes = troveTypes)
        if pkgName not in r:
            return 0
        elif len(r[pkgName]) != 1:
            return 0

        return r[pkgName].keys()[0]

    def _checkPermissions(self, authToken, chgSetList, hidden=False):
        trvList = self._lookupTroves(authToken,
                                     [(x[0], x[2][0], x[2][1])
                                      for x in chgSetList],
                                     hidden=hidden)
        for isPresent, hasAccess in trvList:
            if isPresent and not hasAccess:
                raise errors.InsufficientPermission

    def _getChangeSetObj(self, authToken, chgSetList, recurse,
                         withFiles, withFileContents, excludeAutoSource):
        # return a changeset object that has all the changesets
        # requested in chgSetList.  Also returns a list of extra
        # troves needed and files needed.
        cu = self.db.cursor()

        self._checkPermissions(authToken, chgSetList, hidden=True)
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            raise errors.InsufficientPermission

        cs = changeset.ReadOnlyChangeSet()
        l = self.toJobList(chgSetList)

        allTrovesNeeded = []
        allFilesNeeded = []
        allRemovedTroves = []

        for ret in self.repos.createChangeSet(l,
                                         recurse = recurse,
                                         withFiles = withFiles,
                                         withFileContents = withFileContents,
                                         excludeAutoSource = excludeAutoSource,
                                         roleIds = roleIds):
            (newCs, trovesNeeded, filesNeeded, removedTroves) = ret
            cs.merge(newCs)
            allTrovesNeeded += trovesNeeded
            allFilesNeeded += filesNeeded
            allRemovedTroves += removedTroves

        return (cs, allTrovesNeeded, allFilesNeeded, allRemovedTroves)

    def _createChangeSet(self, destFile, jobList, recurse = False, **kwargs):

        def oneChangeSet(destFile, jobs, **kwargs):
            # dedup jobs here; duplicates confuse the createChangeSet
            # iterator.
            jobDict = dict.fromkeys(jobs)
            jobOrder = jobDict.keys()
            for result in self.repos.createChangeSet(jobOrder, **kwargs):
                job = jobOrder.pop(0)
                jobDict[job] = result

            rc = []
            for job in jobs:
                cs, trovesNeeded, filesNeeded, removedTroves = jobDict[job]
                start = destFile.tell()
                size = cs.appendToFile(destFile, withReferences = True)

                rc.append((str(size), self.fromJobList(trovesNeeded),
                                  self.fromFilesNeeded(filesNeeded),
                                  self.fromJobList(removedTroves),
                                  str(destFile.tell() - start)))


            return rc

        # --- def _createChangeSet() begins here

        retList = []

        if recurse:
            for job in jobList:
                retList += oneChangeSet(destFile, [ job ], **kwargs)
        else:
            retList = oneChangeSet(destFile, jobList, recurse = recurse,
                                   **kwargs)

        return retList

    @accessReadOnly
    def getChangeSet(self, authToken, clientVersion, chgSetList, recurse,
                     withFiles, withFileContents, excludeAutoSource,
                     changeSetVersion = None, mirrorMode = False,
                     infoOnly = False, resumeOffset=None):
        # infoOnly and resumeOffset are for compatibility with the network
        # call; it's ignored here (but implemented in the front-side proxy)

        # try to log more information about these requests
        self.log(2, [x[0] for x in chgSetList],
                 list(set([x[2][0] for x in chgSetList])),
                 "recurse=%s withFiles=%s withFileContents=%s" % (
            recurse, withFiles, withFileContents))

        (fd, retpath) = tempfile.mkstemp(dir = self.tmpPath,
                                         suffix = '.ccs-out')
        url = 'file://localhost' + retpath
        outFile = util.ExtendedFdopen(fd)

        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            raise errors.InsufficientPermission

        try:
            # Requesting hidden troves directly is OK, e.g. commit hooks
            self._checkPermissions(authToken, chgSetList, hidden=True)
            chgSetList = self.toJobList(chgSetList)
            rc = self._createChangeSet(outFile, chgSetList,
                                    recurse = recurse,
                                    withFiles = withFiles,
                                    withFileContents = withFileContents,
                                    excludeAutoSource = excludeAutoSource,
                                    roleIds = roleIds,
                                    mirrorMode = mirrorMode)

            outFile.close()
        except:
            util.removeIfExists(retpath)
            raise

        return url, rc

    @accessReadOnly
    def getChangeSetFingerprints(self, authToken, clientVersion, chgSetList,
                    recurse, withFiles, withFileContents, excludeAutoSource,
                    mirrorMode = False):
        """
        The fingerprints of old troves new troves are relative to doesn't
        matter. If the old versions of a trove could change in a way which
        would invalidate a relative changeset, clients wouldn't be able to
        merge relative changesets against old troves stored in their databases!
        """

        newJobList = fingerprints.expandJobList(self.db, chgSetList, recurse)
        sigItems = []

        for fullJob in newJobList:
            if fullJob is None:
                # uncacheable job
                continue
            for job in fullJob:
                version = versions.VersionFromString(job[2][0])
                if version.trailingLabel().getHost() in self.serverNameList:
                    sigItems.append((job[0], job[2][0], job[2][1]))
                else:
                    sigItems.append(None)

        pureSigList = self.getTroveInfo(authToken, SERVER_VERSIONS[-1],
                                        trove._TROVEINFO_TAG_SIGS,
                                        [ x for x in sigItems if x ])
        pureMetaList = self.getTroveInfo(authToken, SERVER_VERSIONS[-1],
                                        trove._TROVEINFO_TAG_METADATA,
                                        [ x for x in sigItems if x ])
        sigList = []
        metaList = []
        sigCount = 0
        for item in sigItems:
            if not item:
                sigList.append(None)
                metaList.append(None)
            else:
                sigList.append(pureSigList[sigCount])
                metaList.append(pureMetaList[sigCount])
                sigCount += 1

        # 0 is a version number for this signature block; changing this will
        # invalidate all change set signatures downstream
        header = "".join( ('0', "%d" % recurse, "%d" % withFiles,
                    "%d" % withFileContents, "%d" % excludeAutoSource ) )
        if mirrorMode:
            header += '2'

        sigCount = 0
        finalFingerprints = []
        for origJob, fullJob in itertools.izip(chgSetList, newJobList):
            if fullJob is None:
                # uncachable job
                finalFingerprints.append('')
                continue

            fpList = [ header ]
            fpList += [ origJob[0], str(origJob[1][0]), str(origJob[1][1]),
                        origJob[2][0], origJob[2][1], "%d" % origJob[3] ]
            for job in fullJob:
                if job[1][0]:
                    troveTup = (job[0], job[1][0], job[1][1])
                    fpList.append(fingerprints._troveFp(troveTup, None, None))

                fp = fingerprints._troveFp(sigItems[sigCount],
                                        sigList[sigCount],
                                        metaList[sigCount])
                sigCount += 1

                fpList.append(fp)

            fp = sha1helper.sha1String("\0".join(fpList))
            finalFingerprints.append(sha1helper.sha1ToString(fp))

        return finalFingerprints

    @accessReadOnly
    def getDepSuggestions(self, authToken, clientVersion, label, requiresList,
                          leavesOnly = False):
        if not self.auth.check(authToken, write = False,
                               label = self.toLabel(label)):
            raise errors.InsufficientPermission
        self.log(2, label, requiresList)
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            raise errors.InsufficientPermission
        ret = self.deptable.resolve(roleIds, label,
                                    depList = requiresList,
                                    leavesOnly = leavesOnly)
        return ret

    @accessReadOnly
    def getDepSuggestionsByTroves(self, authToken, clientVersion, requiresList,
                                  troveList):
        # the query will run through the RoleInstancesCache filter
        # and only return stuff this user has access to....
        self.log(2, troveList, requiresList)
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            raise errors.InsufficientPermission
        ret = self.deptable.resolve(roleIds, label = None,
                                    depList = requiresList,
                                    troveList = troveList)
        return ret

    @accessReadOnly
    def commitCheck(self, authToken, clientVersion, troveVersionList):
        # troveVersionList is a list of (name, version) tuples
        # commitCheck does it own authToken checking and validation
        return self.auth.commitCheck(
            authToken, ((n, self.toVersion(v)) for n,v in troveVersionList))

    def _checkCommitPermissions(self, authToken, verList, mirror, hidden):
        if (mirror or hidden) and \
               not self.auth.authCheck(authToken, mirror=(mirror or hidden)):
            raise errors.InsufficientPermission
        # verList items are (name, oldVer, newVer). we check both
        # combinations in one step
        def _fullVerList(verList):
            for name, oldVer, newVer in verList:
                assert(newVer)
                yield (name, newVer)
                if oldVer:
                    yield (name, oldVer)
        # check newVer
        if False in self.auth.commitCheck(authToken, _fullVerList(verList) ):
            raise errors.InsufficientPermission

    @accessReadOnly
    def prepareChangeSet(self, authToken, clientVersion, jobList=None,
                         mirror=False):
        if jobList:
            checkList = [(x.name, x.old[0], x.new[0])
                    for x in self.toJobList(jobList)]
            self._checkCommitPermissions(authToken, checkList, mirror, False)

        self.log(2, authToken[0])
        (fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-in')
        os.close(fd)
        fileName = os.path.basename(path)

        # this needs to match up exactly with the parsing of the url we do
        # in commitChangeSet.
        csPath = self.urlBase() + "?%s" % fileName[:-3]
        # for client versions >= 69, we also return a bool to signify
        # if the client should use the getCommitProgress() call
        if clientVersion >= 69:
            return csPath, not self.standalone
        return csPath

    @accessReadWrite
    def presentHiddenTroves(self, authToken, clientVersion):
        # Need both mirror and write permissions.
        if not (self.auth.authCheck(authToken, mirror=True)
                and self.auth.check(authToken, write=True)):
            raise errors.InsufficientPermission

        self.repos.troveStore.presentHiddenTroves()

        return ''

    @accessReadWrite
    def commitChangeSet(self, authToken, clientVersion, url, mirror = False,
                        hidden = False):
        base = util.normurl(self.urlBase())
        url = util.normurl(url)
        if not url.startswith(base):
            raise errors.RepositoryError(
                'The changeset that is being committed was not '
                'uploaded to a URL on this server.  The url is "%s", this '
                'server is "%s".'
                %(url, base))
        # +1 strips off the ? from the query url
        fileName = url[len(base) + 1:] + '-in'
        path = "%s/%s" % (self.tmpPath, fileName)
        statusPath = path + '-status'
        self.log(2, authToken[0], url, 'mirror=%s' % (mirror,))
        attempt = 1
        while True:
            # raise InsufficientPermission if we can't read the changeset
            try:
                cs = changeset.ChangeSetFromFile(path)
            except Exception, e:
                raise HiddenException(e, errors.CommitError(
                                "server cannot open change set to commit"))
            # because we have a temporary file we need to delete, we
            # need to catch the DatabaseLocked errors here and retry
            # the commit ourselves
            try:
                ret = self._commitChangeSet(authToken, cs,
                                            mirror=mirror, hidden=hidden,
                                            statusPath=statusPath)
            except sqlerrors.DatabaseLocked, e:
                # deadlock occurred; we rollback and try again
                log.error("Deadlock id %d: %s", attempt, str(e.args))
                self.log(1, "Deadlock id %d: %s" %(attempt, str(e.args)))
                if attempt < self.deadlockRetry:
                    self.db.rollback()
                    attempt += 1
                    continue
                break
            except Exception, e:
                break
            else: # all went well
                util.removeIfExists(path)
                util.removeIfExists(statusPath)
                return ret
        # we only reach here if we could not handle the exception above
        util.removeIfExists(path)
        # Figure out what to return back
        if isinstance(e, sqlerrors.DatabaseLocked):
            # too many retries
            raise errors.RepositoryLocked()
        raise

    def _commitChangeSet(self, authToken, cs, mirror = False,
                         hidden = False, statusPath = None):
        # walk through all of the branches this change set commits to
        # and make sure the user has enough permissions for the operation
        verList = ((x.getName(), x.getOldVersion(), x.getNewVersion())
                    for x in cs.iterNewTroveList())
        self._checkCommitPermissions(authToken, verList, mirror, hidden)

        items = {}
        removedList = []
        # check removed permissions; _checkCommitPermissions can't do
        # this for us since it's based on the trove type
        for troveCs in cs.iterNewTroveList():
            if troveCs.troveType() != trove.TROVE_TYPE_REMOVED:
                continue

            removedList.append(troveCs.getNewNameVersionFlavor())
            (name, version, flavor) = troveCs.getNewNameVersionFlavor()

            if not self.auth.authCheck(authToken, mirror = (mirror or hidden)):
                raise errors.InsufficientPermission
            if not self.auth.check(authToken, remove = True,
                                   label = version.branch().label(),
                                   trove = name):
                raise errors.InsufficientPermission

            items.setdefault((version, flavor), []).append(name)

        self.log(2, authToken[0], 'mirror=%s' % (mirror,),
                 [ (x[1], x[0][0].asString(), x[0][1]) for x in items.iteritems() ])
        self.repos.commitChangeSet(cs, mirror = mirror,
                                   hidden = hidden,
                                   serialize = self.serializeCommits,
                                   statusPath=statusPath)

        if not self.commitAction:
            return True

        userName = authToken[0]
        if not isinstance(userName, basestring):
            if userName.username:
                # A ValidUser token with a username specified.
                userName = userName.username
            else:
                # No username available.
                userName = 'unknown'

        d = { 'reppath' : self.urlBase(urlName = False),
              'user' : userName,
              }
        cmd = self.commitAction % d
        p = util.popen(cmd, "w")
        try:
            for troveCs in cs.iterNewTroveList():
                p.write("%s\n%s\n%s\n" %(troveCs.getName(),
                                         troveCs.getNewVersion().asString(),
                                         deps.formatFlavor(troveCs.getNewFlavor())))
            p.close()
        except (IOError, RuntimeError), e:
            # util.popen raises RuntimeError on error.  p.write() raises
            # IOError on error (broken pipe, etc)
            # FIXME: use a logger for this
            sys.stderr.write('commitaction failed: %s\n' %e)
            sys.stderr.flush()
        except Exception, e:
            sys.stderr.write('unexpected exception occurred when running '
                             'commitaction: %s\n' %e)
            sys.stderr.flush()

        return True

    @accessReadOnly
    def getCommitProgress(self, authToken, clientVersion, url):
        base = util.normurl(self.urlBase())
        url = util.normurl(url)
        if not url.startswith(base):
            raise errors.RepositoryError(
                'The changeset that is being committed was not '
                'uploaded to a URL on this server.  The url is "%s", this '
                'server is "%s".'
                %(url, base))
        # +1 strips off the ? from the query url
        fileName = url[len(base) + 1:] + "-in-status"
        path = "%s/%s" % (self.tmpPath, fileName)
        try:
            buf = file(path).read()
            return cPickle.loads(buf)
        except IOError:
            return False

    @accessReadOnly
    def getFileContentsCapsuleInfo(self, authToken, clientVersion, fileList):
        # OBSOLETE
        return [''] * len(fileList)

    # retrieve the raw streams for a fileId list passed in as a generator
    def _getFileStreams(self, authToken, fileIdGen):
        self.log(3)
        cu = self.db.cursor()

        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return {}
        schema.resetTable(cu, 'tmpFileId')

        # we need to make sure we don't look up the same fileId multiple
        # times to avoid asking the sql server to do busy work
        fileIdMap = {}
        i = 0               # protect against empty fileIdGen
        for i, fileId in enumerate(fileIdGen):
            fileIdMap.setdefault(fileId, []).append(i)
        uniqIdList = fileIdMap.keys()

        # now i+1 is how many items we shall return
        # None in streamMap means the stream wasn't found.
        streamMap = [ None ] * (i+1)

        # use the list of uniqified fileIds to look up streams in the repo
        def _iterIdList(uniqIdList):
            for i, fileId in enumerate(uniqIdList):
                #cu.execute("INSERT INTO tmpFileId (itemId, fileId) VALUES (?, ?)",
                #           (i, cu.binary(fileId)), start_transaction=False)
                yield ((i, cu.binary(fileId)))
        self.db.bulkload("tmpFileId", _iterIdList(uniqIdList),
                         ["itemId", "fileId"], start_transaction=False)
        self.db.analyze("tmpFileId")
        q = """
        SELECT DISTINCT
            tmpFileId.itemId, FileStreams.stream
        FROM tmpFileId
        JOIN FileStreams USING (fileId)
        JOIN TroveFiles USING (streamId)
        JOIN UserGroupInstancesCache ON
            TroveFiles.instanceId = UserGroupInstancesCache.instanceId
        WHERE FileStreams.stream IS NOT NULL
          AND UserGroupInstancesCache.userGroupId IN (%(roleids)s)
        """ % { 'roleids' : ", ".join("%d" % x for x in roleIds) }
        cu.execute(q)

        for (i, stream) in cu:
            fileId = uniqIdList[i]
            if fileId is None:
                 # we've already found this one
                 continue
            if stream is None:
                continue
            for streamIdx in fileIdMap[fileId]:
                streamMap[streamIdx] = stream
            # mark as processed
            uniqIdList[i] = None
        # FIXME: the fact that we're not extracting the list ordered
        # makes it very hard to return an iterator out of this
        # function - for now, returning a list will do...
        return streamMap

    @accessReadOnly
    def getFileVersions(self, authToken, clientVersion, fileList):
        self.log(2, "fileList", fileList)

        # build the list of fileIds for query
        fileIdGen = (self.toFileId(fileId) for (pathId, fileId) in fileList)

        # we rely on _getFileStreams to do the auth for us
        rawStreams = self._getFileStreams(authToken, fileIdGen)
        # return an exception if we couldn't find one of the streams
        if None in rawStreams:
            fileId = self.toFileId(fileList[rawStreams.index(None)][1])
            raise errors.FileStreamMissing(fileId)

        streamMap = [ None ] * len(fileList)
        for i,  (stream, (pathId, fileId)) in enumerate(itertools.izip(rawStreams, fileList)):
            # XXX the only thing we use the pathId for is to set it in
            # the file object; we should just pass the stream back and
            # let the client set it to avoid sending it back and forth
            # for no particularly good reason
            streamMap[i] = self.fromFileAsStream(pathId, stream, rawPathId = True)
        return streamMap

    @accessReadOnly
    def getFileVersion(self, authToken, clientVersion, pathId, fileId,
                       withContents = 0):
        # withContents is legacy; it was never used in conary 1.0.x
        assert(not withContents)
        self.log(2, pathId, fileId, "withContents=%s" % (withContents,))
        # getFileVersions is responsible for authenticating this call
        l = self.getFileVersions(authToken, SERVER_VERSIONS[-1],
                                 [ (pathId, fileId) ])
        assert(len(l) == 1)
        return l[0]

    @accessReadOnly
    def getPackageBranchPathIds(self, authToken, clientVersion, sourceName,
                                branch, dirnames=[], fileIds=None):
        # dirnames should be a list of prefixes to look for
        # It tries to limit the number of results for things that generate
        # unique paths with each build (e.g. the kernel).
        # Added as part of protocol version 39
        # fileIds should be a string with concatenated fileId's to be searched
        # in the database. A path found with a search of file ids should be
        # preferred over a path found by looking up the latest paths built
        # from that source trove.
        # In practical terms, this means that we could jump several revisions
        # back in a file's history.
        # Added as part of protocol version 42
        # decode the fileIds to check before doing heavy work
        if fileIds:
            fileIds = base64.b64decode(fileIds)
        else:
            fileIds = ""
        def splitFileIds(fileIds):
            fileIdLen = 20
            assert(len(fileIds) % fileIdLen == 0)
            fileIdCount = len(fileIds) // fileIdLen
            for i in range(fileIdCount):
                start = fileIdLen * i
                end = start + fileIdLen
                yield fileIds[start : end]
        # fileIds need to be unique for performance reasons
        fileIds = set(splitFileIds(fileIds))
        self.log(2, sourceName, branch, dirnames, fileIds)
        cu = self.db.cursor()

        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return {}

        def _lookupIds(cu, itemList, selectQlist):
            schema.resetTable(cu, "tmpPaths")
            cu.executemany("insert into tmpPaths (path) values (?)",
                    [(cu.binary(x),) for x in itemList],
                    start_transaction=False)
            self.db.analyze("tmpPaths")
            schema.resetTable(cu, "tmpId")
            if cu.driver == "mysql" and len(selectQlist) > 1:
                # MySQL stupidity: a temporaray table can not be used twice in
                # the same statement, so we have to perform the union manually
                tmpId = []
                for selectQ in selectQlist:
                    cu.execute(selectQ)
                    tmpId += [ x[0] for x in cu.fetchall() ]
                cu.executemany("insert into tmpId(id) values (?)", set(tmpId))
            else:
                cu.execute("""insert into tmpId (id) %s """ % (
                    " union ".join(selectQlist),))
            self.db.analyze("tmpId")
            return """join tmpId on fp.dirnameId = tmpId.id """

        prefixQuery = ""
        # Before version 62, dirnames were sent as prefixes. That table has
        # been dropped though, so just return unfiltered results.
        if dirnames and clientVersion >= 62:
            prefixQuery = _lookupIds(cu, dirnames, [
                    """ select d.dirnameId from tmpPaths
                        join Dirnames as d on tmpPaths.path = d.dirname """ ])

        schema.resetTable(cu, "tmpPathIdLookup")
        query = """
        insert into tmpPathIdLookup (versionId, filePathId, streamId, finalTimestamp)
        select distinct
            tf.versionId, tf.filePathId, tf.streamId, Nodes.finalTimestamp
        from UserGroupInstancesCache as ugi
        join Instances using (instanceId)
        join Nodes on Instances.itemId = Nodes.itemId and Instances.versionId = Nodes.versionId
        join Items on Nodes.sourceItemId = Items.itemId
        join TroveFiles as tf on Instances.instanceId = tf.instanceId
        where Items.item = ?
          and Nodes.branchId = ( select branchId from Branches where branch = ? )
          and ugi.userGroupId in (%s) """ % (",".join("%d" % x for x in roleIds), )
        cu.execute(query, (sourceName, branch))
        self.db.analyze("tmpPathIdLookup")
        # now decode the results to human-readable strings
        cu.execute("""
        select fp.pathId, d.dirname, b.basename, v.version, fs.fileId, tpil.finalTimestamp
        from tmpPathIdLookup as tpil
        join Versions as v on tpil.versionId = v.versionId
        join FilePaths as fp on tpil.filePathId = fp.filePathId
        join FileStreams as fs on tpil.streamId = fs.streamId
        join Dirnames as d on fp.dirnameId = d.dirnameId
        join Basenames as b on fp.basenameId = b.basenameId
        %s
        order by tpil.finalTimestamp desc
        """ % (prefixQuery,))
        ids = {}
        for (pathId, dirname, basename, version, fileId, timeStamp) in cu:
            encodedPath = self.fromPath( os.path.join(cu.frombinary(dirname),
                cu.frombinary(basename)) )
            currVal = ids.get(encodedPath, None)
            newVal = (cu.frombinary(pathId), version, cu.frombinary(fileId))
            if currVal is None:
                ids[encodedPath] = newVal
                continue
            # if we already had a value set, we prefer to use the one
            # that has a fileId in the set we were sent
            if newVal[2] in fileIds and not (currVal[2] in fileIds):
                ids[encodedPath] = newVal
        # prepare for return
        ids = dict([(k, (self.fromPathId(v[0]), v[1], self.fromFileId(v[2])))
                    for k,v in ids.iteritems()])
        return ids

    def _lookupTroves(self, authToken, troveList, hidden = False):
        # given a troveList of (n, v, f) returns a sequence of tuples:
        # (True, False) = trove present, no access
        # (True, True) = trove present, access
        # (False, False) = trove not present
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        results = [ (False, False) ] * len(troveList)
        if not roleIds:
            return results

        schema.resetTable(cu, "tmpNVF")
        def _iterTroveList(troveList):
            for i, item in enumerate(troveList):
                yield (i, item[0], item[1], item[2])
        self.db.bulkload("tmpNVF", _iterTroveList(troveList),
                         ["idx", "name", "version", "flavor"],
                         start_transaction=False)
        self.db.analyze("tmpNVF")
        if hidden:
            hiddenClause = ("OR Instances.isPresent = %d"
                        % instances.INSTANCE_PRESENT_HIDDEN)
        else:
            hiddenClause = ""

        # for each n,v,f return the index and if any UGIC entry
        # gives access to the trove.  If MAX(CASE...) GROUP BY idx
        # is too awful we could always return all rows and calculate
        # on the client side.
        query = """
        SELECT idx, MAX(CASE WHEN (ugi.userGroupId in (%s)
                                   AND (Instances.isPresent = ? %s))
                             THEN 1 ELSE 0 END)
        FROM tmpNVF
        JOIN Items ON
            tmpNVF.name = Items.item
        JOIN Versions ON
            tmpNVF.version = Versions.version
        JOIN Flavors ON
            (tmpNVF.flavor is NOT NULL AND tmpNVF.flavor = Flavors.flavor) OR
            (tmpNVF.flavor is NULL AND Flavors.flavorId = 0)
        JOIN Instances ON
            Instances.itemId = Items.itemId AND
            Instances.versionId = Versions.versionId AND
            Instances.flavorId = Flavors.flavorId
        JOIN UserGroupInstancesCache as ugi ON
            Instances.instanceId = ugi.instanceId
        GROUP BY idx
            """ % (
            ",".join("%d" % x for x in roleIds), hiddenClause)
        cu.execute(query, instances.INSTANCE_PRESENT_NORMAL)
        for row in cu:
            # idx, has access
            results[row[0]] = (True, bool(row[1]))
        return results

    @accessReadOnly
    def hasTroves(self, authToken, clientVersion, troveList, hidden = False):
        # returns False for troves the user doesn't have permission to view
        self.log(2, troveList)
        return [ x[1] for x in self._lookupTroves(authToken, troveList,
                                                  hidden=hidden) ]

    @accessReadOnly
    def getTrovesByPaths(self, authToken, clientVersion, pathList, label,
                         all=False):
        self.log(2, pathList, label, all)
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return {}

        if all:
            latestClause = ''
        else:
            latestClause ="""JOIN LatestCache on
            Nodes.itemId = LatestCache.itemId and
            Nodes.versionId = LatestCache.versionId and
            Instances.flavorId = LatestCache.flavorId and
            ugi.userGroupId = LatestCache.userGroupId"""

        schema.resetTable(cu, 'tmpFilePaths')
        for row, path in enumerate(pathList):
            if not isinstance(path, basestring):
                # Somebody's broken script sends requests where the paths are
                # lists, so handle that without throwing an exception.
                return {}
            dirname, basename = os.path.split(path)
            cu.execute("INSERT INTO tmpFilePaths (row, dirname, basename) VALUES (?, ?, ?)",
                       (row, dirname, basename), start_transaction=False)
        self.db.analyze("tmpFilePaths")

        query = """
        SELECT userQ.row, Items.item, Versions.version, Flavors.flavor,
            Nodes.timeStamps
        FROM ( select tfp.row as row, fp.filePathId as filePathId
               from FilePaths as fp
               join Dirnames as d on fp.dirnameId = d.dirnameId
               join Basenames as b on fp.basenameId = b.basenameId
               join tmpFilePaths as tfp on
                   tfp.dirname = d.dirname and
                   tfp.basename = b.basename ) as userQ
        JOIN TroveFiles using(filePathId)
        JOIN Instances using(instanceId)
        JOIN Nodes on
            Instances.itemId = Nodes.itemId
            and Instances.versionId = Nodes.versionId
        JOIN LabelMap on
            Nodes.itemId = LabelMap.itemId
            and Nodes.branchId = LabelMap.branchId
        JOIN Labels on LabelMap.labelId = Labels.labelId
        JOIN UserGroupInstancesCache as ugi on
            Instances.instanceId = ugi.instanceId
        %s
        JOIN Items on Instances.itemId = Items.itemId
        JOIN Versions on Instances.versionId = Versions.versionId
        JOIN Flavors on Instances.flavorId = Flavors.flavorId
        WHERE ugi.userGroupId in (%s)
          AND Instances.isPresent = %d
          AND Labels.label = ?
        ORDER BY Nodes.finalTimestamp DESC
        """ % (latestClause, ",".join("%d" % x for x in roleIds),
               instances.INSTANCE_PRESENT_NORMAL)
        cu.execute(query, label)

        results = [ {} for x in pathList ]
        for idx, name, versionStr, flavor, timeStamps in cu:
            version = versions.VersionFromString(versionStr, timeStamps=[
                float(x) for x in timeStamps.split(':')])
            branch = version.branch()
            retl = results[idx].setdefault((name, branch, flavor), [])
            retl.append(self.freezeVersion(version))
        def _iterAll(resList):
            for (n,b,f), verList in resList.iteritems():
                for v in verList:
                    yield (n,v,f)
        if all:
            return [ list(_iterAll(x)) for x in results ]
        # otherwise, the version stored first is the most recent and
        # is the one that needs to be returned
        return [ [ (n, vl[0], f) for (n,b,f),vl in x.iteritems() ]
                 for x in results ]

    @accessReadOnly
    def getCollectionMembers(self, authToken, clientVersion, troveName,
                             branch):
        if not self.auth.check(authToken, write = False,
                               trove = troveName,
                               label = self.toBranch(branch).label()):
            raise errors.InsufficientPermission
        self.log(2, troveName, branch)
        cu = self.db.cursor()
        query = """
            SELECT DISTINCT IncludedItems.item FROM
                Items, Nodes, Branches, Instances, TroveTroves,
                Instances AS IncludedInstances,
                Items AS IncludedItems
            WHERE
                Items.item = ? AND
                Items.itemId = Nodes.itemId AND
                Nodes.branchId = Branches.branchId AND
                Branches.branch = ? AND
                Instances.itemId = Nodes.itemId AND
                Instances.versionId = Nodes.versionId AND
                TroveTroves.instanceId = Instances.instanceId AND
                IncludedInstances.instanceId = TroveTroves.includedId AND
                IncludedItems.itemId = IncludedInstances.itemId
            """
        args = [troveName, branch]
        cu.execute(query, args)
        self.log(4, "execute query", query, args)
        ret = [ x[0] for x in cu ]
        return ret

    @accessReadOnly
    def getTrovesBySource(self, authToken, clientVersion, sourceName,
                          sourceVersion):
        # You should be able to get all the troves associated with a source
        # even if you cannot get the source itself.   This is important
        # for derived recipes.  Thus, we don't check access until
        # we've calculated a result.
        self.log(2, sourceName, sourceVersion)
        versionMatch = sourceVersion + '-%'
        cu = self.db.cursor()
        query = """
        SELECT Items.item, Versions.version, Flavors.flavor
        FROM Instances
        JOIN Nodes ON
            Instances.itemId = Nodes.itemId AND
            Instances.versionId = Nodes.versionId
        JOIN Items AS SourceItems ON
            Nodes.sourceItemId = SourceItems.itemId
        JOIN Items ON
            Instances.itemId = Items.itemId
        JOIN Versions ON
            Instances.versionId = Versions.versionId
        JOIN Flavors ON
            Instances.flavorId = Flavors.flavorId
        WHERE
            SourceItems.item = ? AND
            Versions.version LIKE ?
        """
        args = [sourceName, versionMatch]
        cu.execute(query, args)
        self.log(4, "execute query", query, args)
        # tuple(x) so that xmlrpc can marshal it
        troveList = [ tuple(x) for x in cu ]
        hasAccess = self.auth.batchCheck(authToken, troveList, write=False, cu=cu)
        if False in hasAccess:
            # don't return a partial answer to this question.
            raise errors.InsufficientPermission
        return troveList

    @accessReadOnly
    def getPackageCreatorTroves(self, authToken, clientVersion, serverName):
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []
        query = """
        SELECT Items.item, Versions.version, Flavors.flavor,
               TroveInfo.data
        FROM (
            SELECT DISTINCT TroveInfo.instanceId AS instanceId FROM TroveInfo
                JOIN UserGroupInstancesCache as ugi ON
                    TroveInfo.instanceId = ugi.instanceId AND
                    TroveInfo.infoType = %d
                WHERE
                    ugi.userGroupId in (%s)
        ) AS Pkg_Created
        JOIN Instances ON
            Pkg_Created.instanceId = Instances.instanceId
        JOIN Items ON
            Instances.itemId = Items.itemId
        JOIN Versions ON
            Instances.versionId = Versions.versionId
        JOIN Flavors ON
            Instances.flavorId = Flavors.flavorId
        JOIN TroveInfo ON
            Instances.instanceId = TroveInfo.instanceId AND
            TroveInfo.infoType = %d
        """ % (trove._TROVEINFO_TAG_PKGCREATORDATA,
               ",".join("%d" % x for x in roleIds),
               trove._TROVEINFO_TAG_PKGCREATORDATA)
        cu.execute(query)
        return sorted([ (name, version, flavor, cu.frombinary(data))
            for (name, version, flavor, data) in cu ])

    @accessReadWrite
    def addMetadataItems(self, authToken, clientVersion, itemList):
        self.log(2, "adding %i metadata items" %len(itemList))
        l = []
        metadata = self.getTroveInfo(authToken, SERVER_VERSIONS[-1],
                                     trove._TROVEINFO_TAG_METADATA,
                                     [ x[0] for x in itemList ])
        # if we're signaled that any trove is missing, bail out
        missing = [ i for i,x in enumerate(metadata) if x[0] < 0]
        if missing: # report the first one
            n,v,f = itemList[missing[0]][0]
            raise errors.TroveMissing(n, v)
        for (troveTup, item), (presence, existing) in itertools.izip(itemList, metadata):
            m = trove.Metadata(base64.decodestring(existing))
            mi = trove.MetadataItem(base64.b64decode(item))
            # don't allow items which don't have digests
            if not list(itertools.chain(mi.oldSignatures, mi.signatures)):
                raise errors.RepositoryError("Metadata cannot be added "
                                             "without a digest")
            mi.verifyDigests()
            m.addItem(mi)
            i = trove.TroveInfo()
            i.metadata = m
            l.append((troveTup, i.freeze()))
        return self._setTroveInfo(authToken, clientVersion, l)

    @accessReadWrite
    @requireClientProtocol(45)
    def addDigitalSignature(self, authToken, clientVersion, name, version,
                            flavor, encSig):
        version = self.toVersion(version)
        if not self.auth.check(authToken, write = True, trove = name,
                               label = version.branch().label()):
            raise errors.InsufficientPermission
        flavor = self.toFlavor(flavor)
        self.log(2, name, version, flavor)

        sigs = trove.VersionedSignaturesSet(base64.b64decode(encSig))

        # get the key being used; they should all be the same of course
        fingerprint = None
        for sigBlock in sigs:
            for sig in sigBlock.signatures:
                if fingerprint is None:
                    fingerprint = sig[0]
                elif fingerprint != sig[0]:
                    raise errors.IncompatibleKey('Multiple keys in signature')

        # ensure repo knows this key
        keyCache = self.repos.troveStore.keyTable.keyCache
        pubKey = keyCache.getPublicKey(fingerprint)

        if pubKey.isRevoked():
            raise errors.IncompatibleKey('Key %s has been revoked. '
                                  'Signature rejected' %sig[0])
        if (pubKey.getTimestamp()) and (pubKey.getTimestamp() < time.time()):
            raise errors.IncompatibleKey('Key %s has expired. '
                                  'Signature rejected' %sig[0])

        # start a transaction now as a means of protecting against
        # simultaneous signing by different clients. The real fix
        # would need "SELECT ... FOR UPDATE" support in the SQL
        # engine, which is not universally available
        cu = self.db.transaction()

        # get the instanceId that corresponds to this trove.
        cu.execute("""
        SELECT instanceId FROM Instances
        JOIN Items ON Instances.itemId = Items.itemId
        JOIN Versions ON Instances.versionId = Versions.versionId
        JOIN Flavors ON Instances.flavorId = Flavors.flavorId
        WHERE Items.item = ?
          AND Versions.version = ?
          AND Flavors.flavor = ?
        """, (name, version.asString(), flavor.freeze()))
        ret = cu.fetchone()
        if not ret:
            raise errors.TroveMissing(name, version)
        instanceId = ret[0]
        # try to create a row lock for the signature record if needed
        cu.execute("UPDATE TroveInfo SET changed = changed "
                   "WHERE instanceId = ? AND infoType = ?",
                   (instanceId, trove._TROVEINFO_TAG_SIGS))

        # now we should have the proper locks
        trv = self.repos.getTrove(name, version, flavor)

        # don't add exactly the same set of sigs again
        try:
            existingSigs = trv.getDigitalSignature(fingerprint)

            if (set(x.version() for x in existingSigs) ==
                set(x.version() for x in sigs)):
                raise errors.AlreadySignedError("Trove already signed by key")
        except KeyNotFound:
            pass

        trv.addPrecomputedDigitalSignature(sigs)
        # verify the new signature is actually good
        trv.verifyDigitalSignatures(keyCache = keyCache)

        # see if there's currently any troveinfo in the database
        cu.execute("""
        SELECT COUNT(*) FROM TroveInfo WHERE instanceId=? AND infoType=?
        """, (instanceId, trove._TROVEINFO_TAG_SIGS))
        trvInfo = cu.fetchone()[0]
        if trvInfo:
            # we have TroveInfo, so update it
            cu.execute("""
            UPDATE TroveInfo SET data = ?
            WHERE instanceId = ? AND infoType = ?
            """, (cu.binary(trv.troveInfo.sigs.freeze()), instanceId,
                  trove._TROVEINFO_TAG_SIGS))
        else:
            # otherwise we need to create a new row with the signatures
            cu.execute("""
            INSERT INTO TroveInfo (instanceId, infoType, data)
            VALUES (?, ?, ?)
            """, (instanceId, trove._TROVEINFO_TAG_SIGS,
                  cu.binary(trv.troveInfo.sigs.freeze())))
        return True

    @accessReadWrite
    def addNewAsciiPGPKey(self, authToken, label, user, keyData):
        if (not self.auth.authCheck(authToken, admin = True)
            and (not self.auth.check(authToken, allowAnonymous = False) or
                     authToken[0] != user)):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], label, user)
        uid = self.auth.userAuth.getUserIdByName(user)
        self.repos.troveStore.keyTable.addNewAsciiKey(uid, keyData)
        return True

    @accessReadWrite
    def addNewPGPKey(self, authToken, label, user, encKeyData):
        if (not self.auth.authCheck(authToken, admin = True)
            and (not self.auth.check(authToken, allowAnonymous = False) or
                     authToken[0] != user)):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], label, user)
        uid = self.auth.userAuth.getUserIdByName(user)
        keyData = base64.b64decode(encKeyData)
        self.repos.troveStore.keyTable.addNewKey(uid, keyData)
        return True

    @accessReadWrite
    def changePGPKeyOwner(self, authToken, label, user, key):
        if not self.auth.authCheck(authToken, admin = True):
            raise errors.InsufficientPermission
        if user:
            uid = self.auth.userAuth.getUserIdByName(user)
        else:
            uid = None
        self.log(2, authToken[0], label, user, str(key))
        self.repos.troveStore.keyTable.updateOwner(uid, key)
        return True

    @accessReadOnly
    def getAsciiOpenPGPKey(self, authToken, label, keyId):
        # don't check auth. this is a public function
        return self.repos.troveStore.keyTable.getAsciiPGPKeyData(keyId)

    @accessReadOnly
    def listUsersMainKeys(self, authToken, label, user = None):
        # the only reason to lock this fuction down is because it correlates
        # a valid user to valid fingerprints. neither of these pieces of
        # information is sensitive separately.
        if (not self.auth.authCheck(authToken, admin = True)
            and (user != authToken[0]) or not self.auth.check(authToken)):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], label, user)
        return self.repos.troveStore.keyTable.getUsersMainKeys(user)

    @accessReadOnly
    def listSubkeys(self, authToken, label, fingerprint):
        self.log(2, authToken[0], label, fingerprint)
        # Public function. Don't check auth.
        return self.repos.troveStore.keyTable.getSubkeys(fingerprint)

    @accessReadOnly
    def getOpenPGPKeyUserIds(self, authToken, label, keyId):
        # Public function. Don't check auth.
        return self.repos.troveStore.keyTable.getUserIds(keyId)

    @accessReadOnly
    def getConaryUrl(self, authtoken, clientVersion, revStr, flavorStr):
        return ""

    @accessReadOnly
    def getMirrorMark(self, authToken, clientVersion, host):
        if not self.auth.authCheck(authToken, mirror = True):
            raise errors.InsufficientPermission
        self.log(2, host)
        cu = self.db.cursor()
        cu.execute("select mark from LatestMirror where host=?", host)
        result = cu.fetchall()
        if not result or result[0][0] == None:
            return -1
        return float(result[0][0])

    @accessReadWrite
    def setMirrorMark(self, authToken, clientVersion, host, mark):
        # need to treat the mark as long
        try:
            mark = long(mark)
        except: # deny invalid marks
            raise errors.InsufficientPermission
        # Need both mirror and write permissions.
        if not (self.auth.authCheck(authToken, mirror=True)
                and self.auth.check(authToken, write=True)):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], host, mark)
        cu = self.db.cursor()
        cu.execute("select mark from LatestMirror where host=?", host)
        result = cu.fetchall()
        if not result:
            cu.execute("insert into LatestMirror (host, mark) "
                       "values (?, ?)", (host, mark))
        else:
            cu.execute("update LatestMirror set mark=? where host=?",
                       (mark, host))
        return ""

    @accessReadOnly
    def getNewSigList(self, authToken, clientVersion, mark):
        # only show troves the user is allowed to see
        try:
            mark = long(mark)
        except: # deny invalid marks
            raise errors.InsufficientPermission
        self.log(2, mark)
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []
        # Since signatures are small blobs, it doesn't make a lot
        # of sense to use a LIMIT on this query...
        query = """
        SELECT item, version, flavor, Instances.changed
        FROM Instances
        JOIN TroveInfo USING (instanceId)
        JOIN UserGroupInstancesCache as ugi ON
            Instances.instanceId = ugi.instanceId
        JOIN UserGroups ON
            ugi.userGroupId = UserGroups.userGroupId AND
            UserGroups.canMirror = 1
        JOIN Items ON Instances.itemId = Items.itemId
        JOIN Versions ON Instances.versionId = Versions.versionId
        JOIN Flavors ON Instances.flavorId = flavors.flavorId
        WHERE ugi.userGroupId in (%s)
          AND Instances.changed <= ?
          AND Instances.isPresent = %d
          AND TroveInfo.changed >= ?
          AND TroveInfo.infoType = %d
        ORDER BY TroveInfo.changed
        """ % (",".join("%d" % x for x in roleIds),
               instances.INSTANCE_PRESENT_NORMAL, trove._TROVEINFO_TAG_SIGS)
        # the fewer query parameters passed in, the better PostgreSQL optimizes the query
        # so we embed the constants in the query and bind the user supplied data
        cu.execute(query, (mark, mark))
        l = [ (float(m), (n,v,f)) for n,v,f,m in cu ]
        return list(set(l))

    @accessReadOnly
    def getNewTroveInfo(self, authToken, clientVersion, mark, infoTypes,
                        labels):

        def freezeTroveInfo(returnList, mark, trove, troveInfo):
            if not trove: return
            mark = float(mark)
            if clientVersion <= 64:
                # mask out extended metadata
                returnList.add((mark, trove, troveInfo.freeze(
                                skipSet = troveInfo._newMetadataItems)))
            else:
                returnList.add((mark, trove, troveInfo.freeze()))

        # only show troves the user is allowed to see
        try:
            mark = long(mark)
        except: # deny invalid marks
            raise errors.InsufficientPermission
        self.log(2, mark)
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []
        if infoTypes:
            try:
                infoTypes = [int(x) for x in infoTypes]
            except:
                raise errors.InsufficientPermission
            infoTypeLimiter = ('AND TroveInfo.infoType IN (%s)'
                               %(','.join(str(x) for x in infoTypes)))
        else:
            infoTypeLimiter = ''
        if labels:
            try:
                [ self.toLabel(x) for x in labels ]
            except:
                raise errors.InsufficientPermission
            cu.execute('select labelId from labels where label in (%s)'
                       % ",".join('?'*len(labels)), labels)
            labelIds = [ str(x[0]) for x in cu.fetchall() ]
            if not labelIds:
                # no labels matched, short circuit
                return []
            labelLimit = """
            JOIN Permissions ON
                Permissions.userGroupId = ugi.userGroupId
            JOIN LabelMap ON
                (Permissions.labelId = 0 OR Permissions.labelId = LabelMap.LabelId)
                AND Instances.itemId = LabelMap.itemId
                AND LabelMap.labelId in (%s)
            """ % (','.join(labelIds))
        else:
            labelLimit = ''
        query = """
        SELECT item, version, flavor,
               TroveInfo.infoType, TroveInfo.data, TroveInfo.changed
        FROM Instances
        JOIN TroveInfo USING (instanceId)
        JOIN UserGroupInstancesCache as ugi ON
            Instances.instanceId = ugi.instanceId
        JOIN UserGroups ON
            ugi.userGroupId = UserGroups.userGroupId
            AND UserGroups.canMirror = 1
        %(labelLimit)s
        JOIN Items ON Instances.itemId = Items.itemId
        JOIN Versions ON Instances.versionId = Versions.versionId
        JOIN Flavors ON Instances.flavorId = flavors.flavorId
        WHERE ugi.userGroupId IN (%(roleids)s)
          AND Instances.changed <= ?
          AND Instances.isPresent = %(present)d
          AND TroveInfo.changed >= ?
          %(infoType)s
        ORDER BY Instances.instanceId, TroveInfo.changed
        """ % {
            "roleids" : ",".join("%d" % x for x in roleIds),
            "present" : instances.INSTANCE_PRESENT_NORMAL,
            "infoType" : infoTypeLimiter,
            "labelLimit" : labelLimit,
            }
        cu.execute(query, (mark, mark))

        l = set()
        currentTrove = None
        currentTroveInfo = None
        currentMark = None
        for name, version, flavor, tag, data, tmark in cu:
            t = (name, version, flavor)
            if currentTrove != t:
                freezeTroveInfo(l, currentMark, currentTrove, currentTroveInfo)

                currentTrove = t
                currentTroveInfo = trove.TroveInfo()
                currentMark = tmark
            if tag == -1:
                currentTroveInfo.thaw(cu.frombinary(data))
            else:
                name = currentTroveInfo.streamDict[tag][2]
                currentTroveInfo.__getattribute__(name).thaw(cu.frombinary(data))
            if currentMark is None:
                currentMark = tmark
            currentMark = min(currentMark, tmark)

        freezeTroveInfo(l, currentMark, currentTrove, currentTroveInfo)

        return [ (x[0], x[1], base64.b64encode(x[2])) for x in l ]

    @accessReadWrite
    def setTroveInfo(self, authToken, clientVersion, infoList):
        infoList = [ (x[0], base64.b64decode(x[1])) for x in infoList ]
        return self._setTroveInfo(authToken, clientVersion, infoList,
                                  requireMirror=True)

    def _setTroveInfo(self, authToken, clientVersion, infoList,
                      requireMirror=False):
        # return the number of signatures which have changed
        self.log(2, infoList)
        # this requires mirror access and write access for that trove
        if requireMirror and not self.auth.authCheck(authToken, mirror=True):
            raise errors.InsufficientPermission
        # batch permission check for writing
        cu = self.db.cursor()
        if False in self.auth.batchCheck(authToken, [t for t,s in infoList],
                                         write=True, cu = cu):
            raise errors.InsufficientPermission

        # look up if we have all the troves we're asked
        schema.resetTable(cu, "tmpInstanceId")
        schema.resetTable(cu, "tmpTroveInfo")
        # tmpNVF is already seeded from the batchCheck() call earlier
        cu.execute("""
        insert into tmpInstanceId(idx, instanceId)
        select idx, Instances.instanceId
        from tmpNVF
        join Items on tmpNVF.name = Items.item
        join Versions on tmpNVF.version = Versions.version
        join Flavors on tmpNVF.flavor = Flavors.flavor
        join Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        where Instances.isPresent in (%d, %d)
          and Instances.troveType != %d
        """ % (instances.INSTANCE_PRESENT_NORMAL,
               instances.INSTANCE_PRESENT_HIDDEN,
               trove.TROVE_TYPE_REMOVED),
                   start_transaction=False)
        self.db.analyze("tmpInstanceId")
        # see what troves are missing, if any
        cu.execute("""
        select tmpNVF.idx from tmpNVF
        left join tmpInstanceId on tmpNVF.idx = tmpInstanceId.idx
        where tmpInstanceId.instanceId is NULL
        """)
        ret = cu.fetchall()
        if len(ret): # we'll report the first one
            i = ret[0][0]
            raise errors.TroveMissing(infoList[i][0][0], infoList[i][0][1])

        cu.execute('select instanceId from tmpInstanceId order by idx')
        def _trvInfoIter(instanceIds, iList):
            i = -1
            for (instanceId,), (trvTuple, trvInfo) in itertools.izip(instanceIds, iList):
                for infoType, data in streams.splitFrozenStreamSet(trvInfo):
                    # make sure that only signatures and metadata
                    # are modified
                    if infoType not in (trove._TROVEINFO_TAG_SIGS,
                                        trove._TROVEINFO_TAG_METADATA):
                        continue
                    i += 1
                    yield (i, instanceId, infoType, cu.binary(data))
        updateTroveInfo = list(_trvInfoIter(cu, infoList))
        cu.executemany("insert into tmpTroveInfo (idx, instanceId, infoType, data) "
                       "values (?,?,?,?)", updateTroveInfo, start_transaction=False)

        # first update the existing entries
        cu.execute("""
        select uti.idx
        from tmpTroveInfo as uti
        join TroveInfo on
            TroveInfo.instanceId = uti.instanceId
            and TroveInfo.infoType = uti.infoType
        """)
        rows = cu.fetchall()
        for (idx,) in rows:
            info = updateTroveInfo[idx]
            cu.execute("update troveInfo set data=? where infoType=? and "
                       "instanceId=?", (info[3], info[2], info[1]))
        #first update the existing entries
        # mysql could do it this way
##         cu.execute("""
##         update TroveInfo join TmpTroveInfo as uti on
##             TroveInfo.instanceId = uti.instanceId
##             and TroveInfo.infoType = uti.infoType
##         set troveInfo.data=uti.data
##         """)

        # now insert the rest
        cu.execute("""
        insert into TroveInfo (instanceId, infoType, data)
        select uti.instanceId, uti.infoType, uti.data
        from tmpTroveInfo as uti
        left join TroveInfo on
            TroveInfo.instanceId = uti.instanceId
            and TroveInfo.infoType = uti.infoType
        where troveInfo.instanceId is NULL
        """)

        self.log(3, "updated trove info for", len(updateTroveInfo), "troves")
        return len(updateTroveInfo)

    @accessReadOnly
    def getTroveSigs(self, authToken, clientVersion, infoList):
        self.log(2, infoList)
        # process the results of the more generic call
        ret = self.getTroveInfo(authToken, clientVersion,
                                trove._TROVEINFO_TAG_SIGS, infoList)
        try:
            midx = [x[0] for x in ret].index(-1)
        except ValueError:
            pass
        else:
            raise errors.TroveMissing(infoList[midx][0], infoList[midx][1])
        return [ x[1] for x in ret ]

    @accessReadWrite
    def setTroveSigs(self, authToken, clientVersion, infoList):
        # re-use common setTroveInfo code
        def _transform(l):
            i = trove.TroveInfo()
            for troveTuple, sig in l:
                i.sigs = trove.TroveSignatures(base64.decodestring(sig))
                yield troveTuple, i.freeze()
        return self._setTroveInfo(authToken, clientVersion,
                                  list(_transform(infoList)),
                                  requireMirror=True)

    @accessReadOnly
    def getNewPGPKeys(self, authToken, clientVersion, mark):
        try:
            mark = long(mark)
        except: # deny invalid marks
            raise errors.InsufficientPermission
        if not self.auth.authCheck(authToken, mirror = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], mark)
        cu = self.db.cursor()

        cu.execute("select pgpKey from PGPKeys where changed >= ?", mark)
        return [ base64.encodestring(x[0]) for x in cu ]

    @accessReadWrite
    def addPGPKeyList(self, authToken, clientVersion, keyList):
        # Need both mirror and write permissions.
        if not (self.auth.authCheck(authToken, mirror=True)
                and self.auth.check(authToken, write=True)):
            raise errors.InsufficientPermission

        for encKey in keyList:
            key = base64.decodestring(encKey)
            # this ignores duplicate keys
            self.repos.troveStore.keyTable.addNewKey(None, key)

        return ""

    @accessReadOnly
    def getNewTroveList(self, authToken, clientVersion, mark):
        try:
            mark = long(mark)
        except: # deny invalid marks
            raise errors.InsufficientPermission
        if not self.auth.authCheck(authToken, mirror = True):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], mark)
        # only show troves the user is allowed to see
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []
        # compute the max number of troves with the same mark for
        # dynamic sizing; the client can get stuck if we keep
        # returning the same subset because of a LIMIT too low
        cu.execute("""
        SELECT MAX(c) + 1 AS lim
        FROM (
           SELECT COUNT(instanceId) AS c
           FROM Instances
           WHERE Instances.isPresent = ?
             AND Instances.changed >= ?
           GROUP BY changed
           HAVING COUNT(instanceId) > 1
        ) AS lims""", (instances.INSTANCE_PRESENT_NORMAL, mark))
        lim = cu.fetchall()[0][0]
        if lim is None or lim < 1000:
            lim = 1000 # for safety and efficiency

        # To avoid using a LIMIT value too low on the big query below,
        # we need to find out how many roles might grant access to a
        # trove for this user
        cu.execute("""
        SELECT COUNT(*) AS rolesWithMirrorAccess
        FROM UserGroups
        WHERE UserGroups.canMirror = 1
          AND UserGroups.userGroupId in (%s)
        """ % (",".join("%d" % x for x in roleIds),))
        roleCount = cu.fetchall()[0][0]
        if roleCount == 0:
            raise errors.InsufficientPermission
        if roleCount is None:
            roleCount = 1

        # Next look at the permissions that could grant access
        cu.execute("""
        SELECT COUNT(*) AS perms
        FROM Permissions
        JOIN UserGroups USING(userGroupId)
        WHERE UserGroups.canMirror = 1
          AND UserGroups.userGroupId in (%s)
        """ % (",".join("%d" % x for x in roleIds),))
        permCount = cu.fetchall()[0][0]
        if permCount is None:
            permCount = 1

        # take a guess at the total number of access paths for
        # a trove - the number of roles that have the mirror
        # flag set + the number of permissions contained by those
        # roles
        accessPathCount = roleCount + permCount
        # multiply LIMIT by accessPathCount so that after duplicate
        # elimination we are sure to return at least 'lim' troves
        # back to the client
        query = """
        SELECT DISTINCT
            item, version, flavor,
            Nodes.timeStamps,
            Instances.changed, Instances.troveType
        FROM Instances
        JOIN UserGroupInstancesCache as ugi ON
            Instances.instanceId = ugi.instanceId
        JOIN UserGroups ON
            ugi.userGroupId = UserGroups.userGroupId AND
            UserGroups.canMirror = 1
        JOIN Items ON Items.itemId = Instances.itemId
        JOIN Versions ON Versions.versionId = Instances.versionId
        JOIN Flavors ON Flavors.flavorId = Instances.flavorId
        JOIN Nodes ON
            Instances.itemId = Nodes.itemId AND
            Instances.versionId = Nodes.versionId
            WHERE Instances.changed >= ?
          AND Instances.isPresent = %d
          AND ugi.userGroupId in (%s)
        ORDER BY Instances.changed
        LIMIT %d
        """ % ( instances.INSTANCE_PRESENT_NORMAL,
                ",".join("%d" % x for x in roleIds),
                lim * accessPathCount)
        cu.execute(query, mark)
        self.log(4, "executing query", query, mark)
        ret = []
        for name, version, flavor, timeStamps, mark, troveType in cu:
            version = self.versionStringToFrozen(version, timeStamps)
            ret.append( (float(mark), (name, version, flavor), troveType) )
            if len(ret) >= lim:
                # we need to flush the cursor to stop a backend from complaining
                cu.fetchall()
                break
        # older mirror clients do not support getting the troveType values
        if clientVersion < 40:
            return [ (x[0], x[1]) for x in ret ]
        return ret

    @accessReadOnly
    def getTimestamps(self, authToken, clientVersion, nameVersionList):
        """
        Returns : separated list of timestamps for the versions in
        a list of (name, version) tuples. Note that the flavor is excluded
        here, as the timestamps are necessarily the same for all flavors
        of a (name, version) pair. Timestamps are not considered privledged
        information, so no permission checking is performed. An int value of
        zero is returned for (name, version) paris which are not found in the
        repository.
        """
        self.log(2, nameVersionList)
        cu = self.db.cursor()

        schema.resetTable(cu, "tmpNVF")
        self.db.bulkload("tmpNVF",
                     [ [i,] + tup for i, tup in enumerate(nameVersionList) ],
                     ["idx","name","version" ],
                     start_transaction=False)

        cu.execute("""
            SELECT tmpNVF.idx, Nodes.timeStamps FROM tmpNVF
            JOIN Items ON
                tmpNVF.name = Items.item
            JOIN Versions ON
                tmpNVF.version = Versions.version
            JOIN Nodes ON
                Items.itemId = Nodes.itemId AND
                Versions.versionId = Nodes.versionId
        """)

        results = [ 0 ] * len(nameVersionList)
        for (idx, timeStamps) in cu:
            results[idx] = timeStamps

        return results

    @accessReadOnly
    def getDepsForTroveList(self, authToken, clientVersion, troveList,
                            provides = True, requires = True):
        """
        Returns list of (provides, requires) for troves. For troves which
        are missing or we do not have access to, {} is returned. Empty
        strings are returned for for provides if provides parameter is
        False; same for requires.
        """
        self.log(2, troveList)
        cu = self.db.cursor()

        schema.resetTable(cu, "tmpNVF")
        self.db.bulkload("tmpNVF",
                         [ [i,] + tup for i, tup in enumerate(troveList) ],
                         ["idx","name","version", "flavor"],
                         start_transaction=False)

        req = []
        prov = []
        for tup in troveList:
            req.append({})
            prov.append({})

        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return zip(prov, req)

        tblList = []

        if requires:
            tblList.append( ('Requires', req) )
        else:
            req = [ '' ] * len(troveList)

        if provides:
            tblList.append( ('Provides', prov) )
        else:
            prov = [ '' ] * len(troveList)

        for tableName, dsList in tblList:
            cu.execute("""
                SELECT tmpNVF.idx, D.class, D.name, D.flag FROM tmpNVF
                    JOIN Items ON
                        Items.item = tmpNVF.name
                    JOIN Versions ON
                        Versions.version = tmpNVF.version
                    JOIN Flavors ON
                        Flavors.flavor = tmpNVF.flavor
                    JOIN Instances ON
                        Items.itemId = Instances.itemId AND
                        Versions.versionId = Instances.versionId AND
                        Flavors.flavorId = Instances.flavorId
                    JOIN UserGroupInstancesCache AS ugi
                        USING (instanceId)
                    JOIN %s USING (InstanceId)
                    JOIN Dependencies AS D USING (depId)
                WHERE
                    ugi.userGroupId in (%s)
            """ %  (tableName, ",".join("%d" % x for x in roleIds) ))

            l = [ x for x in cu ]
            last = None
            flags = []
            for idx, depClassId, depName, depFlag in l:
                this = (idx, depClassId, depName)

                if this != last:
                    if last:
                        dsList[last[0]].setdefault((last[1], last[2]), []).extend(flags)

                    last = this
                    flags = []

                if depFlag != deptable.NO_FLAG_MAGIC:
                    flags.append((depFlag, deps.FLAG_SENSE_REQUIRED))

            if last:
                dsList[last[0]].setdefault((last[1], last[2]), []).extend(flags)
            flagMap = [ None, '', '~', '~!', '!' ]
            for i, itemList in enumerate(dsList):
                depList = itemList.items()
                depList.sort()

                if not depList:
                    frz = ''
                else:
                    l = []
                    for (depClassId, depName), depFlags in depList:
                        l += [ '|', str(depClassId), '#', depName.replace(':', '::') ]
                        lastFlag = None
                        for flag in sorted(depFlags):
                            if flag == lastFlag:
                                continue

                            l.append(':')
                            l.append(flagMap[flag[1]])
                            l.append(flag[0].replace(':', '::'))

                    frz = ''.join(l[1:])

                dsList[i] = frz

        result = zip(prov, req)
        return result

    @accessReadOnly
    def getTroveInfo(self, authToken, clientVersion, infoType, troveList):
        """
        we return tuples (present, data) to aid netclient in making its decoding decisions
        present values are:
        -2 = insufficient permission
        -1 = trovemissing
        0  = valuemissing
        1 = valueattached
        """
        # infoType should be valid
        if infoType not in trove.TroveInfo.streamDict.keys():
            raise errors.RepositoryError("Unknown trove infoType requested", infoType)

        self.log(2, infoType, troveList)

        # by default we should mark all troves with insuficient permission
        ## disabled for now until we deal with protocol compatibility issues
        ## for insufficient permission
        ##ret = [ (-2, '') ] * len(troveList)
        ret = [ (-1, '') ] * len(troveList)
        # check permissions using the batch interface
        cu = self.db.cursor()
        permList = self.auth.batchCheck(authToken, troveList, cu = cu)
        if True not in permList:
            # we got no permissions, shortcircuit all of them as missing
            return ret
        if False in permList:
            # drop troves for which we have no permissions to avoid busy work
            cu.execute("delete from tmpNVF where idx in (%s)" % (",".join(
                "%d" % i for i,perm in enumerate(permList) if not perm)))
            self.db.analyze("tmpNVF")
        # get the data doing a full scan of tmpNVF
        cu.execute("""
        SELECT tmpNVF.idx, TroveInfo.data
        FROM tmpNVF
        JOIN Items ON tmpNVF.name = Items.item
        JOIN Versions ON tmpNVF.version = Versions.version
        JOIN Flavors ON tmpNVF.flavor = Flavors.flavor
        JOIN Instances ON
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        LEFT JOIN TroveInfo ON
            Instances.instanceId = TroveInfo.instanceId
            AND TroveInfo.infoType = ?
        """, infoType)
        for i, data in cu:
            data = cu.frombinary(data)
            if data is None:
                ret[i] = (0, '') # value missing
                continue

            # else, we have a value we need to return
            if (infoType == trove._TROVEINFO_TAG_METADATA and
                clientVersion <= 64):
                # ugly, but just instantiates a metadata object
                md = trove.TroveInfo.streamDict[
                                 trove._TROVEINFO_TAG_METADATA][1]()

                md.thaw(data)
                data = md.freeze(skipSet = trove.TroveInfo._newMetadataItems)

            ret[i] = (1, base64.encodestring(data))
        return ret

    @accessReadOnly
    def getTroveReferences(self, authToken, clientVersion, troveInfoList):
        """
        troveInfoList is a list of (name, version, flavor) tuples. For
        each (name, version, flavor) specied, return a list of the troves
        (groups and packages) which reference it (either strong or weak)
        """
        # design decision: the user must have permission to see the
        # referencing trove, but not the trove being referenced.
        if not self.auth.check(authToken):
            raise errors.InsufficientPermission
        self.log(2, troveInfoList)
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpNVF")
        schema.resetTable(cu, "tmpInstanceId")
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []
        for (n,v,f) in troveInfoList:
            cu.execute("insert into tmpNVF(name,version,flavor) values (?,?,?)",
                       (n, v, f), start_transaction=False)
        self.db.analyze("tmpNVF")
        # we'll need the min idx to account for differences in SQL backends
        cu.execute("SELECT MIN(idx) from tmpNVF")
        minIdx = cu.fetchone()[0]
        # get the instanceIds of the parents of what we can find
        cu.execute("""
        insert into tmpInstanceId(idx, instanceId)
        select tmpNVF.idx, TroveTroves.instanceId
        from tmpNVF
        join Items on tmpNVF.name = Items.item
        join Versions on tmpNVF.version = Versions.version
        join Flavors on tmpNVF.flavor = Flavors.flavor
        join Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        join TroveTroves on TroveTroves.includedId = Instances.instanceId
        """, start_transaction=False)
        self.db.analyze("tmpInstanceId")
        # tmpInstanceId now has instanceIds of the parents. retrieve the data we need
        cu.execute("""
        select
            tmpInstanceId.idx, Items.item, Versions.version, Flavors.flavor
        from tmpInstanceId
        join Instances on tmpInstanceId.instanceId = Instances.instanceId
        join UserGroupInstancesCache as ugi ON
            Instances.instanceId = ugi.instanceId
        join Items on Instances.itemId = Items.itemId
        join Versions on Instances.versionId = Versions.versionId
        join Flavors on Instances.flavorId = Flavors.flavorId
        where ugi.userGroupId in (%s)
        """ % (",".join("%d" % x for x in roleIds), ))
        # get the results
        ret = [ set() for x in range(len(troveInfoList)) ]
        for i, n,v,f in cu:
            s = ret[i-minIdx]
            s.add((n,v,f))
        ret = [ list(x) for x in ret ]
        return ret

    @accessReadOnly
    def getLabelsForHost(self, authToken, clientVersion, serverName):
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []
        cu.execute('''
             SELECT branch FROM
                (SELECT DISTINCT branchId
                    FROM LatestCache
                    WHERE userGroupId IN (%s)
                    AND latestType=1) AS AvailBranches
             JOIN Branches USING(branchId)'''
                % (", ".join("%d" % x for x in roleIds),))
        # NOTE: this is faster than joining against the LabelMap,
        # as there is no direct branchId -> labelId mapping (you
        # must also use the itemId.)  The python done below is
        # going to be faster than SQL until we add such an table.
        # And in general neither number of labels or number of branches is
        # likely to go beyond thousands.
        labelList = (versions.VersionFromString(x[0]).label() for x in cu)
        labelList = (str(x) for x in labelList if x.getHost() == serverName)
        # dedup.
        return list(set(labelList))

    @accessReadOnly
    def getTroveDescendants(self, authToken, clientVersion, troveList):
        """
        troveList is a list of (name, branch, flavor) tuples. For each
        item, return the full version and flavor of each trove named
        name which exists on a downstream branch from the branch
        passed in and is of the specified flavor. If the flavor is not
        specified, all matches should be returned. Only troves the
        user has permission to view should be returned.
        """
        if not self.auth.check(authToken):
            raise errors.InsufficientPermission
        self.log(2, troveList)
        cu = self.db.cursor()
        roleIds = self.auth.getAuthRoles(cu, authToken)
        if not roleIds:
            return []
        ret = [ [] for x in range(len(troveList)) ]
        d = {"roleids" : ",".join(["%d" % x for x in roleIds])}
        for i, (n, branch, f) in enumerate(troveList):
            assert ( branch.startswith('/') )
            args = [n, '%s/%%' % (branch,)]
            d["flavor"] = ""
            if f is not None:
                d["flavor"] = "and Flavors.flavor = ?"
                args.append(f)
            cu.execute("""
            select Versions.version, Flavors.flavor
            from Items
            join Nodes on Items.itemId = Nodes.itemId
            join Instances on
                Nodes.versionId = Instances.versionId and
                Nodes.itemId = Instances.itemId
            join UserGroupInstancesCache as ugi on
                Instances.instanceId = ugi.instanceId
            join Branches on Nodes.branchId = Branches.branchId
            join Versions on Nodes.versionId = Versions.versionId
            join Flavors on Instances.flavorId = Flavors.flavorId
            where Items.item = ?
              and Branches.branch like ?
              and ugi.userGroupId in (%(roleids)s)
              %(flavor)s
            """ % d, args)
            for verStr, flavStr in cu:
                ret[i].append((verStr,flavStr))
        return ret

    @accessReadOnly
    def checkVersion(self, authToken, clientVersion):
        """
        Check the repository's protocol version to see that it's compatible with
        the client

        @raises errors.InvalidClientVersion: raised if the client is either too
                                             old or too new.
        @raises errors.InsufficientPermission: raised if the authToken is
                                               invalid
        """
        if clientVersion > 50:
            raise errors.InvalidClientVersion(
                    'checkVersion call only supports protocol versions 50 '
                    'and lower')

        if not self.auth.check(authToken):
            raise errors.InsufficientPermission
        self.log(2, authToken[0], "clientVersion=%s" % clientVersion)
        # cut off older clients entirely, no negotiation
        if clientVersion < SERVER_VERSIONS[0]:
            raise errors.InvalidClientVersion(
               'Invalid client version %s.  Server accepts client versions %s '
               '- read http://wiki.rpath.com/wiki/Conary:Conversion' %
               (clientVersion, ', '.join(str(x) for x in SERVER_VERSIONS)))
        return SERVER_VERSIONS

# this has to be at the end to get the publicCalls list correct; the proxy
# uses the publicCalls list, so maintaining it
NetworkRepositoryServer.publicCalls = set()
for attr, val in NetworkRepositoryServer.__dict__.iteritems():
    if type(val) == types.FunctionType:
        if hasattr(val, '_accessType'):
            NetworkRepositoryServer.publicCalls.add(attr)


class ManifestWriter(object):

    def __init__(self, tmpDir, resumeOffset=None):
        self.fobj = tempfile.NamedTemporaryFile(dir=tmpDir, suffix='.cf-out')
        if resumeOffset:
            self.fobj.write('resumeOffset=%d\n' % resumeOffset)

    def append(self, path, expandedSize, isChangeset, preserveFile, offset):
        print >> self.fobj, "%s %d %d %d %d" % (path, expandedSize,
                isChangeset, preserveFile, offset)

    def close(self):
        name = os.path.basename(self.fobj.name)[:-4]
        self.fobj.delete = False
        self.fobj.close()
        return name


class HiddenException(Exception):

    def __init__(self, forLog, forReturn):
        self.forLog = forLog
        self.forReturn = forReturn


class ServerConfig(ConfigFile):
    authCacheTimeout        = CfgInt
    baseUri                 = (CfgString, None)
    bugsToEmail             = CfgString
    bugsFromEmail           = CfgString
    bugsEmailName           = (CfgString, 'Conary Repository')
    bugsEmailSubject        = (CfgString, 'Conary Repository Error Message')
    memCache                = CfgString
    memCacheUserAuth        = (CfgBool, True)
    memCacheTimeout         = (CfgInt, -1)
    memCachePrefix          = CfgString
    changesetCacheDir       = CfgPath
    changesetCacheLogFile   = CfgPath
    commitAction            = CfgString
    contentsDir             = CfgContentStore
    deadlockRetry           = (CfgInt, 5)
    entitlement             = CfgEntitlement
    entitlementCheckURL     = CfgString
    externalPasswordURL     = CfgString
    forceSSL                = CfgBool
    geoIpFiles              = CfgList(CfgPath)
    logFile                 = CfgPath
    proxy                   = (CfgProxy, None)
    conaryProxy             = (CfgProxy, None)
    proxyMap                =  CfgProxyMap
    paranoidCommits         = (CfgBool, False)
    proxyContentsDir        = CfgPath
    readOnlyRepository      = CfgBool
    repositoryDB            = dbstore.CfgDriver
    repositoryMap           = CfgRepoMap
    requireSigs             = CfgBool
    serverName              = CfgLineList(CfgString, listType = GlobListType)
    staticPath              = (CfgString, '/conary-static')
    serializeCommits        = (CfgBool, False)
    tmpDir                  = (CfgPath, '/var/tmp')
    traceLog                = tracelog.CfgTraceLog
    user                    = CfgUserInfo
    webEnabled              = (CfgBool, True)

    # DEPRECATED
    capsuleServerUrl        = (CfgString, None)
    excludeCapsuleContents  = (CfgBool, False)
    injectCapsuleContentServers = CfgList(CfgString)

    def getProxyMap(self):
        return getProxyMap(self)
