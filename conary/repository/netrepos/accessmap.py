#
# Copyright (c) 2007 rPath, Inc.
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

from conary.dbstore import sqlerrors
from conary.server import schema
from conary.repository import errors
from conary.repository.netrepos import instances

# class and methods for handling UserGroupTroves operations
class UserGroupTroves:
    def __init__(self, db):
        self.db = db

    # given a list of (n,v,f) tuples, convert them to instanceIds in the tmpInstances table
    def _findInstanceIds(self, troveList, checkMissing=True):
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpNVF")
        schema.resetTable(cu, "tmpInstanceId")
        for (n,v,f) in troveList:
            cu.execute("insert into tmpNVF (name, version, flavor) "
                       "values (?,?,?)", (n,v,f))
        self.db.analyze("tmpNVF")
        cu.execute("""
        insert into tmpInstanceId (idx, instanceId)
        select tmpNVF.idx, Instances.instanceId
        from tmpNVF
        join Items on tmpNVF.name = Items.item
        join Versions on tmpNVF.version = Versions.version
        join Flavors on tmpNVF.flavor = Flavors.flavor
        join Instances on
            Instances.itemId = Items.itemId and
            Instances.versionId = Versions.versionId and
            Instances.flavorId = Flavors.flavorId
        where
            Instances.isPresent in (%d,%d)
        """ % (instances.INSTANCE_PRESENT_NORMAL, instances.INSTANCE_PRESENT_HIDDEN))
        self.db.analyze("tmpInstances")
        # check if any troves specified are missing
        cu.execute("""
        select tmpNVF.idx, name, version, flavor
        from tmpNVF
        left join tmpInstanceId using(idx)
        where tmpInstanceId.instanceId is NULL
        """)
        if checkMissing:
            for i, n, v, v in cu.fetchall():
                raise errors.TroveMissing(n,v)
        return True

    def update(self, userGroupId):
        """updates the UserGroupInstancesCache table with permissions granted
        by the UserGroupTroves table. It is assumed that UserGroupInstancesCache
        tables has been previously sanitized for the userGroupId passed in
        """
        # extract the list of instanceIds we might need to recurse
        # through to grant access
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpInstanceId")
        cu.execute("select instanceId, recursive "
                   "from UserGroupTroves where userGroupId = ?",
                   userGroupId)
        for instanceId, recursive in cu.fetchall():
            cu.execute("insert into tmpInstanceId(instanceId) values (?)",
                       instanceId)
            if recursive:
                # recurse through the new instanceIds that have been granted access
                cu.execute("insert into tmpInstanceId(instanceId) "
                           "select includedId from TroveTroves "
                           "where instanceId = ?", instanceId)
        self.db.analyze("tmpInstanceId")
        # check for which instanceIds we don't have access already
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select distinct tmpInstanceId.instanceId
        from tmpInstanceId
        left outer join UserGroupInstancesCache as ugi on
            tmpInstanceId.instanceId = ugi.instanceId and
            ugi.userGroupId = ?
        where ugi.instanceId is NULL""", userGroupId)
        cu.execute("insert into UserGroupInstancesCache (userGroupId, instanceId) "
                   "select %d, instanceId from tmpInstances" %(userGroupId,))
        return True
        
    # grant access on a troveList to userGroup
    def add(self, userGroupId, troveList, recursive=True):
        """grant access on a troveList to a userGroup. If recursive = True,
        then access is also granted to all the children of the troves passed
        """
        self._findInstanceIds(troveList)
        recursive = int(bool(recursive))
        cu = self.db.cursor()
        # grab the list of instanceIds we need to add to the UserGroupTroves table
        cu.execute("""
        select distinct tmpInstanceId.instanceId from tmpInstanceId
        left outer join UserGroupTroves as ugt on
            (tmpInstanceId.instanceId = ugt.instanceId and
             ugt.userGroupId = ?)
        where
            ugt.instanceId is NULL
        """, userGroupId)
        # record the new permissions
        for instanceId, in cu.fetchall():
            cu.execute("insert into UserGroupTroves(userGroupId, instanceId, recursive) "
                       "values (?,?,?)", (userGroupId, instanceId, recursive))
        return True

    # remove trove access grants
    def delete(self, userGroupId, troveList):
        """remove group access to troves passed in the (n,v,f) troveList"""
        self._findInstanceIds(troveList, checkMissing=False)
        cu = self.db.cursor()
        cu.execute("""
        delete from UserGroupTroves
        where userGroupId = ?
          and instanceId in (select instanceId from tmpInstanceId)
        """, userGroupId)
        return True

    # list what we have in the repository for a userGroupId
    def list(self, userGroupId):
        """return a list of the troves this usergroup is granted special access"""
        cu = self.db.cursor()
        cu.execute("""
        select Items.item, Versions.version, Flavors.flavor, ugt.recursive
        from UserGroupTroves as ugt
        join Instances using(instanceId)
        join Items on Instances.itemId = Items.itemId
        join Versions on Instances.versionId = Versions.versionId
        join Flavors on Instances.flavorId = Flavors.flavorId
        where ugt.userGroupId = ? """, userGroupId)
        return [ ((n,v,f),r) for n,v,f,r in cu.fetchall()]

    def rebuild(self):
        """ updated the access cache for all the usergroups that have
        special accessmaps. The UserGroupInstancesCache table should
        be scrubbed before calling this """
        cu = self.db.cursor()
        cu.execute("select distinct userGroupId from UserGroupTroves")
        # this is actually the fastest way to regenerate all the
        # entries, because the individual steps are much reduced in
        # complexity and simpler to execute for the database backend.
        for userGroupId, in cu.fetchall():
            self.update(userGroupId)
        
# class and methods for handling UserGroupInstancesCache operations
class UserGroupInstances:
    def __init__(self, db):
        self.db = db

    def update(self, cu, instanceId = None, userGroupId = None):
        """rebuilds the UserGroupInstancesCache. If both instanceId
        and userGroupId are None, it will rebuild the entire table;
        otherwise the rebuilding scope is limited
        """
        where = []
        args = []
        if instanceId is not None:
            where.append("Instances.instanceId = ?")
            args.append(instanceId)
        if userGroupId is not None:
            where.append("Permissions.userGroupId = ?")
            args.append(userGroupId)
        whereStr = ""
        if len(where):
            whereStr = "where %s" % (' and '.join(where),)
        cu.execute("""
        insert into UserGroupInstancesCache (instanceId, userGroupId, canWrite, canRemove)
        select
            Instances.instanceId as instanceId,
            Permissions.userGroupId as userGroupId,
            case when sum(Permissions.canWrite) = 0 then 0 else 1 end as canWrite,
            case when sum(Permissions.canRemove) = 0 then 0 else 1 end as canRemove
        from Instances
        join Nodes using(itemId, versionId)
        join LabelMap using(itemId, branchId)
        join Permissions on
            Permissions.labelId = 0 or
            Permissions.labelId = LabelMap.labelId
        join CheckTroveCache on
            Permissions.itemId = CheckTroveCache.patternId and
            Instances.itemId = CheckTroveCache.itemId
        %s
        group by Instances.instanceId, Permissions.userGroupId
        """ % (whereStr,), args)

    def updateInstanceId(self, instanceId):
        """update UserGroupInstancesCache for a changed instanceId"""
        cu = self.db.cursor()
        cu.execute("delete from UserGroupInstancesCache where instanceId = ?",
                   instanceId)
        self.update(cu, instanceId=instanceId)

    def updateUserGroupId(self, userGroupId):
        """update UserGroupInstancesCache for acl changes for a userGroupId"""
        cu = self.db.cursor()
        cu.execute("delete from UserGroupInstancesCache where userGroupId = ?",
                   userGroupId)
        self.update(cu, userGroupId=userGroupId)

    def rebuild(self):
        """ rebuild the entire UserGroupInstancesCache  """
        cu = self.db.cursor()
        cu.execute("delete from UserGroupInstancesCache")
        self.update(cu)
        self.db.analyze("UserGroupInstancesCache")
        
# class and methods for handling UserGroupInstancesCache operations
class UserGroupLatest:
    def __init__(self, db):
        self.db = db
    def rebuild(self):
        raise NotImplementedError
    def updateUserGroupId(self, userGroupId):
        pass

# generic wrapper operations that handle updating and syncing all the
# relevant usergroup access maps
class UserGroupOps:
    def __init__(self, db):
        self.db = db
        self.ugt = UserGroupTroves(db)
        self.ugi = UserGroupInstances(db)
        self.ugl = UserGroupLatest(db)

    def _getGroupId(self, userGroup):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
                   userGroup)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
        raise errors.GroupNotFound

    def addTroveAccess(self, userGroup, troveList, recursive=True):
        userGroupId = self._getGroupId(userGroup)
        self.ugt.add(userGroupId, troveList, recursive)
        # add is simpler because we can only add the new stuff to UserGroupInstances
        self.ugt.update(userGroupId)
        # need to recompute the latest stuff for this userGroupId
        self.ugl.updateUserGroupId(userGroupId)

    def deleteTroveAccess(self, userGroup, troveList):
        userGroupId = self._getGroupId(userGroup)
        self.ugt.delete(userGroupId, troveList)
        # add extra troves allowed by UserGroupTroves
        self.ugt.update(userGroupId)
        # in the remove access case, we need to recompute the entire
        # UserGroupInstances and UserGroupLatest for this userGroupId
        # rebuild the UserGroupInstances
        self.ugi.updateUserGroupId(userGroupId)
        # and recompute the Latest entries for this user
        self.ugl.updateUserGroupId(userGroupId)

    def listTroveAccess(self, userGroup):
        userGroupId = self._getGroupId(userGroup)
        return self.ugt.list(userGroupId)

    # rebuild the cache tables completely for a userGroup
    def updateUserGroupId(self, userGroupId):
        self.ugi.updateUserGroupId(userGroupId)
        self.ugt.update(userGroupId)
        self.ugl.updateUserGroupId(userGroupId)
    def updateUserGroup(self, userGroup):
        userGroupId = self._getGroupId(userGroup)
        self.updateUserGroupId(userGroupId)
        
    # rebuild all caches
    def rebuild(self):
        self.ugi.rebuild()
        self.ugt.rebuild()
        self.ugl.rebuild()
        
