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

from conary.server import schema
from conary.repository import errors
from conary.repository.netrepos import instances, versionops
from conary.lib.tracelog import logMe

# - Entries in the UserGroupTroves table are processed and flattened
#   into the UserGroupAllTroves table
# - Entries in the Permissions table are processed and flattened into
#   the UserGroupAllPermissions table
# - Then, UserGroupAllTroves and UserGroupAllPermissions are summarized
#   in the UserGroupInstancesCache table

# class and methods for handling UserGroupTroves operations
class UserGroupTroves:
    def __init__(self, db):
        self.db = db

    # given a list of (n,v,f) tuples, convert them to instanceIds in
    # the tmpInstanceId table
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

    # update the UserGroupAllTroves table for a new ugtId
    def _updateAllTroves(self, cu, ugtId = None, userGroupId = None):
        where = []
        args = {}
        if ugtId is not None:
            where.append("ugtId = :ugtId")
            args["ugtId"] = ugtId
        if userGroupId is not None:
            where.append("userGroupId = :userGroupId")
            args["userGroupId"] = userGroupId
        whereCond = ""
        andCond = ""
        if where:
            whereCond = "where " + " and ".join(where)
            andCond   = "and "   + " and ".join(where)
        # update the UserGroupAllTroves table
        cu.execute("delete from UserGroupAllTroves %s" % (whereCond,), args)
        cu.execute("""
        insert into UserGroupAllTroves (ugtId, instanceId)
        select ugtId, instanceId from UserGroupTroves %s
        union
        select ugtId, TroveTroves.includedId
        from UserGroupTroves as ugt
        join TroveTroves using (instanceId)
        where ugt.recursive = 1 %s
        """ %(whereCond, andCond), args)
        return True
    
    # grant access on a troveList to userGroup
    def add(self, userGroupId, troveList, recursive=True):
        """grant access on a troveList to a userGroup. If recursive = True,
        then access is also granted to all the children of the troves passed
        """
        self._findInstanceIds(troveList)
        recursive = int(bool(recursive))
        # we have now the list of instanceIds in the tmpInstanceId table.
        # avoid inserting duplicates
        cu = self.db.cursor()
        cu.execute("""
        select distinct tmpInstanceId.instanceId, ugt.ugtId, ugt.recursive
        from tmpInstanceId
        left join UserGroupTroves as ugt using(instanceId)
        where ugt.userGroupId is NULL or ugt.userGroupId = ?
        """, userGroupId)
        # record the new permissions
        ugtList = []
        for instanceId, ugtId, recflag in cu.fetchall():
            if ugtId is None: # new instanceId, left join returned a NULL ugt record
                cu.execute("insert into UserGroupTroves(userGroupId, instanceId, recursive) "
                           "values (?,?,?)", (userGroupId, instanceId, recursive))
                ugtId = cu.lastrowid
            elif recursive and not recflag:
                # granting recursive access to something that wasn't recursive before
                cu.execute("update UserGroupTroves set recursive = ? where ugtId = ?",
                           (recursive, ugtId))
            else: # not worth bothering with a rebuild
                ugtId = None
            if ugtId: # we have a new (or changed) acl
                self._updateAllTroves(cu, ugtId, userGroupId)
                ugtList.append(ugtId)
        return ugtList

    # remove trove access grants
    def delete(self, userGroupId, troveList):
        """remove group access to troves passed in the (n,v,f) troveList"""
        self._findInstanceIds(troveList, checkMissing=False)
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpId")
        cu.execute("""
        insert into tmpId(id)
        select ugtId from UserGroupTroves
        where userGroupId = ?
        and instanceId in (select instanceId from tmpInstanceId)
        """, userGroupId, start_transaction=False)
        self.db.analyze("tmpId")
        # save what instanceIds will be affected by this delete
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select distinct ugat.instanceId
        from UserGroupAllInstances as ugat
        join tmpInstances using(instanceId)
        where ugat.userGroupId = ?
          and ugat.ugtId in (select id from tmpId)
        """, userGroupId, start_transaction=False)
        # filter out the ones that are still allowed based on other permissions
        cu.execute("""
        delete from tmpInstances
        where exists (
            select 1 from UserGroupAllTroves as ugat
            where userGroupId = ?
              and ugat.instanceId = tmpInstances.instanceId )
        """, userGroupId, start_transaction=False)
        cu.execute("delete from UserGroupAllTroves where ugtId in (select id from tmpId)")
        cu.execute("delete from UserGroupTroves where ugtId in (select id from tmpId)")
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

    # Assumes that UserGroupInstancesCache has been sanitized before calling in
    def _updateInstancesCache(self, cu, userGroupId):
        """updates the UserGroupInstancesCache table with permissions granted
        by the UserGroupTroves table. """
        # we have the troves we are granted access to in the UserGroupAllTroves
        # we need to add the new stuff into the UserGroupInstancesCache table
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select distinct instanceId from UserGroupAllTroves as ugat
        where not exists
        ( select 1 from UserGroupInstancesCache as ugi
          where ugi.instanceId = ugat.instanceId
            and ugi.userGroupId = ugat.userGroupId )
        and ugat.userGroupId = ? """, userGroupId)
        cu.execute("insert into UserGroupInstancesCache(userGroupId, instanceId) "
                   "select %d, instanceId from tmpInstances" % (userGroupId,))
        return True
        
    def update(self, userGroupId):
        cu = self.db.cursor()
        ret = self._updateInstancesCache(cu, userGroupId)
        return ret
    
    # the UserGroupInstancesCache table should be scrubbed before calling this
    def rebuild(self, userGroupId = None):
        """ updates the access cache for all the usergroups that have
        special accessmaps. """
        cu = self.db.cursor()
        # first, rebuild the UserGroupAllTroves table
        self._updateAllTroves(cu, userGroupId = userGroupId)
        if userGroupId is not None:
            self._updateInstancesCache(cu, userGroupId)
            return
        cu.execute("select distinct userGroupId from UserGroupTroves")
        # this is actually the fastest way to regenerate all the
        # entries, because the individual steps are much reduced in
        # complexity and simpler to execute for the database backend.
        for userGroupId, in cu.fetchall():
            self._updateInstancesCache(cu, userGroupId)
        
# class and methods for handling UserGroupAllPermissions operations
class UserGroupPermissions:
    def __init__(self, db):
        self.db = db

    def addId(self, cu, permissionId = None, instanceId = None):
        # adds into the UserGroupAllPermissions table new entries
        # triggered by one or more recordIds
        where = []
        args = []
        if permissionId is not None:
            where.append("Permissions.permissionId = ?")
            args.append(permissionId)
        if instanceId is not None:
            where.append("Instances.instanceId = ?")
            args.append(instanceId)
        whereStr = ""
        if len(where):
            whereStr = "where %s" % (' and '.join(where),)
        cu.execute("""
        insert into UserGroupAllPermissions
            (permissionId, userGroupId, instanceId, canWrite, canRemove)
        select
            Permissions.permissionId as permissionId,
            Permissions.userGroupId as userGroupId,
            Instances.instanceId as instanceId,
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
        %s """ % (whereStr,), args)
        return True

    def deleteId(self, cu, permissionId = None, instanceId = None,
                 userGroupId = None):
        if permissionId is not None:
            where.append("permissionId = ?")
            args.append(permissionId)
        if instanceId is not None:
            where.append("instanceId = ?")
            args.append(instanceId)
        if userGroupId is not None:
            where.append("userGroupId = ?")
            args.append(userGroupId)
        whereStr = ""
        if len(where):
            whereStr = "where %s" % (' and '.join(where),)
        cu.execute("delete from UserGroupAllPermissions %s" % (whereStr,), args)
        return True
    

# this class takes care of the UserGroupInstancesCache table, which is a summary
# of rows present in UserGroupAllTroves and UserGroupAllPermissions tables
class UserGroupInstances:
    def __init__(self, db):
        self.db = db
        self.ugt = UserGroupTroves(db)
        self.ugp = UserGroupPermissions(db)
        
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
        ugtList = self.ugt.add(userGroupId, troveList, recursive)
        # we now know the ids of the new acls added. They're useful in
        # updating the UGIC table
        cu = self.db.cursor()
        # grab the list of instanceIds we are adding to the UGIC table;
        # we need those for a faster recomputation of the LatestCache table
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select distinct ugat.instanceId
        from UserGroupAllTroves as ugat
        where ugat.ugtId in (%s)
          and ugat.userGroupId = ?
          and not exists (
              select 1 from UserGroupInstancesCache as ugi
              where ugi.userGroupId = ?
                and ugi.instanceId = ugat.instanceId )
        """ % (",".join("%d" % x for x in ugtList),),
                   (userGroupId, userGroupId), start_transaction=False)
        # insert into UGIC and recompute the latest table
        cu.execute("""
        insert into UserGroupInstancesTable (userGroupId, instanceId)
        select %d, instanceId from tmpInstances """ %(userGroupId,))
        # tmpInstances has instanceIds for which Latest needs to be recomputed
        self.db.analyze("tmpInstances")
        self.latest.updateUserGroupId(userGroupId, tmpInstances=True)

    def deleteTroveAccess(self, userGroup, troveList):
        userGroupId = self._getGroupId(userGroup)
        # remove the UserGroupTrove access
        cu = self.db.cursor()
        self.ugt.delete(userGroupId, troveList)
        # instanceIds that were removed from UGAT are in tmpInstances now
        # UGAP might still grant permissions to some, so we filter those out
        cu.execute("""
        delete from tmpInstances
        where exists (
            select 1 from UserGroupAllPermissions as ugap
            where ugap.userGroupId = ?
              and ugap.instanceId = tmpInstances.instanceId )
        """, userGroupId)
        self.db.analyze("tmpInstances")
        # now we should have in tmpInstances the instanceIds of the
        # troves this user can no longer access.
        cu.execute("""
        delete from UserGroupInstancesCache
        where userGroupId = ?
          and instanceId in (select instanceId from tmpInstances)
        """, userGroupId)
        # tmpInstances has instanceIds for which Latest needs to be recomputed
        self.latest.updateUserGroupId(userGroupId, tmpInstances=True)

    def listTroveAccess(self, userGroup):
        userGroupId = self._getGroupId(userGroup)
        return self.ugt.list(userGroupId)


    # changes in the Permissions table
    def addPermissionId(self, permissionId, userGroupId):
        cu = self.db.cursor()
        self.ugp.addId(cu, permissionId = permissionId)
        # figure out newly accessible troves. We keep track separately
        # to speed up the Latest update
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select instanceId from UserGroupAllPermissions as ugap
        where permissionId = ?
          and not exists (
              select instanceId from UserGroupInstancesCache as ugi
              where ugi.userGroupId = ?
              and ugi.instanceId = ugap.instanceId ) """.
                   (permissionId, userGroupId))
        # update UsergroupInstancesCache
        cu.execute("""
        insert into UserGroupInstancesCache
              (userGroupId, instanceId, canWrite, canRemove)
        select userGroupId, instanceId,
               case when sum(canWrite) = 0 then 0 else 1 end as canWrite,
               case when sum(canRemove) = 0 then 0 else 1 end as canRemove
        from UserGroupAllPermissions
        where permissionId = ?
          and instanceId in (select instanceId from tmpInstances)
        """, permissionId)
        # update Latest
        self.latest.updateUserGroupId(userGroupId, tmpInstances=True)

    def updatePermissionId(self, permissionId, userGroupId):
        cu = self.db.cursor()
        pass
    
    def deletePermissionId(self, permissionId, userGroupId):
        cu = self.db.cursor()
        # compute the list of troves for which no other UGAP/UGAT access exists
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select instanceId from UserGroupInstancesCache as ugi
        where ugi.userGroupId = ?
          and not exists (
              select 1 from UserGroupAllPermissions as ugap
              where ugap.userGroupId = ? 
                and ugap.instanceId = ugi.instanceId
                and ugap.permissionId != ?
              union all
              select 2 from UserGroupAllTroves as ugat
              where ugat.instanceId = ugi.instanceId
                and ugat.userGroupId = ? )""",
                   (userGroupId, userGroupId, permissionId, userGroupId))
        # clean up the flattened table
        cu.execute("delete from UserGroupAllPermissions where permissionId = ?",
                   permissionId)
        # now we have only the troves which need to be erased out of UGIC
        self.db.analyze(cu, "tmpInstances")
        cu.execute("""
        delete from UserGroupInstancesCache
        where userGroupId = ?
        and instanceId in (select instanceId from tmpInstances)""", userGroupId)
        # update Latest
        self.latest.updateUserGroupId(userGroupId, tmpInstances=True)


    def update(self, cu, instanceId = None, userGroupId = None):
        """rebuilds the UserGroupInstancesCache. If both instanceId
        and userGroupId are None, it will rebuild the entire table;
        otherwise the rebuilding scope is limited
        """
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
        logMe(3, "deleted old stuff", userGroupId)
        self.update(cu, userGroupId=userGroupId)

    def rebuild(self):
        """ rebuild the entire UserGroupInstancesCache  """
        cu = self.db.cursor()
        cu.execute("delete from UserGroupInstancesCache")
        self.update(cu)
        self.db.analyze("UserGroupInstancesCache")
        
# generic wrapper operations that handle updating and syncing all the
# relevant usergroup access maps
class UserGroupOps:
    def __init__(self, db):
        self.db = db
        self.ugt = UserGroupTroves(db)
        self.ugi = UserGroupInstances(db)

    # rebuild the cache tables completely for a userGroup
    def updateUserGroupId(self, userGroupId):
        logMe(3, userGroupId)
        self.ugi.updateUserGroupId(userGroupId)
        logMe(3, "ugi.updateUserGroupId", userGroupId)
##         self.ugt.update(userGroupId)
##         logMe(3, "ugt.update", userGroupId)
##         self.latest.updateUserGroupId(userGroupId)
##         logMe(3, "latest.updateUserGroupId", userGroupId)
    def updateUserGroup(self, userGroup):
        userGroupId = self._getGroupId(userGroup)
        self.updateUserGroupId(userGroupId)

    # rebuild all caches
    def rebuild(self):
        self.ugi.rebuild()
        self.ugt.rebuild()
        self.latest.rebuild()
        
